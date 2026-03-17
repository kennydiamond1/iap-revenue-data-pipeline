-- =============================================================================
-- Ingestion Monitoring Materialized View
-- Purpose : Tracks data progression from collection → raw staging →
--           final staging → fact table for a given provider/channel.
--           Surfaces variance metrics so data ops can detect missed or
--           partially loaded files quickly.
--
-- Pattern : UNION ALL of 7 sub-queries, each contributing one "layer"
--           of the pipeline, then aggregated per file_tx_id.
--
-- Metrics produced:
--   files_collected           – files seen in collection audit trail
--   records_audit_trail       – expected row count from collection metadata
--   raw_file_count            – files present in raw staging
--   raw_transaction_count     – rows in raw staging
--   final_file_count          – files present in typed staging
--   final_transaction_count   – rows in typed staging
--   fact_file_count           – files present in fact table
--   fact_transaction_count    – rows in fact table
--   overwritten               – rows replaced by superseded MTD files
--   pct_fact                  – % of expected rows that reached the fact table
--   variance                  – % including superseded row adjustments
-- =============================================================================

DROP MATERIALIZED VIEW IF EXISTS ingestion_monitoring.mv_iap_google_sales;

CREATE MATERIALIZED VIEW ingestion_monitoring.mv_iap_google_sales AS

WITH
-- ── Collection audit: files expected within the configured monitoring window ─
collection AS (
    SELECT
        cat.file_tx_id,
        cat.data_source,
        cat.collection_type,
        cat.data_file,
        cat.data_file_timestamp::DATE AS file_date,
        cat.tx_row_count,
        lat.status                    AS load_status
    FROM audit_trail.collection_audit_trail cat
    JOIN reference.ingestion_monitoring_config cfg
      ON cfg.mv_id            = 'ingestion_monitoring.mv_iap_google_sales'
     AND cfg.mv_status        = 'ENABLED'
     AND SPLIT_PART(cat.data_source, '_', 1) = cfg.data_source
     AND cat.collection_type  = cfg.collection_type
     AND cat.data_file        NOT LIKE '%full%'
     AND cat.data_file_timestamp >= DATEADD(month, -cfg.mv_range_in_months, CURRENT_DATE)
     AND cat.data_file_timestamp <= CURRENT_DATE
    LEFT JOIN audit_trail.load_audit_trail lat
           ON cat.file_tx_id  = lat.file_tx_id
)

SELECT
    data_source,
    collection_type,
    file_date,
    data_file,
    file_tx_id,
    load_status,

    SUM(files_collected)                                           AS files_collected,
    SUM(records_audit_trail)                                       AS records_audit_trail,
    SUM(raw_file_count)                                            AS raw_file_count,
    SUM(raw_transaction_count)                                     AS raw_transaction_count,
    SUM(final_file_count)                                          AS final_file_count,
    SUM(final_transaction_count)                                   AS final_transaction_count,
    SUM(fact_file_count)                                           AS fact_file_count,
    SUM(fact_transaction_count)                                    AS fact_transaction_count,
    SUM(fact_recorded_file_count)                                  AS fact_recorded_file_count,

    -- Overwritten: rows replaced by a later MTD file in the same month
    CASE
        WHEN SUM(fact_recorded_file_count) != SUM(files_collected) THEN 0
        ELSE SUM(final_transaction_count) - SUM(fact_transaction_count)
    END                                                            AS overwritten,

    -- pct_fact: what fraction of expected rows landed in the fact table
    CASE
        WHEN SUM(records_audit_trail) = 0 THEN 0
        ELSE ROUND(
            SUM(fact_transaction_count) * 100.0 / SUM(records_audit_trail), 4
        )::FLOAT
    END                                                            AS pct_fact,

    SUM(fact_recorded_transaction_count)                           AS fact_recorded_transaction_count,

    -- variance: pct_fact adjusted for superseded/ignored files
    CASE
        WHEN SUM(records_audit_trail) = 0 THEN 0
        ELSE ROUND(
            SUM(fact_recorded_transaction_count) * 100.0 / SUM(records_audit_trail), 4
        )::FLOAT
    END                                                            AS variance

FROM (

    -- Q1: Collection counts (baseline expected rows)
    SELECT data_source, collection_type, file_date, data_file,
           file_tx_id, load_status,
           COUNT(DISTINCT file_tx_id) AS files_collected,
           SUM(tx_row_count)          AS records_audit_trail,
           0 AS raw_file_count,            0 AS raw_transaction_count,
           0 AS final_file_count,          0 AS final_transaction_count,
           0 AS fact_file_count,           0 AS fact_transaction_count,
           0 AS fact_recorded_file_count,  0 AS fact_recorded_transaction_count
    FROM collection
    GROUP BY data_source, collection_type, file_date, data_file, file_tx_id, load_status

    UNION ALL

    -- Q2: Raw staging counts
    SELECT c.data_source, c.collection_type, c.file_date, c.data_file,
           c.file_tx_id, c.load_status,
           0, 0,
           COUNT(DISTINCT r.file_tx_id), SUM(r.row_count),
           0, 0, 0, 0, 0, 0
    FROM collection c
    INNER JOIN (
        SELECT file_tx_id, COUNT(1) AS row_count FROM staging_data.google_sales_raw GROUP BY file_tx_id
        UNION ALL
        SELECT file_tx_id, COUNT(1) FROM staging_data.google_sales_raw_emea GROUP BY file_tx_id
    ) r ON c.file_tx_id = r.file_tx_id
    GROUP BY c.data_source, c.collection_type, c.file_date, c.data_file, c.file_tx_id, c.load_status

    UNION ALL

    -- Q3: Final (typed) staging counts
    SELECT c.data_source, c.collection_type, c.file_date, c.data_file,
           c.file_tx_id, c.load_status,
           0, 0, 0, 0,
           COUNT(DISTINCT s.file_tx_id), SUM(s.row_count),
           0, 0, 0, 0
    FROM collection c
    INNER JOIN (
        SELECT file_tx_id, COUNT(1) AS row_count FROM staging_data.google_sales GROUP BY file_tx_id
    ) s ON c.file_tx_id = s.file_tx_id
    GROUP BY c.data_source, c.collection_type, c.file_date, c.data_file, c.file_tx_id, c.load_status

    UNION ALL

    -- Q4: Fact table counts (total)
    SELECT c.data_source, c.collection_type, c.file_date, c.data_file,
           c.file_tx_id, c.load_status,
           0, 0, 0, 0, 0, 0,
           COUNT(DISTINCT f.file_tx_id), COUNT(1),
           0, 0
    FROM collection c
    INNER JOIN facts.ft_payments f
            ON c.file_tx_id = f.file_tx_id
           AND f.pmt_channel = 'Google'
    GROUP BY c.data_source, c.collection_type, c.file_date, c.data_file, c.file_tx_id, c.load_status

    UNION ALL

    -- Q5: Fact table counts (active / non-superseded)
    SELECT c.data_source, c.collection_type, c.file_date, c.data_file,
           c.file_tx_id, c.load_status,
           0, 0, 0, 0, 0, 0, 0, 0,
           COUNT(DISTINCT f.file_tx_id), COUNT(1)
    FROM collection c
    INNER JOIN facts.ft_payments f
            ON c.file_tx_id = f.file_tx_id
           AND f.pmt_channel = 'Google'
    WHERE c.load_status NOT IN ('LOADED_FACT_IGNORED','SUPERSEDED')
    GROUP BY c.data_source, c.collection_type, c.file_date, c.data_file, c.file_tx_id, c.load_status

    UNION ALL

    -- Q6: Superseded / ignored file counts (contribute to variance denominator)
    SELECT c.data_source, c.collection_type, c.file_date, c.data_file,
           c.file_tx_id, c.load_status,
           0, 0, 0, 0, 0, 0, 0, 0,
           COUNT(DISTINCT c.file_tx_id), SUM(c.tx_row_count)
    FROM collection c
    WHERE c.load_status IN ('LOADED_FACT_IGNORED','SUPERSEDED')
    GROUP BY c.data_source, c.collection_type, c.file_date, c.data_file, c.file_tx_id, c.load_status

    UNION ALL

    -- Q7: Files pending load but superseded by a later file in the same month
    SELECT c.data_source, c.collection_type, c.file_date, c.data_file,
           c.file_tx_id, c.load_status,
           0, 0, 0, 0, 0, 0, 0, 0,
           COUNT(DISTINCT c.file_tx_id), SUM(c.tx_row_count)
    FROM collection c
    -- A later file for the same source+type+month has already loaded successfully
    INNER JOIN collection loaded
            ON c.data_source     = loaded.data_source
           AND c.collection_type = loaded.collection_type
           AND DATE_TRUNC('month', c.file_date) = DATE_TRUNC('month', loaded.file_date)
           AND c.file_date        < loaded.file_date
           AND loaded.load_status = 'LOADED_FACT'
    WHERE (c.load_status NOT IN ('LOADED_FACT','LOADED_FACT_IGNORED','SUPERSEDED')
           OR c.load_status IS NULL)
    GROUP BY c.data_source, c.collection_type, c.file_date, c.data_file, c.file_tx_id, c.load_status

) data_layers

GROUP BY data_source, collection_type, file_date, file_tx_id, data_file, load_status;

-- ---------------------------------------------------------------------------
-- Populate the master summary table from the MV
-- ---------------------------------------------------------------------------
DELETE FROM ingestion_monitoring.ft_ingestion_master
WHERE mv_id = 'ingestion_monitoring.mv_iap_google_sales';

INSERT INTO ingestion_monitoring.ft_ingestion_master
SELECT
    'ingestion_monitoring.mv_iap_google_sales' AS mv_id,
    data_source, collection_type, file_date, data_file, file_tx_id,
    load_status, files_collected, records_audit_trail,
    raw_file_count, raw_transaction_count,
    final_file_count, final_transaction_count,
    fact_file_count, fact_transaction_count,
    fact_recorded_file_count, overwritten, pct_fact,
    fact_recorded_transaction_count, variance
FROM ingestion_monitoring.mv_iap_google_sales;

-- Update config table timestamps and row counts
UPDATE reference.ingestion_monitoring_config
SET mv_created_timestamp = CURRENT_TIMESTAMP,
    mv_updated_timestamp = CURRENT_TIMESTAMP,
    mv_min_date = (SELECT MIN(file_date) FROM ingestion_monitoring.ft_ingestion_master
                   WHERE mv_id = 'ingestion_monitoring.mv_iap_google_sales'),
    mv_max_date = (SELECT MAX(file_date) FROM ingestion_monitoring.ft_ingestion_master
                   WHERE mv_id = 'ingestion_monitoring.mv_iap_google_sales'),
    mv_row_count = (SELECT COUNT(1)      FROM ingestion_monitoring.ft_ingestion_master
                   WHERE mv_id = 'ingestion_monitoring.mv_iap_google_sales')
WHERE mv_id = 'ingestion_monitoring.mv_iap_google_sales';
