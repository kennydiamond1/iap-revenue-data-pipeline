-- =============================================================================
-- Payments Fact Table
-- Purpose : Central fact table unifying IAP transactions from all providers
--           (Apple, Google, Amazon, Roku, direct-billing)
-- Pattern : Incremental load; superseded/duplicate month-to-date files purged
-- =============================================================================

-- ---------------------------------------------------------------------------
-- 1. Fact table DDL
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS facts.ft_payments
(
    -- Standardised PSP / provider columns
    company_account             VARCHAR(255),
    merchant_account            VARCHAR(255),
    psp_reference               VARCHAR(255),

    -- Timestamps
    local_timestamp             TIMESTAMP      ENCODE AZ64,
    timezone                    VARCHAR(40),
    utc_timestamp               TIMESTAMP      ENCODE AZ64,

    -- Financials
    main_currency               VARCHAR(20),
    main_amount                 NUMERIC(20, 6) ENCODE AZ64,
    payment_currency            VARCHAR(20),
    vat                         NUMERIC(20, 6) ENCODE AZ64,
    net                         NUMERIC(20, 6) ENCODE AZ64,

    -- Transaction metadata
    record_type                 VARCHAR(80),
    name                        VARCHAR(255),
    type                        VARCHAR(255),
    invoice_number              VARCHAR(100),
    receipt_id                  VARCHAR(80),
    issuer_country              VARCHAR(20),

    -- Channel identifiers
    pmt_channel                 VARCHAR(40),   -- 'Apple' | 'Google' | 'Amazon' | 'Roku' | 'DirectBilling'
    pmt_channel_nr              SMALLINT,      -- numeric channel code for joins
    pmt_ra_status               SMALLINT,

    -- Provider-specific fields
    transaction_id              VARCHAR(80),
    adjustment                  VARCHAR(20),
    asin                        VARCHAR(40),
    vendor_sku                  VARCHAR(255),
    in_app_subscription_term    VARCHAR(40),
    in_app_subscription_status  VARCHAR(40),
    digital_order_id            VARCHAR(80),
    app_user_id                 VARCHAR(80),
    sales_channel               VARCHAR(80),
    hardware                    VARCHAR(80),
    product_id                  VARCHAR(255),
    product_title               VARCHAR(255),

    -- Pipeline tracking
    record_key                  VARCHAR(500),
    fact_etl_revision           VARCHAR(20),
    file_tx_region              VARCHAR(40),
    load_tx_id                  VARCHAR(40),
    file_tx_id                  VARCHAR(40)
)
SORTKEY (utc_timestamp, pmt_channel, file_tx_region);

-- ---------------------------------------------------------------------------
-- 2. Amazon incremental load
--    Deduplication: if a newer MTD file for the same realm+month exists,
--    the older file's rows are removed and its status set to SUPERSEDED.
-- ---------------------------------------------------------------------------

-- 2a. Remove rows for the incoming file (idempotency)
DELETE FROM facts.ft_payments
WHERE file_tx_id = '{{file_tx_id}}';

-- 2b. Remove rows from older MTD files that are superseded by the incoming file
DELETE FROM facts.ft_payments
WHERE file_tx_id IN (
    SELECT old_file.file_tx_id
    FROM (
        -- All Amazon sales files: extract realm, collection type, and day-of-month
        SELECT SPLIT_PART(data_file, '_', 2)                               AS realm,
               SPLIT_PART(data_file, '_', 3)                               AS coll_type,
               SPLIT_PART(SPLIT_PART(data_file, '_', 4), '.', 1)           AS coll_date,
               SPLIT_PART(coll_date, '-', 1) || SPLIT_PART(coll_date, '-', 2) AS coll_month,
               SPLIT_PART(coll_date, '-', 3)                               AS day_of_month,
               realm || coll_type || coll_month                            AS realm_month_key,
               file_tx_id
        FROM audit_trail.load_audit_trail
        WHERE data_source ILIKE 'Amazon_%'
          AND LOWER(data_file) LIKE '%sales%'
    ) AS old_file
    INNER JOIN (
        -- Just the incoming file
        SELECT SPLIT_PART(data_file, '_', 2)                               AS realm,
               SPLIT_PART(data_file, '_', 3)                               AS coll_type,
               SPLIT_PART(SPLIT_PART(data_file, '_', 4), '.', 1)           AS coll_date,
               SPLIT_PART(coll_date, '-', 1) || SPLIT_PART(coll_date, '-', 2) AS coll_month,
               SPLIT_PART(coll_date, '-', 3)                               AS day_of_month,
               realm || coll_type || coll_month                            AS realm_month_key,
               file_tx_id
        FROM audit_trail.load_audit_trail
        WHERE data_source ILIKE 'Amazon_%'
          AND LOWER(data_file) LIKE '%sales%'
          AND file_tx_id = '{{file_tx_id}}'
    ) AS incoming
      ON old_file.realm_month_key = incoming.realm_month_key
    WHERE (
        -- Same month, older day → superseded by a later MTD
        (LEN(old_file.day_of_month) = LEN(incoming.day_of_month)
         AND old_file.day_of_month < incoming.day_of_month)
        -- Shorter day token → partial file replaced by full monthly
        OR LEN(old_file.day_of_month) < LEN(incoming.day_of_month)
    )
);

-- 2c. Insert from Amazon staging → fact table
INSERT INTO facts.ft_payments
(company_account, merchant_account, psp_reference,
 local_timestamp, timezone, utc_timestamp,
 main_currency, main_amount, payment_currency,
 vat, net,
 record_type, name, type, invoice_number, receipt_id, issuer_country,
 pmt_channel, pmt_channel_nr, pmt_ra_status, record_key, fact_etl_revision,
 transaction_id, adjustment, asin, vendor_sku,
 in_app_subscription_term, in_app_subscription_status,
 digital_order_id, app_user_id, sales_channel, hardware, product_title,
 load_tx_id, file_tx_id, file_tx_region)
SELECT
    title                                                          AS company_account,
    marketplace                                                    AS merchant_account,
    COALESCE(receipt_id, '')                                       AS psp_reference,
    transaction_time_local                                         AS local_timestamp,
    transaction_time_local_tz                                      AS timezone,
    transaction_time_utc                                           AS utc_timestamp,
    marketplace_currency                                           AS main_currency,
    sales_price_marketplace_currency                               AS main_amount,
    marketplace_currency                                           AS payment_currency,
    0                                                              AS vat,
    estimated_earnings_marketplace_currency                        AS net,
    transaction_type                                               AS record_type,
    item_name                                                      AS name,
    item_type                                                      AS type,
    invoice_id                                                     AS invoice_number,
    receipt_id,
    country_region_code                                            AS issuer_country,
    'Amazon'                                                       AS pmt_channel,
    3                                                              AS pmt_channel_nr,
    0                                                              AS pmt_ra_status,
    -- Composite dedup key
    COALESCE(receipt_id,'') || '~' || transaction_time_utc::VARCHAR
        || '~' || transaction_type || '~'                          AS record_key,
    '1.0.0'                                                        AS fact_etl_revision,
    transaction_id,
    adjustment,
    asin,
    vendor_sku,
    in_app_subscription_term,
    in_app_subscription_status,
    digital_order_id,
    app_user_id,
    sales_channel,
    device_type                                                    AS hardware,
    device_os                                                      AS product_title,
    load_tx_id,
    file_tx_id,
    file_tx_region
FROM staging_data.amazon_sales
WHERE file_tx_id = '{{file_tx_id}}';

-- 2d. Mark superseded files in the audit trail
UPDATE audit_trail.load_audit_trail
SET status = 'SUPERSEDED'
WHERE file_tx_id IN (
    -- same subquery as step 2b
    SELECT old_file.file_tx_id
    FROM (
        SELECT SPLIT_PART(data_file, '_', 2) || SPLIT_PART(data_file, '_', 3)
               || SPLIT_PART(SPLIT_PART(data_file,'_',4),'.', 1)  AS realm_month_key,
               SPLIT_PART(SPLIT_PART(data_file,'_',4),'.',1)      AS coll_date,
               SPLIT_PART(SPLIT_PART(SPLIT_PART(data_file,'_',4),'.',1),'-',3) AS day_of_month,
               file_tx_id
        FROM audit_trail.load_audit_trail
        WHERE data_source ILIKE 'Amazon_%' AND LOWER(data_file) LIKE '%sales%'
    ) old_file
    INNER JOIN (
        SELECT SPLIT_PART(data_file, '_', 2) || SPLIT_PART(data_file, '_', 3)
               || SPLIT_PART(SPLIT_PART(data_file,'_',4),'.', 1)  AS realm_month_key,
               SPLIT_PART(SPLIT_PART(data_file,'_',4),'.',1)      AS coll_date,
               SPLIT_PART(SPLIT_PART(SPLIT_PART(data_file,'_',4),'.',1),'-',3) AS day_of_month,
               file_tx_id
        FROM audit_trail.load_audit_trail
        WHERE data_source ILIKE 'Amazon_%' AND LOWER(data_file) LIKE '%sales%'
          AND file_tx_id = '{{file_tx_id}}'
    ) incoming ON old_file.realm_month_key = incoming.realm_month_key
    WHERE (LEN(old_file.day_of_month) = LEN(incoming.day_of_month)
           AND old_file.day_of_month < incoming.day_of_month)
       OR LEN(old_file.day_of_month) < LEN(incoming.day_of_month)
);
