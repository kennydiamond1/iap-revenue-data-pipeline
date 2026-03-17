-- =============================================================================
-- Amazon IAP Sales Staging
-- Purpose : Raw ingestion + type-cast staging for Amazon Appstore sales reports
-- Pattern : Incremental load via file transaction ID (file_tx_id)
-- Layers  : raw (varchar everything) → staging (typed) → facts
-- =============================================================================

-- ---------------------------------------------------------------------------
-- 1. Raw landing table
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS staging_data.amazon_sales_raw
(
    marketplace                              VARCHAR(40),
    country_region_code                      VARCHAR(40),
    invoice_id                               VARCHAR(100),
    transaction_id                           VARCHAR(40),
    transaction_time                         VARCHAR(80),   -- "2024-01-15 12:00:00 UTC" format
    transaction_type                         VARCHAR(40),
    adjustment                               VARCHAR(20),
    asin                                     VARCHAR(40),
    vendor_sku                               VARCHAR(255),
    title                                    VARCHAR(255),
    item_name                                VARCHAR(255),
    item_type                                VARCHAR(255),
    in_app_subscription_term                 VARCHAR(40),
    in_app_subscription_status               VARCHAR(40),
    units                                    VARCHAR(80),
    usage_time_for_underground_apps_seconds  VARCHAR(80),
    marketplace_currency                     VARCHAR(40),
    sales_price_marketplace_currency         VARCHAR(80),
    estimated_earnings_marketplace_currency  VARCHAR(80),
    digital_order_id                         VARCHAR(80),
    app_user_id                              VARCHAR(80),
    receipt_id                               VARCHAR(80),
    sales_channel                            VARCHAR(80),
    device_type                              VARCHAR(80),
    device_os                                VARCHAR(80),
    load_tx_id                               VARCHAR(40),
    file_tx_id                               VARCHAR(40)
);

-- ---------------------------------------------------------------------------
-- 2. Typed staging table
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS staging_data.amazon_sales
(
    marketplace                              VARCHAR(40),
    country_region_code                      VARCHAR(40),
    invoice_id                               VARCHAR(100),
    transaction_id                           VARCHAR(40),
    -- Amazon timestamps include a timezone token ("UTC") that must be split out
    transaction_time_local                   TIMESTAMP        ENCODE AZ64,
    transaction_time_local_tz                VARCHAR(40),
    transaction_time_utc                     TIMESTAMP        ENCODE AZ64,
    transaction_type                         VARCHAR(40),
    adjustment                               VARCHAR(20),
    asin                                     VARCHAR(40),
    vendor_sku                               VARCHAR(255),
    title                                    VARCHAR(255),
    item_name                                VARCHAR(255),
    item_type                                VARCHAR(255),
    in_app_subscription_term                 VARCHAR(40),
    in_app_subscription_status               VARCHAR(40),
    units                                    INTEGER          ENCODE AZ64,
    usage_time_for_underground_apps_seconds  NUMERIC(20, 6)   ENCODE AZ64,
    marketplace_currency                     VARCHAR(40),
    sales_price_marketplace_currency         NUMERIC(20, 6)   ENCODE AZ64,
    estimated_earnings_marketplace_currency  NUMERIC(20, 6)   ENCODE AZ64,
    digital_order_id                         VARCHAR(80),
    app_user_id                              VARCHAR(80),
    receipt_id                               VARCHAR(80),
    sales_channel                            VARCHAR(80),
    device_type                              VARCHAR(80),
    device_os                                VARCHAR(80),
    file_tx_region                           VARCHAR(40),
    load_tx_id                               VARCHAR(40),
    file_tx_id                               VARCHAR(40)
);

-- ---------------------------------------------------------------------------
-- 3. Incremental upsert
-- ---------------------------------------------------------------------------
DELETE FROM staging_data.amazon_sales
WHERE file_tx_id = '{{file_tx_id}}';

INSERT INTO staging_data.amazon_sales
SELECT marketplace,
       country_region_code,
       invoice_id,
       transaction_id,
       -- Cast the raw string to TIMESTAMP; split_part extracts the TZ token
       transaction_time::TIMESTAMP                                    AS transaction_time_local,
       SPLIT_PART(transaction_time, ' ', 3)                           AS transaction_time_local_tz,
       -- TIMESTAMPTZ cast converts to UTC then strips the offset
       transaction_time::TIMESTAMPTZ::TIMESTAMP                       AS transaction_time_utc,
       transaction_type,
       adjustment,
       asin,
       vendor_sku,
       title,
       item_name,
       item_type,
       in_app_subscription_term,
       in_app_subscription_status,
       units::INTEGER,
       usage_time_for_underground_apps_seconds::NUMERIC(20, 6),
       marketplace_currency,
       sales_price_marketplace_currency::NUMERIC(20, 6),
       estimated_earnings_marketplace_currency::NUMERIC(20, 6),
       digital_order_id,
       app_user_id,
       receipt_id,
       sales_channel,
       device_type,
       device_os,
       '{{region}}'   AS file_tx_region,
       load_tx_id,
       file_tx_id
FROM staging_data.amazon_sales_raw
WHERE file_tx_id = '{{file_tx_id}}';

-- ---------------------------------------------------------------------------
-- 4. Update audit trail
-- ---------------------------------------------------------------------------
UPDATE audit_trail.load_audit_trail
SET record_count_staging = (
        SELECT COUNT(1)
        FROM staging_data.amazon_sales
        WHERE file_tx_id = '{{file_tx_id}}'
    ),
    load_date_time = CURRENT_TIMESTAMP
WHERE file_tx_id = '{{file_tx_id}}';
