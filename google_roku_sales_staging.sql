-- =============================================================================
-- Google Play IAP Sales Staging
-- Purpose : Raw ingestion + type-cast staging for Google Play sales reports
-- Pattern : Incremental load via file_tx_id
-- =============================================================================

CREATE TABLE IF NOT EXISTS staging_data.google_sales_raw
(
    order_number            VARCHAR(100),
    order_charged_date      VARCHAR(80),
    order_charged_timestamp VARCHAR(80),
    financial_status        VARCHAR(80),
    device_model            VARCHAR(255),
    product_title           VARCHAR(255),
    product_id              VARCHAR(255),
    product_type            VARCHAR(80),
    sku_id                  VARCHAR(255),
    hardware                VARCHAR(80),
    buyer_country           VARCHAR(80),
    buyer_state             VARCHAR(80),
    buyer_postal_code       VARCHAR(80),
    buyer_currency          VARCHAR(20),
    amount_buyer_currency   VARCHAR(80),
    currency_of_sale        VARCHAR(20),
    amount_merchant_currency VARCHAR(80),
    load_tx_id              VARCHAR(40),
    file_tx_id              VARCHAR(40)
);

CREATE TABLE IF NOT EXISTS staging_data.google_sales
(
    order_number             VARCHAR(100),
    order_charged_date       DATE             ENCODE AZ64,
    order_charged_timestamp  TIMESTAMP        ENCODE AZ64,
    financial_status         VARCHAR(80),
    device_model             VARCHAR(255),
    product_title            VARCHAR(255),
    product_id               VARCHAR(255),
    product_type             VARCHAR(80),
    sku_id                   VARCHAR(255),
    hardware                 VARCHAR(80),
    buyer_country            VARCHAR(80),
    buyer_state              VARCHAR(80),
    buyer_postal_code        VARCHAR(80),
    buyer_currency           VARCHAR(20),
    amount_buyer_currency    NUMERIC(20, 6)   ENCODE AZ64,
    currency_of_sale         VARCHAR(20),
    amount_merchant_currency NUMERIC(20, 6)   ENCODE AZ64,
    file_tx_region           VARCHAR(40),
    load_tx_id               VARCHAR(40),
    file_tx_id               VARCHAR(40)
);

DELETE FROM staging_data.google_sales
WHERE file_tx_id = '{{file_tx_id}}';

INSERT INTO staging_data.google_sales
SELECT order_number,
       order_charged_date::DATE,
       order_charged_timestamp::TIMESTAMP,
       financial_status,
       device_model,
       product_title,
       product_id,
       product_type,
       sku_id,
       hardware,
       buyer_country,
       buyer_state,
       buyer_postal_code,
       buyer_currency,
       amount_buyer_currency::NUMERIC(20, 6),
       currency_of_sale,
       amount_merchant_currency::NUMERIC(20, 6),
       '{{region}}'  AS file_tx_region,
       load_tx_id,
       file_tx_id
FROM staging_data.google_sales_raw
WHERE file_tx_id = '{{file_tx_id}}';

UPDATE audit_trail.load_audit_trail
SET record_count_staging = (
        SELECT COUNT(1) FROM staging_data.google_sales WHERE file_tx_id = '{{file_tx_id}}'
    ),
    load_date_time = CURRENT_TIMESTAMP
WHERE file_tx_id = '{{file_tx_id}}';


-- =============================================================================
-- Roku IAP Transaction Staging
-- Purpose : Raw ingestion + type-cast staging for Roku channel transaction reports
-- =============================================================================

CREATE TABLE IF NOT EXISTS staging_data.roku_transactions_raw
(
    transaction_id          VARCHAR(80),
    channel_id              VARCHAR(80),
    channel_name            VARCHAR(255),
    product_id              VARCHAR(255),
    product_name            VARCHAR(255),
    product_type            VARCHAR(80),
    transaction_type        VARCHAR(80),
    transaction_date        VARCHAR(80),
    amount                  VARCHAR(80),
    currency                VARCHAR(20),
    country_code            VARCHAR(20),
    user_id                 VARCHAR(80),
    load_tx_id              VARCHAR(40),
    file_tx_id              VARCHAR(40)
);

CREATE TABLE IF NOT EXISTS staging_data.roku_transactions
(
    transaction_id          VARCHAR(80),
    channel_id              VARCHAR(80),
    channel_name            VARCHAR(255),
    product_id              VARCHAR(255),
    product_name            VARCHAR(255),
    product_type            VARCHAR(80),
    transaction_type        VARCHAR(80),
    transaction_date        TIMESTAMP        ENCODE AZ64,
    amount                  NUMERIC(20, 6)   ENCODE AZ64,
    currency                VARCHAR(20),
    country_code            VARCHAR(20),
    user_id                 VARCHAR(80),
    file_tx_region          VARCHAR(40),
    load_tx_id              VARCHAR(40),
    file_tx_id              VARCHAR(40)
);

DELETE FROM staging_data.roku_transactions
WHERE file_tx_id = '{{file_tx_id}}';

INSERT INTO staging_data.roku_transactions
SELECT transaction_id,
       channel_id,
       channel_name,
       product_id,
       product_name,
       product_type,
       transaction_type,
       transaction_date::TIMESTAMP,
       amount::NUMERIC(20, 6),
       currency,
       country_code,
       user_id,
       '{{region}}'  AS file_tx_region,
       load_tx_id,
       file_tx_id
FROM staging_data.roku_transactions_raw
WHERE file_tx_id = '{{file_tx_id}}';

UPDATE audit_trail.load_audit_trail
SET record_count_staging = (
        SELECT COUNT(1) FROM staging_data.roku_transactions WHERE file_tx_id = '{{file_tx_id}}'
    ),
    load_date_time = CURRENT_TIMESTAMP
WHERE file_tx_id = '{{file_tx_id}}';
