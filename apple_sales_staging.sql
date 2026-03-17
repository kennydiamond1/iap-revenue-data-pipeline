-- =============================================================================
-- Apple IAP Sales Staging
-- Purpose : Raw ingestion + type-cast staging for Apple App Store sales reports
-- Pattern : Incremental load via file transaction ID (file_tx_id)
-- Layers  : raw (varchar everything) → staging (typed) → facts
-- =============================================================================

-- ---------------------------------------------------------------------------
-- 1. Raw landing table  (varchar columns, no transforms)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS staging_data.apple_sales_raw
(
    provider                VARCHAR(255),
    provider_country        VARCHAR(80),
    sku                     VARCHAR(80),
    developer               VARCHAR(255),
    title                   VARCHAR(255),
    version                 VARCHAR(80),
    product_type_identifier VARCHAR(80),
    units                   VARCHAR(80),       -- cast to integer downstream
    developer_proceeds      VARCHAR(80),       -- cast to numeric downstream
    begin_date              VARCHAR(80),       -- cast to date downstream (MM/DD/YYYY)
    end_date                VARCHAR(80),       -- cast to date downstream (MM/DD/YYYY)
    customer_currency       VARCHAR(80),
    country_code            VARCHAR(80),
    currency_of_proceeds    VARCHAR(80),
    apple_identifier        VARCHAR(80),
    customer_price          VARCHAR(80),       -- cast to numeric downstream
    promo_code              VARCHAR(80),
    parent_identifier       VARCHAR(255),
    subscription            VARCHAR(80),
    period                  VARCHAR(80),
    category                VARCHAR(255),
    cmb                     VARCHAR(255),
    device                  VARCHAR(80),
    supported_platforms     VARCHAR(255),
    proceeds_reason         VARCHAR(255),
    preserved_pricing       VARCHAR(255),
    client                  VARCHAR(255),
    order_type              VARCHAR(255),
    load_tx_id              VARCHAR(40),
    file_tx_id              VARCHAR(40)
);

-- ---------------------------------------------------------------------------
-- 2. Typed staging table  (strongly typed, ready for fact load)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS staging_data.apple_sales
(
    provider                VARCHAR(255),
    provider_country        VARCHAR(20),
    sku                     VARCHAR(80),
    developer               VARCHAR(255),
    title                   VARCHAR(255),
    version                 VARCHAR(80),
    product_type_identifier VARCHAR(40),
    units                   INTEGER          ENCODE AZ64,
    developer_proceeds      NUMERIC(20, 6)   ENCODE AZ64,
    begin_date              DATE             ENCODE AZ64,
    end_date                DATE             ENCODE AZ64,
    customer_currency       VARCHAR(20),
    country_code            VARCHAR(20),
    currency_of_proceeds    VARCHAR(20),
    apple_identifier        VARCHAR(80),
    customer_price          NUMERIC(20, 6)   ENCODE AZ64,
    promo_code              VARCHAR(80),
    parent_identifier       VARCHAR(255),
    subscription            VARCHAR(40),
    period                  VARCHAR(40),
    category                VARCHAR(255),
    cmb                     VARCHAR(255),
    device                  VARCHAR(40),
    supported_platforms     VARCHAR(255),
    proceeds_reason         VARCHAR(255),
    preserved_pricing       VARCHAR(255),
    client                  VARCHAR(255),
    order_type              VARCHAR(255),
    file_tx_region          VARCHAR(40),      -- region tag injected at load time
    load_tx_id              VARCHAR(40),
    file_tx_id              VARCHAR(40)
);

-- ---------------------------------------------------------------------------
-- 3. Incremental upsert: delete-insert for idempotency
-- ---------------------------------------------------------------------------
DELETE FROM staging_data.apple_sales
WHERE file_tx_id = '{{file_tx_id}}';

INSERT INTO staging_data.apple_sales
SELECT provider,
       provider_country,
       sku,
       developer,
       title,
       version,
       product_type_identifier,
       units::INTEGER,
       developer_proceeds::NUMERIC(20, 6),
       -- Handle blank dates gracefully before casting
       TO_DATE(DECODE(TRIM(begin_date) = '', TRUE, NULL, begin_date), 'MM/DD/YYYY') AS begin_date,
       TO_DATE(DECODE(TRIM(end_date)   = '', TRUE, NULL, end_date),   'MM/DD/YYYY') AS end_date,
       customer_currency,
       country_code,
       currency_of_proceeds,
       apple_identifier,
       customer_price::NUMERIC(20, 6),
       promo_code,
       parent_identifier,
       subscription,
       period,
       category,
       cmb,
       device,
       supported_platforms,
       proceeds_reason,
       preserved_pricing,
       client,
       order_type,
       '{{region}}'      AS file_tx_region,   -- parameterised at pipeline runtime
       load_tx_id,
       file_tx_id
FROM staging_data.apple_sales_raw
WHERE file_tx_id = '{{file_tx_id}}';

-- ---------------------------------------------------------------------------
-- 4. Update audit trail with staged record count
-- ---------------------------------------------------------------------------
UPDATE audit_trail.load_audit_trail
SET record_count_staging = (
        SELECT COUNT(1)
        FROM staging_data.apple_sales
        WHERE file_tx_id = '{{file_tx_id}}'
    ),
    load_date_time = CURRENT_TIMESTAMP
WHERE file_tx_id = '{{file_tx_id}}';
