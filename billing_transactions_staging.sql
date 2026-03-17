-- =============================================================================
-- Direct-Billing Transaction Staging
-- Purpose : Raw + typed staging for direct-billing provider transactions
--           (covers both charges and refunds in separate tables)
-- Key feature: Multi-currency minor-unit normalisation (3-decimal, 0-decimal,
--              and standard 2-decimal currencies handled via CASE)
-- =============================================================================

-- ---------------------------------------------------------------------------
-- Shared currency normalisation note
-- ---------------------------------------------------------------------------
-- ISO 4217 currencies differ in their minor-unit exponents:
--   3-decimal (÷1000) : BHD, IQD, JOD, KWD, LYD, OMR, TND
--   0-decimal (÷1)    : CVE, CLP, DJF, GNF, ISK, JPY, KMF, KRW,
--                        PYG, RWF, UGX, VND, VUV, XAF, XOF, XPF
--   2-decimal (÷100)  : all other currencies (default)
-- ---------------------------------------------------------------------------

-- ---------------------------------------------------------------------------
-- 1. Transactions – Raw landing table
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS staging_data.billing_transactions_raw
(
    transaction_id                          VARCHAR(80),
    user_id                                 VARCHAR(80),
    transaction_status                      VARCHAR(80),
    transaction_type                        VARCHAR(80),
    product_name                            VARCHAR(80),
    product_description                     VARCHAR(80),
    payment_period                          VARCHAR(80),
    amount                                  VARCHAR(80),   -- minor units; see normalisation above
    currency                                VARCHAR(80),
    vat                                     VARCHAR(80),
    subscription_id                         VARCHAR(80),
    refunded_transaction_id                 VARCHAR(80),
    created_timestamp                       VARCHAR(80),
    updated_timestamp                       VARCHAR(80),
    charged_timestamp                       VARCHAR(80),
    payment_country                         VARCHAR(80),
    card_provider                           VARCHAR(80),
    vendor_reference_id                     VARCHAR(80),
    price_plan_id                           VARCHAR(80),
    vat_amount                              VARCHAR(80),
    sales_tax_amount                        VARCHAR(80),
    invoice_id                              VARCHAR(80),
    merchant_reference_id                   VARCHAR(80),
    payment_provider                        VARCHAR(80),
    payment_type                            VARCHAR(80),
    geo_location_country_iso                VARCHAR(80),
    geo_location_subdivision_iso            VARCHAR(80),
    refunder_user_id                        VARCHAR(80),
    refund_reason                           VARCHAR(3000),
    invoice_generated_timestamp             VARCHAR(80),
    realm                                   VARCHAR(80),
    global_subscription_id                  VARCHAR(80),
    installment_details_number_of_installments VARCHAR(40),
    items_item_id                           VARCHAR(80),
    load_tx_id                              VARCHAR(40),
    file_tx_id                              VARCHAR(40),
    e_invoice_id                            VARCHAR(80),
    pay_per_view_access_id                  VARCHAR(80)
);

-- ---------------------------------------------------------------------------
-- 2. Transactions – Typed staging table
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS staging_data.billing_transactions
(
    transaction_id                          VARCHAR(80),
    user_id                                 VARCHAR(80),
    transaction_status                      VARCHAR(80),
    transaction_type                        VARCHAR(80),
    product_name                            VARCHAR(80),
    product_description                     VARCHAR(80),
    payment_period                          VARCHAR(80),
    -- Amounts stored in major units after normalisation
    amount                                  DECIMAL(20, 6),
    currency                                VARCHAR(80),
    vat                                     DECIMAL(20, 6),
    subscription_id                         INTEGER,
    refunded_transaction_id                 VARCHAR(80),
    created_timestamp                       TIMESTAMP,
    updated_timestamp                       TIMESTAMP,
    charged_timestamp                       TIMESTAMP,
    payment_country                         VARCHAR(80),
    card_provider                           VARCHAR(80),
    vendor_reference_id                     VARCHAR(80),
    price_plan_id                           VARCHAR(80),
    vat_amount                              DECIMAL(20, 6),
    sales_tax_amount                        DECIMAL(20, 6),
    invoice_id                              VARCHAR(80),
    merchant_reference_id                   VARCHAR(80),
    payment_provider                        VARCHAR(80),
    payment_type                            VARCHAR(80),
    geo_location_country_iso                VARCHAR(80),
    geo_location_subdivision_iso            VARCHAR(80),
    refunder_user_id                        VARCHAR(80),
    refund_reason                           VARCHAR(3000),
    invoice_generated_timestamp             VARCHAR(80),
    realm                                   VARCHAR(80),
    global_subscription_id                  VARCHAR(80),
    installment_details_number_of_installments INTEGER,
    items_item_id                           VARCHAR(80),
    load_tx_id                              VARCHAR(40),
    file_tx_id                              VARCHAR(40),
    e_invoice_id                            VARCHAR(80),
    pay_per_view_access_id                  VARCHAR(80)
);

-- ---------------------------------------------------------------------------
-- 3. Incremental upsert with minor-unit normalisation
-- ---------------------------------------------------------------------------
INSERT INTO staging_data.billing_transactions
SELECT
    transaction_id,
    user_id,
    transaction_status,
    transaction_type,
    product_name,
    product_description,
    payment_period,

    -- ── Amount: convert from provider minor units to major units ─────────
    CASE
        WHEN currency IN ('BHD','IQD','JOD','KWD','LYD','OMR','TND')
            THEN (amount::DECIMAL(20,6) / 1000)
        WHEN currency IN ('CVE','CLP','DJF','GNF','ISK','JPY','KMF','KRW',
                          'PYG','RWF','UGX','VND','VUV','XAF','XOF','XPF')
            THEN  amount::DECIMAL(20,6)
        ELSE        (amount::DECIMAL(20,6) / 100)
    END AS amount,

    currency,

    -- ── VAT ───────────────────────────────────────────────────────────────
    CASE
        WHEN currency IN ('BHD','IQD','JOD','KWD','LYD','OMR','TND')
            THEN (vat::DECIMAL(20,6) / 1000)
        WHEN currency IN ('CVE','CLP','DJF','GNF','ISK','JPY','KMF','KRW',
                          'PYG','RWF','UGX','VND','VUV','XAF','XOF','XPF')
            THEN  vat::DECIMAL(20,6)
        ELSE        (vat::DECIMAL(20,6) / 100)
    END AS vat,

    subscription_id::INTEGER,
    refunded_transaction_id,
    created_timestamp::TIMESTAMP,
    updated_timestamp::TIMESTAMP,
    charged_timestamp::TIMESTAMP,
    payment_country,
    card_provider,
    vendor_reference_id,
    price_plan_id,

    -- ── VAT amount ────────────────────────────────────────────────────────
    CASE
        WHEN currency IN ('BHD','IQD','JOD','KWD','LYD','OMR','TND')
            THEN (vat_amount::DECIMAL(20,6) / 1000)
        WHEN currency IN ('CVE','CLP','DJF','GNF','ISK','JPY','KMF','KRW',
                          'PYG','RWF','UGX','VND','VUV','XAF','XOF','XPF')
            THEN  vat_amount::DECIMAL(20,6)
        ELSE        (vat_amount::DECIMAL(20,6) / 100)
    END AS vat_amount,

    -- ── Sales tax ─────────────────────────────────────────────────────────
    CASE
        WHEN currency IN ('BHD','IQD','JOD','KWD','LYD','OMR','TND')
            THEN (sales_tax_amount::DECIMAL(20,6) / 1000)
        WHEN currency IN ('CVE','CLP','DJF','GNF','ISK','JPY','KMF','KRW',
                          'PYG','RWF','UGX','VND','VUV','XAF','XOF','XPF')
            THEN  sales_tax_amount::DECIMAL(20,6)
        ELSE        (sales_tax_amount::DECIMAL(20,6) / 100)
    END AS sales_tax_amount,

    invoice_id,
    merchant_reference_id,
    payment_provider,
    payment_type,
    geo_location_country_iso,
    geo_location_subdivision_iso,
    refunder_user_id,
    refund_reason,
    invoice_generated_timestamp,
    'direct_billing'   AS realm,   -- normalised realm tag
    global_subscription_id,
    installment_details_number_of_installments::INTEGER,
    items_item_id,
    load_tx_id,
    file_tx_id,
    e_invoice_id,
    pay_per_view_access_id
FROM staging_data.billing_transactions_raw
WHERE file_tx_id = '{{file_tx_id}}';

UPDATE audit_trail.load_audit_trail
SET record_count_staging = (
        SELECT COUNT(1)
        FROM staging_data.billing_transactions
        WHERE file_tx_id = '{{file_tx_id}}'
    ),
    load_date_time = CURRENT_TIMESTAMP
WHERE file_tx_id = '{{file_tx_id}}';
