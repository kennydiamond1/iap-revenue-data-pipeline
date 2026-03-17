-- =============================================================================
-- IAP Accrual / Deferral Materialized View
-- Purpose : Calculates subscription revenue accrual and deferral positions
--           for all IAP channels using a rolling 24-month window.
--
-- Key concepts demonstrated:
--   • Exchange rate application (daily avg rates, USD as base)
--   • Per-realm reporting currency selection
--   • VAT extraction from gross amounts
--   • Commission calculation
--   • Daily revenue rate for straight-line deferral (MONTH / YEAR periods)
--   • BT revenue-share allocation logic
-- =============================================================================

-- ---------------------------------------------------------------------------
-- 0. Refresh the exchange rate reference MV (dependency)
-- ---------------------------------------------------------------------------
REFRESH MATERIALIZED VIEW meta.avg_daily_exchange_rate;

-- ---------------------------------------------------------------------------
-- 1. Drop and recreate the accrual/deferral MV
-- ---------------------------------------------------------------------------
DROP MATERIALIZED VIEW IF EXISTS dimensions.iap_accrual_deferral;

CREATE MATERIALIZED VIEW dimensions.iap_accrual_deferral AS
(
SELECT
    -- ── Identifiers ──────────────────────────────────────────────────────
    payment_provider,
    psp_reference,
    payment_period,
    product_name,
    tier_type,

    -- ── Gross financials (transaction currency) ───────────────────────────
    net,
    main_amount,
    sales_tax,
    amount,
    currency,

    -- ── Exchange rates ─────────────────────────────────────────────────────
    CASE
        WHEN exch.exchange_rate = 0 THEN 0
        WHEN currency = 'USD'       THEN 1
        ELSE 1.0 / exch.exchange_rate
    END                                                            AS exchange_rate_to_usd,

    exch_rpt.currency_code                                         AS reporting_currency,
    CASE
        WHEN exch.exchange_rate = 0 THEN 0
        ELSE exch_rpt.exchange_rate / exch.exchange_rate
    END                                                            AS reporting_currency_exchange_rate,

    -- ── Geography ─────────────────────────────────────────────────────────
    subscribed_in_country,
    payments.country_code,
    transaction_type,
    realm,
    file_tx_region,
    source_region,
    payments.company,
    created_timestamp,

    -- ── VAT ───────────────────────────────────────────────────────────────
    COALESCE(vat_cty.vat_rate_meta, 0)                             AS vat_rate,
    amount - amount / (1.0 + COALESCE(vat_cty.vat_rate_meta, 0))  AS vat_amount,
    amount - vat_amount                                            AS revenue,

    -- ── Commission ────────────────────────────────────────────────────────
    commission_rates.commission_rate,
    revenue * commission_rates.commission_rate                     AS commission,

    -- ── Straight-line deferral: days in subscription period ───────────────
    CASE
        WHEN payment_period = 'MONTH'
            THEN DATEDIFF(day,
                     DATE_TRUNC('month', created_timestamp),
                     DATEADD(month, 1, DATE_TRUNC('month', created_timestamp)))
        WHEN payment_period = 'YEAR'
            THEN DATEDIFF(day,
                     DATE_TRUNC('day', created_timestamp),
                     DATEADD(year,  1, DATE_TRUNC('day', created_timestamp)))
        ELSE 1
    END                                                            AS days_in_period,

    revenue    / NULLIF(days_in_period, 0)                        AS daily_revenue_rate,
    commission / NULLIF(days_in_period, 0)                        AS daily_commission_rate,

    -- ── Partner revenue share (region-gated) ──────────────────────────────
    -- Only applies for specific realms and non-qualifying country sets
    CASE
        WHEN realm = 'eurosport'
         AND payments.country_code NOT IN ('gb','uk','gg','gi','ie','im','je')
            THEN 0
        ELSE bt_revenue_share
    END                                                            AS partner_rev_share,

    CURRENT_TIMESTAMP                                              AS mv_last_updated

FROM (
    -- ── Inner subquery: typed payment rows with region/company tags ────────
    SELECT
        psp_reference,
        created_timestamp,
        transaction_type,
        product_name,
        currency,
        main_amount                                                AS amount,
        net,
        main_amount                                                AS main_amount,
        sales_tax,
        subscribed_in_country,
        file_tx_region,
        source_region,
        realm,
        company,
        payment_provider,
        -- Fall back to currency-implied country when subscribed_in_country is blank
        CASE
            WHEN COALESCE(TRIM(subscribed_in_country), '') IN ('', '**')
                THEN LOWER(fcc.country_code)
            ELSE LOWER(subscribed_in_country)
        END                                                        AS country_code,
        payment_period,
        -- Commission area classification per provider/company combination
        CASE
            WHEN company = 'StreamCo' AND payment_provider = 'Amazon' THEN 'All'
            WHEN company = 'MaxStream' AND payment_provider = 'Amazon' THEN 'All'
            ELSE ''
        END                                                        AS commission_area,
        tier_type,
        bt_revenue_share

    FROM (
        -- ── Innermost: raw payment rows from fact table ────────────────────
        SELECT
            psp_reference,
            utc_timestamp                                          AS created_timestamp,
            CURRENT_DATE::DATE                                     AS accrual_end_timestamp,
            record_type                                            AS transaction_type,
            product_title                                          AS product_name,
            product_id,
            vendor_sku                                             AS product_name,
            main_currency                                          AS currency,
            net,
            main_amount,
            0                                                      AS sales_tax,
            issuer_country                                         AS subscribed_in_country,
            file_tx_region,
            -- Source region derived from file_tx_region
            CASE
                WHEN file_tx_region = 'MAX'   THEN 'max'
                WHEN file_tx_region = 'AMER'  THEN 'amer'
                WHEN file_tx_region = 'EMEA'  THEN 'emea'
                ELSE NULL
            END                                                    AS source_region,
            -- Realm derived from file_tx_region
            CASE
                WHEN file_tx_region = 'AMER'  THEN 'go'
                WHEN file_tx_region = 'EMEA'  THEN 'dplay'
                WHEN file_tx_region = 'MAX'   THEN 'streaming'
                WHEN file_tx_region = 'ESP'   THEN 'eurosport'
                WHEN file_tx_region = 'INDIA' THEN 'streaming_india'
                ELSE ''
            END                                                    AS realm,
            -- Company tag
            CASE
                WHEN file_tx_region IN ('AMER','EMEA','ESP') THEN 'StreamCo'
                WHEN file_tx_region IN ('MAX','CNN')         THEN 'MaxStream'
                ELSE ''
            END                                                    AS company,
            pmt_channel                                            AS payment_provider
        FROM facts.ft_payments
        WHERE utc_timestamp  >= DATEADD(month, -24, CURRENT_DATE)
          AND utc_timestamp  <  CURRENT_DATE
          AND record_type     = 'Charge'
          AND type            = 'Subscription'
          AND file_tx_region IN ('AMER','EMEA','MAX','ESP','CNN')
          AND pmt_channel     = 'Amazon'
    ) base_payments

    LEFT JOIN (
        SELECT currency_code, country_code
        FROM reference.fallback_country_code
    ) fcc ON base_payments.currency = fcc.currency_code

    INNER JOIN (
        SELECT payment_provider, realm, source_region, file_tx_region,
               sku_id, payment_period, tier_type, included_in_accrual_yn,
               bt_revenue_share
        FROM reference.iap_tier_type_sku
        WHERE included_in_accrual_yn = 'Y'
    ) sku_ref
          ON base_payments.product_name     = sku_ref.sku_id
         AND base_payments.file_tx_region   = sku_ref.file_tx_region
         AND base_payments.payment_provider = sku_ref.payment_provider

    WHERE payment_period IN ('MONTH','YEAR')
) payments

-- ── Exchange rate joins ────────────────────────────────────────────────────
LEFT JOIN meta.avg_daily_exchange_rate exch
       ON payments.currency = exch.currency_code
      AND DATE(payments.created_timestamp) = exch.exchange_rate_date

LEFT JOIN meta.avg_daily_exchange_rate exch_rpt
       ON CASE payments.realm
              WHEN 'eurosport'      THEN 'EUR'
              WHEN 'dplay'          THEN 'GBP'
              WHEN 'go'             THEN 'USD'
              WHEN 'streaming_india'THEN 'INR'
              WHEN 'streaming'      THEN 'USD'
              ELSE payments.currency
          END = exch_rpt.currency_code
      AND DATE(payments.created_timestamp) = exch_rpt.exchange_rate_date

LEFT JOIN (
    SELECT country_code, COALESCE(vat_rate, 0) AS vat_rate_meta
    FROM meta.vat_by_country
) vat_cty ON payments.country_code = vat_cty.country_code

LEFT JOIN (
    SELECT company, provider, area, commission_rate
    FROM reference.commission_rates
) commission_rates
       ON payments.company         = commission_rates.company
      AND payments.payment_provider= commission_rates.provider
      AND payments.commission_area = commission_rates.area
);

-- ---------------------------------------------------------------------------
-- 2. Grant read access to analytics users
-- ---------------------------------------------------------------------------
GRANT SELECT ON dimensions.iap_accrual_deferral TO GROUP analytics_users;
