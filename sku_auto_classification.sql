-- =============================================================================
-- IAP SKU Auto-Classification
-- Purpose : Automatically inserts newly discovered SKUs into the tier-type
--           reference table using pattern-matching rules on the SKU string.
--
-- Called as part of the incremental pipeline after each new provider file
-- lands. Ensures new products are classified without manual intervention.
--
-- Channels covered : Amazon, Google, Roku (via ft_payments)
--                    Apple              (via ft_apple_sales_reports)
-- =============================================================================

INSERT INTO reference.iap_tier_type_sku
(
    payment_provider,
    realm,
    source_region,
    file_tx_region,
    sku_id,
    payment_period,
    tier_type,
    included_in_accrual_yn,
    partner_revenue_share,
    sku_create_timestamp,
    auto_generated_yn
)

-- ── Block 1: Amazon / Google / Roku ────────────────────────────────────────
SELECT
    pmt.pmt_channel                        AS payment_provider,

    -- Realm derived from file_tx_region
    CASE pmt.file_tx_region
        WHEN 'AMER'   THEN 'go'
        WHEN 'EMEA'   THEN 'dplay'
        WHEN 'APAC'   THEN 'streaming_apac'
        WHEN 'MAX'    THEN 'streaming'
        WHEN 'MAXTR'  THEN 'streaming'
        WHEN 'CNN'    THEN 'streaming'
        WHEN 'ESP'    THEN 'eurosport'
        WHEN 'MT'     THEN 'motortrend'
        WHEN 'INDIA'  THEN 'streaming_india'
        -- DPLUS region: infer realm from SKU prefix
        WHEN 'DPLUS' THEN
            CASE
                WHEN pmt.vendor_sku ILIKE 'br_%'
                  OR pmt.vendor_sku ILIKE 'ca_%'
                  OR pmt.vendor_sku ILIKE 'us_%'
                  OR pmt.vendor_sku ILIKE 'dplus_%' THEN 'go'
                WHEN pmt.vendor_sku ILIKE 'de.%'
                  OR pmt.vendor_sku ILIKE 'ie_%'
                  OR pmt.vendor_sku ILIKE 'uk_%'    THEN 'dplay'
                ELSE 'go'
            END
    END                                    AS realm,

    -- Source sub-region (relevant for MAX/CNN global launches)
    CASE
        WHEN pmt.file_tx_region = 'MAX'
         AND (pmt.vendor_sku ILIKE '%.us%' OR pmt.vendor_sku ILIKE 'stream.%')  THEN 'amer'
        WHEN pmt.file_tx_region = 'MAX'
         AND (pmt.vendor_sku ILIKE '%latam%' OR pmt.vendor_sku ILIKE '%.lat.%') THEN 'latam'
        WHEN pmt.file_tx_region = 'MAXTR'                                        THEN 'emea'
        WHEN pmt.file_tx_region = 'MAX' AND pmt.vendor_sku ILIKE '%emea%'        THEN 'emea'
        WHEN pmt.file_tx_region = 'MAX' AND pmt.vendor_sku ILIKE '%apac%'        THEN 'apac'
        WHEN pmt.file_tx_region = 'MAX'                                          THEN 'amer'
        WHEN pmt.file_tx_region = 'CNN'                                          THEN 'cnn'
        ELSE NULL
    END                                    AS source_region,

    pmt.file_tx_region,
    pmt.vendor_sku                         AS sku_id,

    -- Payment period inferred from SKU name
    CASE
        WHEN pmt.vendor_sku ILIKE '%annual%'   THEN 'YEAR'
        WHEN pmt.vendor_sku ILIKE '%.year%'    THEN 'YEAR'
        WHEN pmt.vendor_sku ILIKE '%.1y%'      THEN 'YEAR'
        WHEN pmt.vendor_sku ILIKE '%12month%'  THEN 'YEAR'
        WHEN pmt.vendor_sku ILIKE '%12m.%'     THEN 'YEAR'
        WHEN pmt.vendor_sku ILIKE '%monthly%'  THEN 'MONTH'
        WHEN pmt.vendor_sku ILIKE '%.3month%'  THEN 'MONTH'
        WHEN pmt.vendor_sku ILIKE '%.1m.%'     THEN 'MONTH'
        WHEN pmt.vendor_sku ILIKE '%.month%'   THEN 'MONTH'
        WHEN pmt.vendor_sku ILIKE '%month%'    THEN 'MONTH'
        ELSE 'MONTH'
    END                                    AS payment_period,

    -- Tier type inferred from SKU name
    CASE
        WHEN pmt.vendor_sku ILIKE '%premium%'  THEN 'premium'
        WHEN pmt.vendor_sku ILIKE '%svod%'     THEN 'ad_free'
        WHEN pmt.vendor_sku ILIKE '%avod%'     THEN 'ad_lite'
        WHEN pmt.vendor_sku ILIKE '%lite%'     THEN 'ad_lite'
        WHEN pmt.vendor_sku ILIKE '%light%'    THEN 'ad_lite'
        WHEN pmt.vendor_sku ILIKE '%free%'     THEN 'ad_free'
        ELSE 'ad_free'
    END                                    AS tier_type,

    -- Accrual eligibility rules per channel
    CASE
        WHEN pmt.file_tx_region NOT IN ('AMER','MAX','DPLUS','EMEA','ESP','CNN','MAXTR','INDIA') THEN 'N'
        WHEN pmt.vendor_sku ILIKE '%.foodnetwork.%'  THEN 'N'
        WHEN pmt.vendor_sku ILIKE '%golftv%'         THEN 'N'
        WHEN pmt.vendor_sku ILIKE '%ppv%'            THEN 'N'
        WHEN pmt.pmt_channel = 'Amazon'
         AND SUM(pmt.main_amount) != 0
         AND pmt.type = 'Subscription'                THEN 'Y'
        WHEN pmt.pmt_channel = 'Google'
         AND SUM(pmt.main_amount) != 0                THEN 'Y'
        WHEN pmt.pmt_channel = 'Roku'
         AND SUM(pmt.main_amount) != 0                THEN 'Y'
        ELSE 'N'
    END                                    AS included_in_accrual_yn,

    0                                      AS partner_revenue_share,
    MIN(pmt.utc_timestamp)                 AS sku_create_timestamp,
    'Y'                                    AS auto_generated_yn

FROM facts.ft_payments pmt
LEFT JOIN reference.iap_tier_type_sku sku
       ON pmt.vendor_sku    = sku.sku_id
      AND pmt.pmt_channel   = sku.payment_provider
      AND pmt.file_tx_region = sku.file_tx_region

WHERE sku.sku_id IS NULL   -- only insert genuinely new SKUs
  AND pmt.pmt_channel IN ('Amazon','Google','Roku')
  AND pmt.vendor_sku IS NOT NULL
  AND CASE WHEN pmt.pmt_channel = 'Amazon'
               THEN pmt.record_type = 'Charge' AND pmt.type = 'Subscription'
           ELSE TRUE END
  AND CASE WHEN pmt.pmt_channel = 'Google'
               THEN pmt.record_type = 'Charged' AND pmt.type ILIKE 'subscription'
           ELSE TRUE END
  AND CASE WHEN pmt.pmt_channel = 'Roku'
               THEN pmt.record_type IN ('Purchase','UpgradeSale')
           ELSE TRUE END

GROUP BY pmt.pmt_channel, pmt.vendor_sku, pmt.file_tx_region, pmt.type

UNION ALL

-- ── Block 2: Apple (separate source table) ─────────────────────────────────
SELECT
    'Apple'                                AS payment_provider,

    CASE pmt.file_tx_region
        WHEN 'AMER'   THEN 'go'
        WHEN 'EMEA'   THEN 'dplay'
        WHEN 'APAC'   THEN 'streaming_apac'
        WHEN 'MAX'    THEN 'streaming'
        WHEN 'MAXTR'  THEN 'streaming'
        WHEN 'CNN'    THEN 'streaming'
        WHEN 'ESP'    THEN 'eurosport'
        WHEN 'MT'     THEN 'motortrend'
        WHEN 'INDIA'  THEN 'streaming_india'
        WHEN 'DPLUS'  THEN
            CASE
                WHEN pmt.sku ILIKE 'br_%'
                  OR pmt.sku ILIKE 'ca_%'
                  OR pmt.sku ILIKE 'us_%'   THEN 'go'
                WHEN pmt.sku ILIKE 'de.%'
                  OR pmt.sku ILIKE 'uk_%'   THEN 'dplay'
                ELSE 'go'
            END
    END                                    AS realm,

    CASE
        WHEN pmt.file_tx_region = 'MAX'
         AND (pmt.sku ILIKE '%.us%' OR pmt.sku ILIKE 'stream.%')    THEN 'amer'
        WHEN pmt.file_tx_region = 'MAX'
         AND (pmt.sku ILIKE '%latam%' OR pmt.sku ILIKE '%.lat.%')   THEN 'latam'
        WHEN pmt.file_tx_region = 'MAX'  AND pmt.sku ILIKE '%emea%' THEN 'emea'
        WHEN pmt.file_tx_region = 'MAX'  AND pmt.sku ILIKE '%apac%' THEN 'apac'
        WHEN pmt.file_tx_region = 'MAX'                              THEN 'amer'
        WHEN pmt.file_tx_region = 'CNN'                              THEN 'cnn'
        ELSE NULL
    END                                    AS source_region,

    pmt.file_tx_region,
    pmt.sku                                AS sku_id,

    CASE
        WHEN pmt.sku ILIKE '%annual%'  THEN 'YEAR'
        WHEN pmt.sku ILIKE '%.year%'   THEN 'YEAR'
        WHEN pmt.sku ILIKE '%12month%' THEN 'YEAR'
        WHEN pmt.sku ILIKE '%monthly%' THEN 'MONTH'
        WHEN pmt.sku ILIKE '%month%'   THEN 'MONTH'
        ELSE 'MONTH'
    END                                    AS payment_period,

    CASE
        WHEN pmt.sku ILIKE '%premium%' THEN 'premium'
        WHEN pmt.sku ILIKE '%svod%'    THEN 'ad_free'
        WHEN pmt.sku ILIKE '%avod%'    THEN 'ad_lite'
        WHEN pmt.sku ILIKE '%lite%'    THEN 'ad_lite'
        ELSE 'ad_free'
    END                                    AS tier_type,

    CASE
        WHEN pmt.file_tx_region = 'APAC'            THEN 'N'
        WHEN pmt.sku ILIKE '%ppv%'                  THEN 'N'
        WHEN pmt.sku ILIKE '%golftv%'               THEN 'N'
        WHEN pmt.sku ILIKE '%.foodnetwork.%'        THEN 'N'
        WHEN SUM(ABS(pmt.customer_price) * pmt.units) != 0 THEN 'Y'
        ELSE 'N'
    END                                    AS included_in_accrual_yn,

    0                                      AS partner_revenue_share,
    MIN(pmt.end_date)                      AS sku_create_timestamp,
    'Y'                                    AS auto_generated_yn

FROM facts.ft_apple_sales_reports pmt
LEFT JOIN reference.iap_tier_type_sku sku
       ON pmt.sku          = sku.sku_id
      AND 'Apple'           = sku.payment_provider
      AND pmt.file_tx_region = sku.file_tx_region

WHERE sku.sku_id IS NULL
  AND pmt.subscription IN ('Renewal','New')
  AND pmt.sku IS NOT NULL

GROUP BY pmt.sku, pmt.file_tx_region

ORDER BY payment_provider, realm, source_region, file_tx_region,
         sku_id, payment_period, tier_type, included_in_accrual_yn;

-- ---------------------------------------------------------------------------
-- Post-insert: flip auto-generated SKUs to eligible once they show revenue
-- ---------------------------------------------------------------------------

-- Apple SKUs that now have non-zero sales
UPDATE reference.iap_tier_type_sku sku
SET included_in_accrual_yn = 'Y'
FROM (
    SELECT SUM(ABS(customer_price) * units) AS revenue_sum,
           apple_sku,
           file_tx_region
    FROM facts.ft_apple_sales_reports
    WHERE subscription IN ('Renewal','New')
    GROUP BY apple_sku, file_tx_region
) pmt
WHERE sku.sku_id              = pmt.apple_sku
  AND sku.file_tx_region      = pmt.file_tx_region
  AND sku.auto_generated_yn   = 'Y'
  AND sku.included_in_accrual_yn = 'N'
  AND pmt.revenue_sum > 0
  AND sku.file_tx_region IN ('AMER','MAX','DPLUS','EMEA','ESP','CNN','MAXTR','INDIA')
  AND sku.sku_id NOT ILIKE '%.foodnetwork.%'
  AND sku.sku_id NOT ILIKE '%golftv%'
  AND sku.sku_id NOT ILIKE '%ppv%';

-- IAP channels (Amazon / Google / Roku)
UPDATE reference.iap_tier_type_sku sku
SET included_in_accrual_yn = 'Y'
FROM (
    SELECT SUM(ABS(main_amount)) AS revenue_sum,
           vendor_sku,
           file_tx_region
    FROM facts.ft_payments
    WHERE pmt_channel IN ('Amazon','Google','Roku')
    GROUP BY vendor_sku, file_tx_region
) pmt
WHERE sku.sku_id              = pmt.vendor_sku
  AND sku.file_tx_region      = pmt.file_tx_region
  AND sku.auto_generated_yn   = 'Y'
  AND sku.included_in_accrual_yn = 'N'
  AND pmt.revenue_sum > 0
  AND sku.file_tx_region IN ('AMER','MAX','DPLUS','EMEA','ESP','CNN','MAXTR','INDIA')
  AND sku.sku_id NOT ILIKE '%.foodnetwork.%'
  AND sku.sku_id NOT ILIKE '%golftv%'
  AND sku.sku_id NOT ILIKE '%ppv%';
