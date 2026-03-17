# IAP Revenue Data Pipeline

A production-style data engineering pipeline for ingesting, normalising, and aggregating **In-App Purchase (IAP) revenue data** from multiple app-store and direct-billing providers into a centralised Amazon Redshift data warehouse.

---

## Overview

Streaming and subscription businesses receive sales and transaction reports from each distribution platform in different formats, currencies, and cadences. This pipeline provides a unified, auditable path from raw provider files through to analytics-ready fact tables and accrual/deferral dimensions.

```
S3 (raw files)
     │
     ▼
Raw Staging Tables     ← provider-specific, varchar columns, no transforms
     │
     ▼
Typed Staging Tables   ← cast to correct types, timezone normalised, region-tagged
     │
     ▼
facts.ft_payments      ← unified payment fact table across all providers
     │
     ├──▶ dimensions.iap_accrual_deferral   (MV: straight-line revenue deferral)
     └──▶ ingestion_monitoring.mv_*         (MV: pipeline health & variance metrics)
```

---

## Providers Supported

| Provider | Source Report | Staging Table | Notes |
|---|---|---|---|
| Apple App Store | Sales/Trends TSV | `staging_data.apple_sales` | Date parsing `MM/DD/YYYY` |
| Google Play | Earnings CSV | `staging_data.google_sales` | Multi-region (AMER, EMEA, APAC…) |
| Amazon Appstore | Sales CSV | `staging_data.amazon_sales` | Month-to-date deduplication |
| Roku Channel Store | Transaction report | `staging_data.roku_transactions` | `Purchase` + `UpgradeSale` types |
| Direct Billing | Transaction JSON/CSV | `staging_data.billing_transactions` | Minor-unit currency normalisation |

---

## Key Technical Patterns

### Incremental Load with Idempotency
Every load is keyed on a `file_tx_id`. The staging and fact load scripts follow a **delete-insert** pattern — safe to re-run without double-counting.

### Month-to-Date File Supersession (Amazon)
Amazon provides rolling MTD files; each new day's file supersedes the previous. The pipeline detects earlier files for the same realm+month and marks them `SUPERSEDED` in the audit trail, removing their rows from the fact table.

### Multi-Currency Minor-Unit Normalisation
Provider APIs submit amounts as integers in the currency's minor unit. The pipeline applies ISO 4217 exponent rules:

| Currency type | Exponent | Example |
|---|---|---|
| Standard (USD, EUR, GBP…) | ÷ 100 | 1099 → 10.99 |
| 3-decimal (KWD, BHD, OMR…) | ÷ 1000 | 1500 → 1.500 |
| 0-decimal (JPY, KRW, CLP…) | ÷ 1 | 500 → 500 |

See [`python/currency_utils.py`](python/currency_utils.py) for the standalone utility.

### Straight-Line Revenue Deferral
The `iap_accrual_deferral` materialized view calculates the number of days in each subscription period (monthly or annual) and derives a **daily revenue rate** for accrual accounting:

```sql
revenue / days_in_period AS daily_revenue_rate
```

### SKU Auto-Classification
New product SKUs discovered in incoming files are automatically classified into the tier-type reference table using pattern-matching rules on the SKU string (e.g. `%annual%` → `YEAR`, `%lite%` → `ad_lite`).

### Ingestion Monitoring
A 7-query UNION ALL materialized view tracks row counts at each pipeline layer — collection → raw staging → final staging → fact — and surfaces a `variance` metric to catch missed or partial loads.

---

## Repository Structure

```
iap-revenue-pipeline/
├── sql/
│   ├── staging/
│   │   ├── apple_sales_staging.sql          # Apple raw → typed staging
│   │   ├── amazon_sales_staging.sql         # Amazon raw → typed staging
│   │   ├── google_roku_sales_staging.sql    # Google + Roku staging
│   │   └── billing_transactions_staging.sql # Direct-billing + currency normalisation
│   ├── facts/
│   │   └── ft_payments.sql                  # Unified payment fact table DDL + load
│   ├── materialized_views/
│   │   ├── iap_accrual_deferral_mv.sql      # Revenue deferral dimension
│   │   └── ingestion_monitoring_mv.sql      # Pipeline health tracking MV
│   └── reference/
│       └── sku_auto_classification.sql      # Auto-insert new SKUs
├── python/
│   ├── iap_pipeline_orchestrator.py         # End-to-end pipeline runner
│   └── currency_utils.py                    # ISO 4217 minor-unit normalisation
└── docs/
    └── architecture.md
```

---

## Getting Started

### Prerequisites
- Python 3.10+
- Amazon Redshift (or compatible PostgreSQL)
- AWS credentials with S3 + Redshift access

### Install dependencies
```bash
pip install psycopg2-binary boto3 pyyaml
```

### Configure
Copy and edit the config template:
```bash
cp config.example.yaml config.yaml
# Edit database connection details
```

Or set environment variables:
```bash
export DB_HOST=your-cluster.redshift.amazonaws.com
export DB_NAME=analytics
export DB_USER=pipeline_user
export DB_PASSWORD=...
```

### Run the pipeline for a single file
```bash
python python/iap_pipeline_orchestrator.py \
  --provider google \
  --region AMER \
  --file-path s3://your-bucket/google/2024-01-15_google_amer_sales.csv \
  --file-tx-id $(uuidgen)
```

### Test the currency utility
```bash
python python/currency_utils.py
```

---

## SQL Conventions

- All staging tables use `VARCHAR` for raw columns; typed tables use Redshift-native types (`TIMESTAMP`, `NUMERIC(20,6)`, `INTEGER`) with `ENCODE AZ64` for numerics.
- `{{file_tx_id}}` and `{{region}}` are runtime token placeholders substituted by the orchestrator.
- All fact loads follow: **DELETE existing rows** → **INSERT new rows** → **UPDATE audit trail**.

---

## Audit Trail

Every file load writes to `audit_trail.load_audit_trail`:

| Column | Description |
|---|---|
| `file_tx_id` | Unique ID for this file load |
| `status` | `IN_PROGRESS` → `LOADED_FACT` or `LOAD_ERROR` or `SUPERSEDED` |
| `record_count_staging` | Rows written to staging |
| `load_date_time` | Timestamp of last update |

---

## Analytics & Reporting Layer

In addition to the ETL pipeline, this project includes a library of operational analytics queries covering revenue assurance, subscription tracking, payment reconciliation, and SOX reporting. All queries target Amazon Redshift and are parameterised for use in a reporting tool (parameters use `@1`, `@2` … notation).

### Subscription Trend Reporting (`WBD-SUBS-*`)

Month-over-month subscription trend KPIs broken out by payment provider — covering IAP channels (Apple, Google, Amazon, Roku, Samsung, Vizio) and direct-billing providers (Adyen, PayPal, Stripe). Each report computes:

```sql
-- MoM % change pattern used across all trend reports
SELECT ROUND((current_month - prior_month) * 100 / prior_month, 2) AS kpi_value,
       current_month - prior_month                                   AS trend_value
FROM ( ... )
```

Key SQL technique: a **UNION of two aggregations** — one counting starts (`start_timestamp`) and one counting terminations (`termination_timestamp`) — rolled up across realm partitions using `CASE` guards for wildcard vs. specific filter parameters.

Realm-awareness is handled via a `CASE`-based filter that maps special tokens (`%` = all, `$` = all Discovery realms, `^` = all MAX/streaming realms) to subscription realm lists, allowing a single query to serve many dashboard filters.

---

### Active Subscriber Counts (`WBD-SUBS-DISCOVERY-ACTIVE-*`)

Point-in-time active subscriber snapshots from `facts.ft_latest_subscriptions`, filtered by payment provider, realm, source region, and status. Distinct subscriber IDs are counted using realm-aware logic:

```sql
CASE
    WHEN subscr_realm_part IN ('dplay', 'dplusapac', ...) THEN COUNT(DISTINCT subscription_id)
    WHEN subscr_realm_part IN ('bolt')                    THEN COUNT(DISTINCT global_subscription_id)
    ELSE 0
END
```

---

### Fact Table Ingestion Monitoring (`BOLT-*-INGESTION-*`, `GOOGLE-*-INGESTION`, `AMAZON-MTD-TRACKING`, etc.)

A family of multi-level drill-down reports that track data completeness as files move through the pipeline layers. Each report uses the same **UNION ALL of sparse-column sub-queries** pattern (7 queries, one per pipeline layer), allowing `GROUP BY` aggregation to produce a single summary row per file:

| Metric | Description |
|---|---|
| `files_collected` | Files seen in collection audit trail |
| `records_audit_trail` | Expected row count |
| `raw_file_count / raw_transaction_count` | Rows in raw staging |
| `final_file_count / final_transaction_count` | Rows in typed staging |
| `fact_file_count / fact_transaction_count` | Rows in fact table |
| `overwritten` | Rows replaced by later MTD files |
| `pct_fact` | % of expected rows that reached the fact table |
| `variance` | Adjusted % including superseded/ignored files |

Reports exist at two levels — Level 1 (daily rollup) and Level 2 (drill-down to individual file). Covered data sources: Bolt transactions, Bolt subscriptions V2, Bolt priceplans, Bolt products, Google payments, Google sales, Amazon sales, Stripe payments, Sonic subscriptions, Sonic transactions.

---

### Payment Reconciliation — Matched vs. Unmatched (`ADYEN-*`, `PAYPAL-*`, `GOOGLE-UNMATCHED-*`, `AMAZON-UNMATCHED-*`)

Revenue assurance reports that join `facts.ft_payments` (payment provider records) against `facts.ft_charges` (internal subscription billing records) to surface unmatched transactions. Three-level drill-down hierarchy:

- **Level 0** — aggregate matched/unmatched counts across a date range
- **Level 1** — daily counts with % unmatched
- **Level 2** — breakdown by merchant account, record type, product
- **Level 3** — row-level detail with PSP references for investigation

**Adyen matching** translates internal transaction types to Adyen record types, then applies three separate join strategies (Authorised, SentForRefund, Chargeback) with different key columns per strategy. Motortrend/Foodnetwork accounts use a simplified refund match (no merchant reference).

**PayPal matching** joins on `psp_reference` + `invoice_number` with transaction event code filtering (`T0003`, `T0006`, `T1107`). HBOUS (HBO US) accounts are separated from non-HBOUS to account for different matching logic.

**Google matching** joins on `psp_reference` = `vendor_reference_id`, filtering out PGA Tour product IDs which use different reconciliation.

**Amazon matching** uses a more complex strategy: a `LISTAGG` of vendor reference IDs per subscription, with a date window (±1 day) to account for timing differences between provider reports and internal records.

---

### Subscribers Due (Billing Rules) (`SUBS-DUE-*`, `BOLT-SUBS-DUE-*`)

Reports that identify subscribers who were due to be billed but have no corresponding charge record. Pattern: join active subscriptions (`ft_subscriptions` or `ft_latest_subscriptions`) to price plans, then LEFT JOIN to charges filtered to `transaction_type IN ('RECURRING', 'FIRST')`. Rows where the charge join returns NULL are "not billed when due."

Variants exclude RUB (Ruble) currency priceplans, and provider-specific versions cover Apple and Bolt (direct-billing). The Bolt version additionally joins to staging price plan data to resolve provider and payment period.

---

### Affiliate Revenue Reports (`ADYEN-AFFILIATES`, `PAYPAL-AFFILIATES`, `BOLT-AFFILIATES-*`)

Settled transaction reports for subscriptions originating from device manufacturer affiliates (e.g. SONY, XBOX, VIZIO, LGE, Samsung). The Bolt affiliate report calculates pro-rated revenue share:

```sql
CASE
    WHEN months_since_start >= payment_cap_months                         THEN 0
    WHEN months_since_start = payment_cap_months - 1
         AND transaction_type = 'UPGRADE' AND payment_period = 'MONTH'   THEN revenue_share
         * (max_days_in_payment_cap - days_since_start)
         / datediff(day, original_chg_date, dateadd(month, 1, original_chg_date))
    ...
    ELSE revenue_share
END AS revenue_share_multiplier
```

This handles partial-period revenue share for upgrades near the cap boundary and annual plan proration.

---

### HBO US SOX Reports (`01-` through `17-US-*`)

A suite of SOX-compliant subscription analytics reports for the US market, covering:

- **Active subscribers** — current snapshot grouped by product plan, VOD type, price plan, promo, billing cycle anchor
- **Acquired subscribers** — new subscription counts with upgrade/downgrade/true-expire breakdown
- **Renewals summary** — renewal, churn (voluntary/involuntary), and switch counts per invoice date
- **Free trial conversions** — free trial signups vs. conversions vs. cancellations
- **Cancellations** — cancellation reason classification (hard decline, soft decline, refund, auto-renew-off, chargeback)
- **Payment failures** — up to 10 retry attempt success/failure counts by invoice date and product plan
- **Oracle transactions extract** — formatted revenue extract for ERP system ingestion
- **Plan switch transitions** — upgrade/downgrade event counts

These reports involve complex multi-CTE queries spanning `hbo_purchases_us`, `hbo_products_us`, `hbo_promotions_us`, and `hbo_charges_us` schemas, with epoch timestamp conversion (`timestamp 'epoch' + seconds * interval '1 second'`) and both UTC and US/Pacific timezone variants.

---

### Google/Adyen Settlement Reconciliation (`GOOGLE-STL-001`, `AMAZON-STL-001`, `ADYEN-TIME-DIFF`)

Cross-source amount reconciliation comparing charge amounts from `ft_charges` against payment amounts from `ft_payments` by date and currency, with USD conversion via a daily exchange rate table. The Adyen time-difference report calculates percentile distributions of the delay between Authorised and Settled/Refunded records.

---

### New Product / SKU Monitoring (`IAP-NEW-PRODUCT-001`)

Alert query that surfaces newly discovered SKUs in the accrual/deferral dimension views that are not yet present in the SKU reference table — used to trigger the auto-classification process.

---

### Case Management Dashboards (`PMT-CASE-MGMT-*`, `SUBS-CASE-MGMT-*`)

Operational dashboards that query `dimensions.d_payments_states` and `dimensions.d_latest_subscriptions_states` — state machine dimension tables that classify each payment/subscription record into a workflow state. Reports surface counts by state/date at Level 1, with record-level drill-down at Level 2.

---

## Skills Demonstrated

**Data Engineering / ETL**
- Multi-provider ETL pipeline design on Amazon Redshift
- Incremental load patterns with idempotency and MTD file supersession
- ISO 4217 multi-currency minor-unit normalisation
- Materialized view design for revenue accrual/deferral accounting
- Automated SKU classification with pattern-matching rules
- Pipeline observability and data completeness monitoring
- Python pipeline orchestration with audit trail and error handling

**Advanced SQL (Redshift)**
- Multi-level drill-down reporting using parameterised UNION ALL patterns
- Cross-source payment reconciliation with multi-strategy join logic
- Epoch timestamp conversion and multi-timezone reporting
- Percentile distribution analysis (`percentile_cont`)
- Revenue share proration with pro-rated period calculations
- Wildcard filter patterns using special token CASE guards (`%`, `$`, `^`)
- Window functions for deduplication and state tracking
- SOX-compliant subscription lifecycle reporting (renewals, churn, conversions, cancellations)

**Revenue Assurance**
- Payment matched vs. unmatched analysis (Adyen, PayPal, Google, Amazon)
- Subscriber billing compliance (subscribers due but not billed)
- Affiliate revenue accounting with payment cap logic
- Accrual/deferral straight-line revenue recognition
- Exchange rate application for multi-currency USD reporting
