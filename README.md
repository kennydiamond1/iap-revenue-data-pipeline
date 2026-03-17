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

## Skills Demonstrated

- Multi-provider ETL pipeline design on Amazon Redshift
- Incremental load patterns with idempotency and MTD supersession
- ISO 4217 multi-currency normalisation
- Materialized view design for revenue accrual/deferral accounting
- Automated SKU classification with pattern-matching rules
- Pipeline observability via ingestion monitoring MVs
- Python orchestration with audit trail and error handling
