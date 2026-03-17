# Architecture Notes

## Data Flow

```
┌─────────────────────────────────────────────────────────────┐
│                        S3 Data Lake                         │
│  apple/  google/  amazon/  roku/  direct_billing/           │
└──────────────────────────┬──────────────────────────────────┘
                           │  S3 COPY (Redshift)
                           ▼
┌─────────────────────────────────────────────────────────────┐
│                   Raw Staging Schema                        │
│  apple_sales_raw   google_sales_raw   amazon_sales_raw      │
│  roku_transactions_raw   billing_transactions_raw           │
│  (all VARCHAR columns – no transforms at this layer)        │
└──────────────────────────┬──────────────────────────────────┘
                           │  Type-cast + normalise
                           ▼
┌─────────────────────────────────────────────────────────────┐
│                  Typed Staging Schema                       │
│  apple_sales   google_sales   amazon_sales                  │
│  roku_transactions   billing_transactions                   │
│  (TIMESTAMP, NUMERIC, INTEGER; timezone-normalised)         │
└──────────────────────────┬──────────────────────────────────┘
                           │  Provider-specific mapping
                           ▼
┌─────────────────────────────────────────────────────────────┐
│                  facts.ft_payments                          │
│  Unified payment fact table across all providers            │
│  SORTKEY: (utc_timestamp, pmt_channel, file_tx_region)      │
└──────────┬──────────────────────────────┬───────────────────┘
           │                              │
           ▼                              ▼
┌──────────────────────┐   ┌─────────────────────────────────┐
│  dimensions.         │   │  ingestion_monitoring.mv_*      │
│  iap_accrual_deferral│   │                                 │
│                      │   │  Tracks counts at each layer:   │
│  • Exchange rates    │   │  collection → raw → staging →   │
│  • VAT extraction    │   │  fact. Surfaces variance %      │
│  • Commission calc   │   │  for data ops alerting.         │
│  • Daily rate        │   └─────────────────────────────────┘
│    (deferral)        │
└──────────────────────┘
```

## Design Decisions

### Why delete-insert instead of MERGE/UPSERT?
Redshift does not support native `UPSERT`. The delete-insert pattern on `file_tx_id` is idempotent and simpler to reason about than staging-table-based MERGE workarounds.

### Why separate raw and typed staging tables?
Raw tables preserve exactly what the provider sent — useful for debugging and reprocessing. Typed tables fail fast on bad data, keeping bad rows out of the fact table.

### Why materialized views for accrual/deferral?
The accrual view joins fact data with exchange rates, VAT tables, commission rates, and SKU metadata. Materialising it avoids recomputing expensive joins on every analytics query. It is refreshed as part of the pipeline run.

### MTD Supersession Logic
Amazon delivers rolling month-to-date files. Each new file for a given realm+month contains all prior transactions plus the latest day. The pipeline:
1. Identifies all prior files for the same realm+month from the audit trail.
2. Deletes their rows from the fact table.
3. Marks them `SUPERSEDED` in the audit trail.
4. Inserts rows from the new file.

This keeps the fact table to a single canonical set of rows per realm+month.

## Reference Tables

| Table | Purpose |
|---|---|
| `reference.iap_tier_type_sku` | SKU → tier type, payment period, accrual eligibility |
| `reference.commission_rates` | Company × provider × area → commission % |
| `reference.fallback_country_code` | Currency → default country (for missing geo data) |
| `meta.avg_daily_exchange_rate` | Daily FX rates for USD conversion |
| `meta.vat_by_country` | VAT rates by country code |
