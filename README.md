# Genie Demo - Bakehouse Pipeline

End-to-end Spark Declarative Pipeline (SDP) demo using the `samples.bakehouse` dataset.

## Architecture

```
samples.bakehouse (source)
        │
        ▼
┌─────────────────┐
│   Bronze Layer   │  Raw streaming tables (4 tables)
│  01_bronze.sql   │  sales_transactions, customers, franchises, suppliers
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   Silver Layer   │  Cleaned + enriched materialized views (2 views)
│  02_silver.sql   │  silver_transactions (w/ customer join + DQ checks)
│                  │  silver_franchises (w/ supplier join)
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   Gold Layer     │  Business aggregates (2 views)
│  03_gold.sql     │  gold_franchise_revenue (by franchise + month)
│                  │  gold_revenue_metrics (monthly KPIs)
└─────────────────┘
```

## Target
- **Catalog**: `bx4`
- **Schema**: `genie_demo`

## Deployment with DABs

```bash
# Validate the bundle
databricks bundle validate

# Deploy to dev
databricks bundle deploy

# Run the pipeline
databricks bundle run genie_demo_pipeline

# Deploy to prod
databricks bundle deploy -t prod
```

## Data Quality Constraints (Silver Layer)
- `valid_transaction_id`: transactionID IS NOT NULL
- `valid_customer_id`: customerID IS NOT NULL
- `valid_quantity`: quantity > 0
- `valid_total_price`: totalPrice > 0
- `valid_franchise_id`: franchiseID IS NOT NULL
