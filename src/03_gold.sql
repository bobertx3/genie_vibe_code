-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Gold Layer - Business Aggregates
-- MAGIC Revenue metrics by franchise and a summary metric view for executive reporting.

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW gold_franchise_revenue
COMMENT "Revenue and transaction metrics aggregated by franchise and month"
AS
SELECT
  t.franchiseID,
  f.franchise_name,
  f.franchise_city,
  f.franchise_country,
  f.franchise_size,
  DATE_TRUNC('MONTH', t.transaction_date) AS revenue_month,
  COUNT(DISTINCT t.transactionID) AS total_transactions,
  COUNT(DISTINCT t.customerID) AS unique_customers,
  SUM(t.quantity) AS total_items_sold,
  ROUND(SUM(t.total_price), 2) AS total_revenue,
  ROUND(AVG(t.total_price), 2) AS avg_transaction_value
FROM LIVE.silver_transactions t
LEFT JOIN LIVE.silver_franchises f
  ON t.franchiseID = f.franchiseID
GROUP BY ALL;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Metric View - Monthly Revenue KPIs
-- MAGIC Executive-level summary of key revenue metrics across all franchises.

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW gold_revenue_metrics
COMMENT "Key revenue KPIs aggregated monthly for executive reporting"
AS
SELECT
  revenue_month,
  SUM(total_revenue) AS monthly_revenue,
  SUM(total_transactions) AS monthly_transactions,
  SUM(unique_customers) AS monthly_unique_customers,
  SUM(total_items_sold) AS monthly_items_sold,
  ROUND(SUM(total_revenue) / NULLIF(SUM(total_transactions), 0), 2) AS avg_order_value,
  ROUND(SUM(total_revenue) / NULLIF(SUM(unique_customers), 0), 2) AS revenue_per_customer
FROM LIVE.gold_franchise_revenue
GROUP BY revenue_month
ORDER BY revenue_month;

