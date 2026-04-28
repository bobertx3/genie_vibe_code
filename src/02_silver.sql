-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Silver Layer - Cleansed & Enriched
-- MAGIC Data quality constraints, type casting, and dimensional enrichment.

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW silver_transactions (
  CONSTRAINT valid_transaction_id EXPECT (transactionID IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_customer_id EXPECT (customerID IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_quantity EXPECT (quantity > 0) ON VIOLATION DROP ROW,
  CONSTRAINT valid_total_price EXPECT (totalPrice > 0) ON VIOLATION DROP ROW
)
COMMENT "Cleaned and validated sales transactions enriched with customer details"
AS
SELECT
  t.transactionID,
  t.customerID,
  t.franchiseID,
  t.dateTime AS transaction_timestamp,
  DATE(t.dateTime) AS transaction_date,
  YEAR(t.dateTime) AS transaction_year,
  MONTH(t.dateTime) AS transaction_month,
  t.product,
  t.quantity,
  CAST(t.unitPrice AS DOUBLE) AS unit_price,
  CAST(t.totalPrice AS DOUBLE) AS total_price,
  t.paymentMethod AS payment_method,
  c.first_name,
  c.last_name,
  c.city AS customer_city,
  c.state AS customer_state,
  c.country AS customer_country
FROM LIVE.bronze_sales_transactions t
LEFT JOIN LIVE.bronze_sales_customers c
  ON t.customerID = c.customerID;

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW silver_franchises (
  CONSTRAINT valid_franchise_id EXPECT (franchiseID IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT "Enriched franchise data joined with supplier information"
AS
SELECT
  f.franchiseID,
  f.name AS franchise_name,
  f.city AS franchise_city,
  f.district AS franchise_district,
  f.country AS franchise_country,
  f.size AS franchise_size,
  f.longitude,
  f.latitude,
  s.supplierID,
  s.name AS supplier_name,
  s.ingredient AS supplier_ingredient,
  s.approved AS supplier_approved
FROM LIVE.bronze_sales_franchises f
LEFT JOIN LIVE.bronze_sales_suppliers s
  ON f.supplierID = s.supplierID;

