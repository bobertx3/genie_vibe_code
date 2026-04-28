-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Bronze Layer - Raw Ingestion
-- MAGIC Ingest raw tables from `samples.bakehouse` into streaming tables.

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE bronze_sales_transactions
COMMENT "Raw sales transactions ingested from samples.bakehouse"
AS SELECT * FROM STREAM(samples.bakehouse.sales_transactions);

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE bronze_sales_customers
COMMENT "Raw customer data ingested from samples.bakehouse"
AS SELECT * FROM STREAM(samples.bakehouse.sales_customers);

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE bronze_sales_franchises
COMMENT "Raw franchise data ingested from samples.bakehouse"
AS SELECT * FROM STREAM(samples.bakehouse.sales_franchises);

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE bronze_sales_suppliers
COMMENT "Raw supplier data ingested from samples.bakehouse"
AS SELECT * FROM STREAM(samples.bakehouse.sales_suppliers);

