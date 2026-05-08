# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS gold;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gold.dim_date AS
# MAGIC WITH all_dates AS (
# MAGIC     SELECT incident_date   AS dt FROM silver.slv_claims_header_business_v2 WHERE incident_date   IS NOT NULL
# MAGIC     UNION
# MAGIC     SELECT reported_date   AS dt FROM silver.slv_claims_header_business_v2 WHERE reported_date   IS NOT NULL
# MAGIC     UNION
# MAGIC     SELECT policy_start_date AS dt FROM silver.slv_policy_business_v2        WHERE policy_start_date IS NOT NULL
# MAGIC     UNION
# MAGIC     SELECT policy_end_date   AS dt FROM silver.slv_policy_business_v2        WHERE policy_end_date   IS NOT NULL
# MAGIC     UNION
# MAGIC     SELECT payment_date    AS dt FROM silver.slv_payments_business_v2       WHERE payment_date    IS NOT NULL
# MAGIC     UNION
# MAGIC     SELECT event_date      AS dt FROM silver.slv_weather_business_v2        WHERE event_date      IS NOT NULL
# MAGIC )
# MAGIC SELECT
# MAGIC     CAST(date_format(dt, 'yyyyMMdd') AS INT) AS date_key,
# MAGIC     dt                                   AS date,
# MAGIC     year(dt)                             AS year,
# MAGIC     month(dt)                            AS month,
# MAGIC     day(dt)                              AS day,
# MAGIC     weekofyear(dt)                       AS week_of_year,
# MAGIC     quarter(dt)                          AS quarter,
# MAGIC     date_format(dt, 'EEEE')              AS day_name,
# MAGIC     CASE WHEN dayofweek(dt) IN (1,7) THEN TRUE ELSE FALSE END AS is_weekend
# MAGIC FROM all_dates;
# MAGIC
