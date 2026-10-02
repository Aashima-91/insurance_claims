# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Gold — dim_date
# MAGIC Continuous calendar (every day from 1 Jan of the earliest year to 31 Dec of the latest year, capped at today + 10 years),
# MAGIC plus an **unknown member** row `date_key = -1` that facts use when a date is missing.
# MAGIC Old version only contained dates that happened to appear in the data, so gaps broke time-series reports.

# COMMAND ----------

# MAGIC %run ../../configs/common/common_config

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gold.dim_date AS
# MAGIC WITH src AS (
# MAGIC     SELECT incident_date     AS dt FROM silver.slv_claims_header_business
# MAGIC     UNION ALL SELECT reported_date     FROM silver.slv_claims_header_business
# MAGIC     UNION ALL SELECT policy_start_date FROM silver.slv_policy_business
# MAGIC     UNION ALL SELECT policy_end_date   FROM silver.slv_policy_business
# MAGIC     UNION ALL SELECT payment_date      FROM silver.slv_payments_business
# MAGIC     UNION ALL SELECT event_date        FROM silver.slv_weather_business
# MAGIC ),
# MAGIC bounds AS (
# MAGIC     SELECT make_date(year(MIN(dt)), 1, 1)                                           AS start_dt,
# MAGIC            LEAST(make_date(year(MAX(dt)), 12, 31), date_add(current_date(), 3650))  AS end_dt
# MAGIC     FROM src WHERE dt IS NOT NULL
# MAGIC ),
# MAGIC days AS (
# MAGIC     SELECT explode(sequence(start_dt, end_dt, INTERVAL 1 DAY)) AS dt FROM bounds
# MAGIC )
# MAGIC SELECT
# MAGIC     CAST(date_format(dt, 'yyyyMMdd') AS INT)                   AS date_key,
# MAGIC     dt                                                         AS date,
# MAGIC     year(dt)                                                   AS year,
# MAGIC     quarter(dt)                                                AS quarter,
# MAGIC     month(dt)                                                  AS month,
# MAGIC     date_format(dt, 'MMMM')                                    AS month_name,
# MAGIC     day(dt)                                                    AS day,
# MAGIC     weekofyear(dt)                                             AS week_of_year,
# MAGIC     date_format(dt, 'EEEE')                                    AS day_name,
# MAGIC     dayofweek(dt) IN (1, 7)                                    AS is_weekend,
# MAGIC     CASE WHEN month(dt) >= 7 THEN year(dt) + 1 ELSE year(dt) END AS au_financial_year   -- FY ends 30 June
# MAGIC FROM days
# MAGIC UNION ALL
# MAGIC SELECT -1, CAST(NULL AS DATE), NULL, NULL, NULL, 'Unknown', NULL, NULL, 'Unknown', NULL, NULL;
