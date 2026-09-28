# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Gold — dim_weather (1 row per weather event)
# MAGIC FIX: was `CREATE TABLE` (failed on every re-run) and `SELECT *` from a table that had one row per event **per claim**.

# COMMAND ----------

# MAGIC %run ../../configs/common/common_config

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gold.dim_weather AS
# MAGIC SELECT
# MAGIC     xxhash64(event_id)      AS weather_sk,
# MAGIC     event_id,
# MAGIC     event_date,
# MAGIC     postcode,
# MAGIC     state,
# MAGIC     weather_event,
# MAGIC     severity_norm           AS severity,
# MAGIC     is_severe_weather,
# MAGIC     current_timestamp()     AS gold_load_ts
# MAGIC FROM silver.slv_weather_business;
