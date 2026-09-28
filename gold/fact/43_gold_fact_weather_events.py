# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Gold — fact_weather_events (grain: 1 row per weather event)
# MAGIC Measurements + how many claims were lodged for the same postcode and day.

# COMMAND ----------

# MAGIC %run ../../configs/common/common_config

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gold.fact_weather_events AS
# MAGIC SELECT
# MAGIC     xxhash64(w.event_id)          AS weather_sk,
# MAGIC     COALESCE(CAST(date_format(w.event_date, 'yyyyMMdd') AS INT), -1)       AS event_date_key,
# MAGIC     w.event_id,
# MAGIC     w.temperature_c,
# MAGIC     w.rainfall_mm,
# MAGIC     w.wind_speed_kmh,
# MAGIC     w.matched_claim_count,
# MAGIC     w.is_severe_weather,
# MAGIC     current_timestamp()           AS gold_load_ts
# MAGIC FROM silver.slv_weather_business w;
