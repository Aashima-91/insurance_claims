# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gold.fact_weather_events AS
# MAGIC SELECT
# MAGIC     -- Weather event identifiers
# MAGIC     w.event_id,
# MAGIC     w.event_date,
# MAGIC     w.state,
# MAGIC     w.postcode,
# MAGIC     w.weather_event,
# MAGIC     w.severity,
# MAGIC     w.temperature_c,
# MAGIC     w.rainfall_mm,
# MAGIC     w.wind_speed_kmh,
# MAGIC
# MAGIC     -- Weather business flags
# MAGIC     w.severity_norm,
# MAGIC     w.is_severe_weather,
# MAGIC     w.weather_claim_match,
# MAGIC
# MAGIC     -- Claim context (1:1 join)
# MAGIC     c.claim_id,
# MAGIC     c.incident_date,
# MAGIC     c.reported_date,
# MAGIC     c.estimated_loss_amount,
# MAGIC     c.is_large_claim
# MAGIC
# MAGIC FROM silver.slv_weather_business_v2 w
# MAGIC LEFT JOIN silver.slv_claims_header_business_v2 c
# MAGIC     ON w.claim_id = c.claim_id;
# MAGIC
