# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gold.fact_claims AS
# MAGIC SELECT
# MAGIC     c.claim_id,
# MAGIC     c.policy_id,
# MAGIC     c.claim_number,
# MAGIC     c.claim_type,
# MAGIC     c.cause_of_loss_code,
# MAGIC     c.cause_of_loss_desc,
# MAGIC     c.cause_of_loss,
# MAGIC     c.incident_date,
# MAGIC     c.reported_date,
# MAGIC     c.claim_reporting_delay_days,
# MAGIC     c.estimated_loss_amount,
# MAGIC     c.is_large_claim,
# MAGIC     c.policy_active,
# MAGIC
# MAGIC     -- Weather enrichment
# MAGIC     w.weather_claim_match,
# MAGIC     w.is_severe_weather,
# MAGIC     w.severity_norm,
# MAGIC
# MAGIC     -- Fraud enrichment
# MAGIC     f.fraud_score,
# MAGIC     f.fraud_severity,
# MAGIC     f.fraud_flag
# MAGIC
# MAGIC FROM silver.slv_claims_header_business_v2 c
# MAGIC
# MAGIC LEFT JOIN silver.slv_weather_business_v2 w
# MAGIC     ON c.claim_id = w.claim_id
# MAGIC
# MAGIC LEFT JOIN silver.slv_fraud_scores_business_v2 f
# MAGIC     ON c.claim_id = f.claim_id;
# MAGIC
