# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gold.fact_fraud_scores AS
# MAGIC SELECT
# MAGIC     -- Fraud identifiers
# MAGIC     f.claim_id,
# MAGIC     f.fraud_score,
# MAGIC     f.risk_category,
# MAGIC     f.flags_str,
# MAGIC     f.fraud_severity,
# MAGIC     f.fraud_flag,
# MAGIC
# MAGIC     -- Claim context (1:1 join)
# MAGIC     c.claim_number,
# MAGIC     c.claim_type,
# MAGIC     c.incident_date,
# MAGIC     c.reported_date,
# MAGIC     c.estimated_loss_amount,
# MAGIC     c.is_large_claim,
# MAGIC     c.policy_id
# MAGIC
# MAGIC FROM silver.slv_fraud_scores_business_v2 f
# MAGIC LEFT JOIN silver.slv_claims_header_business_v2 c
# MAGIC     ON f.claim_id = c.claim_id;
# MAGIC
