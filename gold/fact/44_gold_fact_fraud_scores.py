# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Gold — fact_fraud_scores (grain: 1 row per scored claim)

# COMMAND ----------

# MAGIC %run ../../configs/common/common_config

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gold.fact_fraud_scores AS
# MAGIC SELECT
# MAGIC     xxhash64(f.claim_id)                                             AS claim_sk,
# MAGIC     CASE WHEN f.policy_id IS NOT NULL THEN xxhash64(f.policy_id) END AS policy_sk,
# MAGIC     COALESCE(CAST(date_format(f.incident_date, 'yyyyMMdd') AS INT), -1)      AS incident_date_key,
# MAGIC     f.claim_id,
# MAGIC     f.fraud_score,
# MAGIC     f.fraud_severity,
# MAGIC     f.fraud_flag,
# MAGIC     f.risk_category              AS fraud_risk_category,
# MAGIC     f.flags_str                  AS fraud_flags,
# MAGIC     current_timestamp()          AS gold_load_ts
# MAGIC FROM silver.slv_fraud_scores_business f;
