# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gold.dim_fraud AS
# MAGIC SELECT
# MAGIC     claim_id,
# MAGIC     fraud_score,
# MAGIC     risk_category,
# MAGIC     flags_str,
# MAGIC     fraud_severity,
# MAGIC     fraud_flag
# MAGIC FROM silver.slv_fraud_scores_business_v2;
# MAGIC
