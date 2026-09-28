# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Gold — fact_payments (grain: 1 row per payment)

# COMMAND ----------

# MAGIC %run ../../configs/common/common_config

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gold.fact_payments AS
# MAGIC SELECT
# MAGIC     xxhash64(p.claim_id)                                             AS claim_sk,
# MAGIC     CASE WHEN p.policy_id IS NOT NULL THEN xxhash64(p.policy_id) END AS policy_sk,
# MAGIC     COALESCE(CAST(date_format(p.payment_date, 'yyyyMMdd') AS INT), -1)      AS payment_date_key,
# MAGIC     p.payment_id,
# MAGIC     p.claim_id,
# MAGIC     p.payment_amount,
# MAGIC     p.currency,
# MAGIC     p.payment_type,
# MAGIC     p.payment_method,
# MAGIC     p.payment_status,
# MAGIC     p.payment_delay_days,
# MAGIC     p.is_settlement,
# MAGIC     p.is_reimbursement,
# MAGIC     current_timestamp()          AS gold_load_ts
# MAGIC FROM silver.slv_payments_business p;
