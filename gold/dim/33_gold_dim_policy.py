# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Gold — dim_policy (1 row per policy)
# MAGIC Changes: surrogate key `policy_sk`; `customer_id` / `agent_id` moved here from dim_claim; street address lines dropped (PII,
# MAGIC not needed for analytics — city/state/postcode kept).

# COMMAND ----------

# MAGIC %run ../../configs/common/common_config

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gold.dim_policy AS
# MAGIC SELECT
# MAGIC     xxhash64(policy_id)     AS policy_sk,
# MAGIC     policy_id,
# MAGIC     policy_number,
# MAGIC     customer_id,
# MAGIC     agent_id,
# MAGIC     underwriter_id,
# MAGIC     product_code,
# MAGIC     product_name,
# MAGIC     lob,
# MAGIC     coverage_type,
# MAGIC     risk_category,
# MAGIC     payment_frequency,
# MAGIC     policy_status,
# MAGIC     policy_start_date,
# MAGIC     policy_end_date,
# MAGIC     policy_term_days,
# MAGIC     premium_amount,
# MAGIC     sum_insured,
# MAGIC     risk_city,
# MAGIC     risk_state,
# MAGIC     risk_postcode,
# MAGIC     risk_country,
# MAGIC     is_active               AS is_active_as_of_load,
# MAGIC     current_timestamp()     AS gold_load_ts
# MAGIC FROM silver.slv_policy_business;
