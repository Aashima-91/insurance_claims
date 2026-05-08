# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gold.dim_policy AS
# MAGIC SELECT
# MAGIC     policy_id,
# MAGIC     policy_number,
# MAGIC     product_code,
# MAGIC     product_name,
# MAGIC     lob,
# MAGIC     payment_frequency,
# MAGIC     policy_start_date,
# MAGIC     policy_end_date,
# MAGIC     policy_status,
# MAGIC     premium_amount,
# MAGIC     risk_address_line1,
# MAGIC     risk_address_line2,
# MAGIC     risk_city,
# MAGIC     risk_country,
# MAGIC     risk_postcode,
# MAGIC     risk_state,
# MAGIC     sum_insured,
# MAGIC     underwriter_id,
# MAGIC     coverage_type,
# MAGIC     risk_category,
# MAGIC     policy_term_days,
# MAGIC     is_active
# MAGIC FROM silver.slv_policy_business_v2;
# MAGIC
