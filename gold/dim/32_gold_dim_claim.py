# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gold.dim_claim AS
# MAGIC SELECT
# MAGIC     claim_id,
# MAGIC     claim_number,
# MAGIC     claim_type,
# MAGIC     cause_of_loss_code,
# MAGIC     cause_of_loss_desc,
# MAGIC     cause_of_loss,
# MAGIC     incident_date,
# MAGIC     reported_date,
# MAGIC     claimant_name,
# MAGIC     claimant_contact,
# MAGIC     loss_location,
# MAGIC     loss_postcode,
# MAGIC     loss_state,
# MAGIC     estimated_loss_amount,
# MAGIC     reported_channel,
# MAGIC     agent_id,
# MAGIC     customer_id,
# MAGIC     lob,
# MAGIC     policy_id,
# MAGIC     policy_number,
# MAGIC     product_code,
# MAGIC     product_name,
# MAGIC     risk_address_line1,
# MAGIC     risk_address_line2,
# MAGIC     risk_city,
# MAGIC     risk_country,
# MAGIC     risk_postcode,
# MAGIC     risk_state,
# MAGIC     sum_insured,
# MAGIC     underwriter_id,
# MAGIC     postcode,
# MAGIC     suburb,
# MAGIC     region,
# MAGIC     state,
# MAGIC     risk_zone,
# MAGIC     is_large_claim
# MAGIC FROM silver.slv_claims_header_business_v2;
# MAGIC
