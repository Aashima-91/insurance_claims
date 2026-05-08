# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gold.fact_payments AS
# MAGIC SELECT
# MAGIC     -- Payment identifiers
# MAGIC     p.payment_id,
# MAGIC     p.claim_id,
# MAGIC     p.policy_id,
# MAGIC
# MAGIC     -- Payment details
# MAGIC     p.payment_date,
# MAGIC     p.payment_amount,
# MAGIC     p.payment_type,
# MAGIC     p.payment_status,
# MAGIC
# MAGIC     -- Claim context (from header)
# MAGIC     c.incident_date,
# MAGIC     c.reported_date,
# MAGIC     c.estimated_loss_amount,
# MAGIC     c.is_large_claim,
# MAGIC     c.policy_active
# MAGIC
# MAGIC FROM silver.slv_payments_business_v2 p
# MAGIC
# MAGIC LEFT JOIN silver.slv_claims_header_business_v2 c
# MAGIC     ON p.claim_id = c.claim_id;
# MAGIC
