# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Gold — dim_claim (1 row per claim)
# MAGIC Changes: surrogate key `claim_sk`; policy attributes removed (they belong in `dim_policy`, joined via `fact_claims.policy_sk`);
# MAGIC **PII**: `claimant_name` / `claimant_contact` are no longer exposed — `claimant_key` is a SHA-256 hash so analysts can still
# MAGIC spot repeat claimants (useful for fraud) without seeing names. On Azure you can alternatively keep the column and
# MAGIC apply a Unity Catalog column mask.

# COMMAND ----------

# MAGIC %run ../../configs/common/common_config

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gold.dim_claim AS
# MAGIC SELECT
# MAGIC     xxhash64(claim_id)                                AS claim_sk,
# MAGIC     claim_id,
# MAGIC     claim_number,
# MAGIC     claim_type,
# MAGIC     claim_status,
# MAGIC     cause_of_loss_code,
# MAGIC     cause_of_loss_desc,
# MAGIC     cause_of_loss,
# MAGIC     reported_channel,
# MAGIC     loss_location,
# MAGIC     loss_postcode,
# MAGIC     loss_state,
# MAGIC     suburb                                            AS loss_suburb,
# MAGIC     region                                            AS loss_region,
# MAGIC     risk_zone                                         AS loss_risk_zone,
# MAGIC     weather_events,
# MAGIC     sha2(lower(trim(claimant_name)), 256)             AS claimant_key,
# MAGIC     current_timestamp()                               AS gold_load_ts
# MAGIC FROM silver.slv_claims_header_business;
