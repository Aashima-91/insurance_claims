# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Gold — fact_claims (grain: 1 row per claim)
# MAGIC Keys to dims (`claim_sk`, `policy_sk`, `*_date_key`) + measures. Descriptive text lives in the dims.
# MAGIC NEW measures rolled up from claim lines and payments. Fraud columns folded in here (old `dim_fraud` removed —
# MAGIC a score is a measurement, not a descriptive dimension).

# COMMAND ----------

# MAGIC %run ../../configs/common/common_config

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gold.fact_claims AS
# MAGIC WITH lines AS (
# MAGIC     SELECT claim_id,
# MAGIC            COUNT(*)               AS line_count,
# MAGIC            SUM(claimed_amount)    AS total_claimed_amount,
# MAGIC            SUM(approved_amount)   AS total_approved_amount,
# MAGIC            SUM(deductible_amount) AS total_deductible_amount
# MAGIC     FROM silver.slv_claims_line_business
# MAGIC     GROUP BY claim_id
# MAGIC ),
# MAGIC pays AS (
# MAGIC     SELECT claim_id,
# MAGIC            COUNT(*)            AS payment_count,
# MAGIC            SUM(payment_amount) AS total_paid_amount
# MAGIC     FROM silver.slv_payments_business
# MAGIC     GROUP BY claim_id
# MAGIC )
# MAGIC SELECT
# MAGIC     xxhash64(c.claim_id)                                            AS claim_sk,
# MAGIC     CASE WHEN c.policy_id IS NOT NULL THEN xxhash64(c.policy_id) END AS policy_sk,
# MAGIC     COALESCE(CAST(date_format(c.incident_date, 'yyyyMMdd') AS INT), -1)      AS incident_date_key,
# MAGIC     COALESCE(CAST(date_format(c.reported_date, 'yyyyMMdd') AS INT), -1)      AS reported_date_key,
# MAGIC     c.claim_id,                                   -- degenerate key, handy for drill-through
# MAGIC     -- measures
# MAGIC     c.estimated_loss_amount,
# MAGIC     c.claim_reporting_delay_days,
# MAGIC     COALESCE(l.line_count, 0)                     AS line_count,
# MAGIC     COALESCE(l.total_claimed_amount, 0)           AS total_claimed_amount,
# MAGIC     COALESCE(l.total_approved_amount, 0)          AS total_approved_amount,
# MAGIC     COALESCE(l.total_deductible_amount, 0)        AS total_deductible_amount,
# MAGIC     COALESCE(p.payment_count, 0)                  AS payment_count,
# MAGIC     COALESCE(p.total_paid_amount, 0)              AS total_paid_amount,
# MAGIC     -- flags
# MAGIC     c.is_large_claim,
# MAGIC     c.policy_active,
# MAGIC     c.weather_claim_match,
# MAGIC     c.weather_event_count,
# MAGIC     c.is_severe_weather,
# MAGIC     -- fraud enrichment
# MAGIC     f.fraud_score,
# MAGIC     COALESCE(f.fraud_severity, 'Not scored')      AS fraud_severity,
# MAGIC     COALESCE(f.fraud_flag, FALSE)                 AS fraud_flag,
# MAGIC     current_timestamp()                           AS gold_load_ts
# MAGIC FROM silver.slv_claims_header_business c
# MAGIC LEFT JOIN lines l                           ON c.claim_id = l.claim_id
# MAGIC LEFT JOIN pays  p                           ON c.claim_id = p.claim_id
# MAGIC LEFT JOIN silver.slv_fraud_scores_business f ON c.claim_id = f.claim_id;
