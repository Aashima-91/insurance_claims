# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Silver validation
# MAGIC
# MAGIC Changes vs old version:
# MAGIC - Uses the renamed tables (no `_v2`).
# MAGIC - Checks are **assertions**: any failure raises at the end, so a job run is marked FAILED and gold does not build
# MAGIC   on bad data. (The old notebook only printed.)
# MAGIC - Profiling output (row counts, distributions) is still printed for information.

# COMMAND ----------

# MAGIC %run ../../configs/common/common_config

# COMMAND ----------

S = SILVER_DB
dq = DQ("silver")

# ---------------------------------------------------------
# 1. Grain / duplicate keys (must be 0)
# ---------------------------------------------------------
grain = {
    "slv_claims_header_business": ["claim_id"],
    "slv_claims_line_business":   ["claim_line_id"],
    "slv_payments_business":      ["payment_id"],
    "slv_policy_business":        ["policy_id"],
    "slv_weather_business":       ["event_id"],
    "slv_fraud_scores_business":  ["claim_id"],
}
for table, keys in grain.items():
    k = ", ".join(keys)
    dq.check(f"{table}: duplicate {k}",
             f"SELECT {k}, COUNT(*) AS cnt FROM {S}.{table} GROUP BY {k} HAVING COUNT(*) > 1")

# ---------------------------------------------------------
# 2. Business tables must not gain or lose rows vs technical
# ---------------------------------------------------------
pairs = [
    ("slv_claims_header_technical", "slv_claims_header_business"),
    ("slv_claims_line_technical",   "slv_claims_line_business"),
    ("slv_payments_technical",      "slv_payments_business"),
    ("slv_policy_technical",        "slv_policy_business"),
    ("slv_weather_technical",       "slv_weather_business"),
    ("slv_fraud_scores_technical",  "slv_fraud_scores_business"),
]
for tech, biz in pairs:
    dq.check(f"{biz}: row count equals {tech}",
             f"""SELECT t.n AS tech_rows, b.n AS biz_rows
                 FROM (SELECT COUNT(*) n FROM {S}.{tech}) t, (SELECT COUNT(*) n FROM {S}.{biz}) b
                 WHERE t.n <> b.n""")

# ---------------------------------------------------------
# 3. Mandatory columns not null
# ---------------------------------------------------------
not_null = {
    "slv_claims_header_business": ["claim_id", "policy_id", "incident_date", "reported_date"],
    "slv_payments_business":      ["payment_id", "claim_id", "payment_date", "payment_amount"],
    "slv_policy_business":        ["policy_id", "policy_start_date", "policy_end_date"],
    "slv_weather_business":       ["event_id", "event_date", "postcode"],
    "slv_fraud_scores_business":  ["claim_id", "fraud_score"],
}
for table, cols in not_null.items():
    cond = " OR ".join(f"{c} IS NULL" for c in cols)
    # keys are already filtered in technical; these are warnings until your data is clean
    dq.check(f"{table}: nulls in {', '.join(cols)}", f"SELECT * FROM {S}.{table} WHERE {cond}", level="warn")

# ---------------------------------------------------------
# 4. Business rules
# ---------------------------------------------------------
dq.check("claims: incident_date after reported_date",
         f"SELECT claim_id, incident_date, reported_date FROM {S}.slv_claims_header_business WHERE claim_reporting_delay_days < 0", level="warn")
dq.check("policy: start date after end date",
         f"SELECT policy_id, policy_start_date, policy_end_date FROM {S}.slv_policy_business WHERE policy_term_days < 0", level="warn")
dq.check(f"fraud: score outside 0..{FRAUD_SCORE_MAX}",
         f"SELECT claim_id, fraud_score FROM {S}.slv_fraud_scores_business WHERE fraud_score < 0 OR fraud_score > {FRAUD_SCORE_MAX}", level="warn")
dq.check("weather: unknown severity value",
         f"""SELECT DISTINCT severity_norm FROM {S}.slv_weather_business
             WHERE severity_norm IS NOT NULL AND severity_norm NOT IN ({", ".join(f"'{v}'" for v in VALID_WEATHER_SEVERITY)})""", level="warn")
dq.check("payments: negative amount",
         f"SELECT payment_id, payment_amount FROM {S}.slv_payments_business WHERE payment_amount < 0", level="warn")

# ---------------------------------------------------------
# 5. Referential integrity
# ---------------------------------------------------------
dq.check("claims whose policy_id is not in policy",
         f"""SELECT c.claim_id, c.policy_id FROM {S}.slv_claims_header_business c
             LEFT ANTI JOIN {S}.slv_policy_business p ON c.policy_id = p.policy_id
             WHERE c.policy_id IS NOT NULL""", level="warn")
dq.check("payments whose claim_id is not in claims",
         f"""SELECT p.payment_id, p.claim_id FROM {S}.slv_payments_business p
             LEFT ANTI JOIN {S}.slv_claims_header_business c ON p.claim_id = c.claim_id""", level="warn")
dq.check("claim lines whose claim_id is not in claims",
         f"""SELECT l.claim_line_id, l.claim_id FROM {S}.slv_claims_line_business l
             LEFT ANTI JOIN {S}.slv_claims_header_business c ON l.claim_id = c.claim_id""", level="warn")

# COMMAND ----------

# Informational profiling (never fails the run)
print("-- Claim reporting delay --")
spark.table(f"{S}.slv_claims_header_business").select("claim_reporting_delay_days").summary().show()
print("-- Payment delay --")
spark.table(f"{S}.slv_payments_business").select("payment_delay_days").summary().show()
print("-- Fraud severity --")
spark.table(f"{S}.slv_fraud_scores_business").groupBy("fraud_severity").count().show()
print("-- Claims matched to a weather event --")
spark.table(f"{S}.slv_claims_header_business").groupBy("weather_claim_match", "is_severe_weather").count().show()

# COMMAND ----------

dq.raise_if_failed()
