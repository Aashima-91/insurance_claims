# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Gold data-quality checks
# MAGIC
# MAGIC Fixes vs old version:
# MAGIC - `fraud_score` range is `0..FRAUD_SCORE_MAX` (100) — the old check used `0..1`, which contradicts the silver
# MAGIC   thresholds (60 / 30) and failed for every scored claim.
# MAGIC - Boolean flags are checked as booleans (the old `NOT IN ('Y','N')` only passed because Spark casts 'Y' to TRUE).
# MAGIC - FK checks use the new surrogate keys, including every date key → `dim_date`.
# MAGIC - Failures of `error` checks **raise**, so the job run is marked failed.

# COMMAND ----------

# MAGIC %run ../../configs/common/common_config

# COMMAND ----------

G = GOLD_DB
dq = DQ("gold")

# ---------------- Dimensions: unique keys ----------------
for table, key in [("dim_date", "date_key"), ("dim_claim", "claim_sk"), ("dim_claim", "claim_id"),
                   ("dim_policy", "policy_sk"), ("dim_policy", "policy_id"),
                   ("dim_weather", "weather_sk"), ("dim_weather", "event_id")]:
    dq.check(f"{table}: duplicate {key}",
             f"SELECT {key}, COUNT(*) cnt FROM {G}.{table} GROUP BY {key} HAVING COUNT(*) > 1")

dq.check("dim_date: null date on a real (non -1) row",
         f"SELECT * FROM {G}.dim_date WHERE date_key <> -1 AND date IS NULL")
dq.check("dim_date: gaps in calendar",
         f"""SELECT MIN(date) mn, MAX(date) mx, COUNT(*) n FROM {G}.dim_date WHERE date_key <> -1
             HAVING datediff(MAX(date), MIN(date)) + 1 <> COUNT(*)""")
dq.check("dim_policy: start date after end date",
         f"SELECT policy_id FROM {G}.dim_policy WHERE policy_start_date > policy_end_date", level="warn")
dq.check("dim_policy: negative premium_amount",
         f"SELECT policy_id, premium_amount FROM {G}.dim_policy WHERE premium_amount < 0", level="warn")

# ---------------- Facts: grain ----------------
for table, key in [("fact_claims", "claim_sk"), ("fact_payments", "payment_id"),
                   ("fact_weather_events", "weather_sk"), ("fact_fraud_scores", "claim_sk")]:
    dq.check(f"{table}: grain violated (duplicate {key})",
             f"SELECT {key}, COUNT(*) cnt FROM {G}.{table} GROUP BY {key} HAVING COUNT(*) > 1")

# ---------------- Facts: foreign keys ----------------
fks = [
    ("fact_claims",         "claim_sk",          "dim_claim",   "claim_sk",   "error"),
    ("fact_claims",         "policy_sk",         "dim_policy",  "policy_sk",  "warn"),
    ("fact_claims",         "incident_date_key", "dim_date",    "date_key",   "error"),
    ("fact_claims",         "reported_date_key", "dim_date",    "date_key",   "error"),
    ("fact_payments",       "claim_sk",          "dim_claim",   "claim_sk",   "warn"),
    ("fact_payments",       "payment_date_key",  "dim_date",    "date_key",   "error"),
    ("fact_weather_events", "weather_sk",        "dim_weather", "weather_sk", "error"),
    ("fact_weather_events", "event_date_key",    "dim_date",    "date_key",   "error"),
    ("fact_fraud_scores",   "claim_sk",          "dim_claim",   "claim_sk",   "warn"),
]
for fact, fk, dim, pk, level in fks:
    dq.check(f"{fact}.{fk} not found in {dim}",
             f"""SELECT f.{fk}, COUNT(*) cnt FROM {G}.{fact} f
                 LEFT ANTI JOIN {G}.{dim} d ON f.{fk} = d.{pk}
                 WHERE f.{fk} IS NOT NULL GROUP BY f.{fk}""", level=level)

# ---------------- Facts: business rules ----------------
dq.check("fact_claims: negative estimated_loss_amount",
         f"SELECT claim_id, estimated_loss_amount FROM {G}.fact_claims WHERE estimated_loss_amount < 0", level="warn")
dq.check("fact_claims: approved more than claimed",
         f"SELECT claim_id, total_claimed_amount, total_approved_amount FROM {G}.fact_claims WHERE total_approved_amount > total_claimed_amount", level="warn")
dq.check("fact_payments: negative payment_amount",
         f"SELECT payment_id, payment_amount FROM {G}.fact_payments WHERE payment_amount < 0", level="warn")
dq.check(f"fact_fraud_scores: fraud_score outside 0..{FRAUD_SCORE_MAX}",
         f"SELECT claim_id, fraud_score FROM {G}.fact_fraud_scores WHERE fraud_score < 0 OR fraud_score > {FRAUD_SCORE_MAX}")
dq.check("fact_fraud_scores: fraud_flag inconsistent with score",
         f"""SELECT claim_id, fraud_score, fraud_flag FROM {G}.fact_fraud_scores
             WHERE fraud_flag <> (fraud_score >= {FRAUD_HIGH_THRESHOLD})""")
dq.check("dim_weather: is_severe_weather inconsistent with severity",
         f"""SELECT event_id, severity, is_severe_weather FROM {G}.dim_weather
             WHERE is_severe_weather <> (severity IN ({", ".join(f"'{v}'" for v in SEVERE_WEATHER_LEVELS)}))""")

# ---------------- Reconciliation: gold must match silver ----------------
dq.check("fact_claims row count = silver claims header",
         f"""SELECT * FROM (SELECT COUNT(*) n FROM {G}.fact_claims) g, (SELECT COUNT(*) n FROM {SILVER_DB}.slv_claims_header_business) s
             WHERE g.n <> s.n""")
dq.check("fact_payments total = silver payments total",
         f"""SELECT * FROM (SELECT SUM(payment_amount) a FROM {G}.fact_payments) g, (SELECT SUM(payment_amount) a FROM {SILVER_DB}.slv_payments_business) s
             WHERE g.a <> s.a""")

# COMMAND ----------

dq.raise_if_failed()
