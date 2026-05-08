# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DB names
GOLD_DB = "gold"
SILVER_DB = "silver"

print(f"Using GOLD_DB = {GOLD_DB}, SILVER_DB = {SILVER_DB}")


# COMMAND ----------

from pyspark.sql import DataFrame

def run_check(name: str, query: str):
    print(f"\n=== CHECK: {name} ===")
    print(f"SQL:\n{query}\n")
    df: DataFrame = spark.sql(query)
    count = df.count()
    if count == 0:
        print(f"RESULT: ✅ PASSED (0 rows returned)\n")
    else:
        print(f"RESULT: ❌ FAILED ({count} rows returned)\n")
        df.show(min(count, 50), truncate=False)


# COMMAND ----------

# MAGIC %md
# MAGIC ## **DIM Checks**

# COMMAND ----------

run_check(
    "dim_date – duplicate date_key",
    f"""
    SELECT date_key, COUNT(*) AS cnt
    FROM {GOLD_DB}.dim_date
    GROUP BY date_key
    HAVING COUNT(*) > 1
    """
)

run_check(
    "dim_date – null date_key or date",
    f"""
    SELECT *
    FROM {GOLD_DB}.dim_date
    WHERE date_key IS NULL OR date IS NULL
    """
)


# COMMAND ----------

run_check(
    "dim_policy – duplicate policy_id",
    f"""
    SELECT policy_id, COUNT(*) AS cnt
    FROM {GOLD_DB}.dim_policy
    GROUP BY policy_id
    HAVING COUNT(*) > 1
    """
)

run_check(
    "dim_policy – null policy_id or policy_number",
    f"""
    SELECT *
    FROM {GOLD_DB}.dim_policy
    WHERE policy_id IS NULL OR policy_number IS NULL
    """
)

run_check(
    "dim_policy – invalid dates (start > end)",
    f"""
    SELECT *
    FROM {GOLD_DB}.dim_policy
    WHERE policy_start_date IS NOT NULL
      AND policy_end_date   IS NOT NULL
      AND policy_start_date > policy_end_date
    """
)

run_check(
    "dim_policy – negative premium_amount",
    f"""
    SELECT *
    FROM {GOLD_DB}.dim_policy
    WHERE premium_amount < 0
    """
)


# COMMAND ----------

run_check(
    "dim_claim – duplicate claim_id",
    f"""
    SELECT claim_id, COUNT(*) AS cnt
    FROM {GOLD_DB}.dim_claim
    GROUP BY claim_id
    HAVING COUNT(*) > 1
    """
)

run_check(
    "dim_claim – null claim_id or claim_number",
    f"""
    SELECT *
    FROM {GOLD_DB}.dim_claim
    WHERE claim_id IS NULL OR claim_number IS NULL
    """
)

run_check(
    "dim_claim – incident_date after reported_date",
    f"""
    SELECT *
    FROM {GOLD_DB}.dim_claim
    WHERE incident_date IS NOT NULL
      AND reported_date IS NOT NULL
      AND incident_date > reported_date
    """
)

run_check(
    "dim_claim – negative estimated_loss_amount",
    f"""
    SELECT *
    FROM {GOLD_DB}.dim_claim
    WHERE estimated_loss_amount < 0
    """
)


# COMMAND ----------

# 1) Duplicate event_id
run_check(
    "dim_weather – duplicate event_id",
    f"""
    SELECT event_id, COUNT(*) AS cnt
    FROM {GOLD_DB}.dim_weather
    GROUP BY event_id
    HAVING COUNT(*) > 1
    """
)

# 2) Null key/date
run_check(
    "dim_weather – null event_id or event_date",
    f"""
    SELECT *
    FROM {GOLD_DB}.dim_weather
    WHERE event_id IS NULL OR event_date IS NULL
    """
)

# 3) Invalid severity category (assuming your expected set)
run_check(
    "dim_weather – invalid severity value",
    f"""
    SELECT DISTINCT severity
    FROM {GOLD_DB}.dim_weather
    WHERE severity IS NOT NULL
      AND severity NOT IN ('Severe', 'Moderate', 'Minor', 'Extreme')
    """
)

# 4) Invalid severity_norm (if it’s just a normalized label, same domain)
run_check(
    "dim_weather – invalid severity_norm value",
    f"""
    SELECT DISTINCT severity_norm
    FROM {GOLD_DB}.dim_weather
    WHERE severity_norm IS NOT NULL
      AND severity_norm NOT IN ('Severe', 'Moderate', 'Minor', 'Extreme')
    """
)

# 5) is_severe_weather should be Y/N or true/false depending on your design
run_check(
    "dim_weather – invalid is_severe_weather flag",
    f"""
    SELECT DISTINCT is_severe_weather
    FROM {GOLD_DB}.dim_weather
    WHERE is_severe_weather IS NOT NULL
      AND is_severe_weather NOT IN ('Y','N')
    """
)


# COMMAND ----------

run_check(
    "dim_fraud – duplicate claim_id",
    f"""
    SELECT claim_id, COUNT(*) AS cnt
    FROM {GOLD_DB}.dim_fraud
    GROUP BY claim_id
    HAVING COUNT(*) > 1
    """
)

run_check(
    "dim_fraud – fraud_score outside [0,1]",
    f"""
    SELECT *
    FROM {GOLD_DB}.dim_fraud
    WHERE fraud_score IS NOT NULL
      AND (fraud_score < 0 OR fraud_score > 1)
    """
)

run_check(
    "dim_fraud – invalid fraud_flag",
    f"""
    SELECT *
    FROM {GOLD_DB}.dim_fraud
    WHERE fraud_flag IS NOT NULL
      AND fraud_flag NOT IN ('Y','N')
    """
)


# COMMAND ----------

# MAGIC %md
# MAGIC ## **FACT Checks**

# COMMAND ----------

run_check(
    "fact_claims – duplicate claim_id (grain check)",
    f"""
    SELECT claim_id, COUNT(*) AS cnt
    FROM {GOLD_DB}.fact_claims
    GROUP BY claim_id
    HAVING COUNT(*) > 1
    """
)

run_check(
    "fact_claims – FK claim_id not in dim_claim",
    f"""
    SELECT f.*
    FROM {GOLD_DB}.fact_claims f
    LEFT JOIN {GOLD_DB}.dim_claim d
      ON f.claim_id = d.claim_id
    WHERE f.claim_id IS NOT NULL
      AND d.claim_id IS NULL
    """
)

run_check(
    "fact_claims – FK policy_id not in dim_policy",
    f"""
    SELECT f.*
    FROM {GOLD_DB}.fact_claims f
    LEFT JOIN {GOLD_DB}.dim_policy p
      ON f.policy_id = p.policy_id
    WHERE f.policy_id IS NOT NULL
      AND p.policy_id IS NULL
    """
)

run_check(
    "fact_claims – negative loss amounts",
    f"""
    SELECT *
    FROM {GOLD_DB}.fact_claims
    WHERE estimated_loss_amount < 0
    """
)


# COMMAND ----------

run_check(
    "fact_payments – duplicate payment_id (grain check)",
    f"""
    SELECT payment_id, COUNT(*) AS cnt
    FROM {GOLD_DB}.fact_payments
    GROUP BY payment_id
    HAVING COUNT(*) > 1
    """
)

run_check(
    "fact_payments – FK claim_id not in dim_claim",
    f"""
    SELECT f.*
    FROM {GOLD_DB}.fact_payments f
    LEFT JOIN {GOLD_DB}.dim_claim d
      ON f.claim_id = d.claim_id
    WHERE f.claim_id IS NOT NULL
      AND d.claim_id IS NULL
    """
)

run_check(
    "fact_payments – negative payment_amount",
    f"""
    SELECT *
    FROM {GOLD_DB}.fact_payments
    WHERE payment_amount < 0
    """
)


# COMMAND ----------

run_check(
    "fact_weather_events – duplicate event_id (grain check)",
    f"""
    SELECT event_id, COUNT(*) AS cnt
    FROM {GOLD_DB}.fact_weather_events
    GROUP BY event_id
    HAVING COUNT(*) > 1
    """
)

run_check(
    "fact_weather_events – FK event_id not in dim_weather",
    f"""
    SELECT f.*
    FROM {GOLD_DB}.fact_weather_events f
    LEFT JOIN {GOLD_DB}.dim_weather d
      ON f.event_id = d.event_id
    WHERE f.event_id IS NOT NULL
      AND d.event_id IS NULL
    """
)

run_check(
    "fact_weather_events – FK claim_id not in dim_claim",
    f"""
    SELECT f.*
    FROM {GOLD_DB}.fact_weather_events f
    LEFT JOIN {GOLD_DB}.dim_claim d
      ON f.claim_id = d.claim_id
    WHERE f.claim_id IS NOT NULL
      AND d.claim_id IS NULL
    """
)


# COMMAND ----------

run_check(
    "fact_fraud_scores – duplicate claim_id (grain check)",
    f"""
    SELECT claim_id, COUNT(*) AS cnt
    FROM {GOLD_DB}.fact_fraud_scores
    GROUP BY claim_id
    HAVING COUNT(*) > 1
    """
)

run_check(
    "fact_fraud_scores – FK claim_id not in dim_claim",
    f"""
    SELECT f.*
    FROM {GOLD_DB}.fact_fraud_scores f
    LEFT JOIN {GOLD_DB}.dim_claim d
      ON f.claim_id = d.claim_id
    WHERE f.claim_id IS NOT NULL
      AND d.claim_id IS NULL
    """
)

run_check(
    "fact_fraud_scores – fraud_score outside [0,1]",
    f"""
    SELECT *
    FROM {GOLD_DB}.fact_fraud_scores
    WHERE fraud_score IS NOT NULL
      AND (fraud_score < 0 OR fraud_score > 1)
    """
)

