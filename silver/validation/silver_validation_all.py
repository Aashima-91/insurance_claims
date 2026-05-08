# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# Databricks notebook: 01_silver_validation_all
# Silver Layer Validation – Clean Schemas, No Metadata

from pyspark.sql import functions as F

SILVER = "silver"

# ---------------------------------------------------------
# 1. Row Count Comparison (Technical vs Business)
# ---------------------------------------------------------

def count_check(tech, biz):
    tech_count = spark.table(tech).count()
    biz_count = spark.table(biz).count()
    print(f"{tech}: {tech_count:,}")
    print(f"{biz}: {biz_count:,}")
    print(f"Δ rows: {biz_count - tech_count:,}\n")

print("===== ROW COUNT CHECKS =====\n")

count_check(f"{SILVER}.slv_claims_header_technical", f"{SILVER}.slv_claims_header_business_v2")
count_check(f"{SILVER}.slv_claims_line_technical", f"{SILVER}.slv_claims_line_business_v2")
count_check(f"{SILVER}.slv_payments_technical", f"{SILVER}.slv_payments_business_v2")
count_check(f"{SILVER}.slv_policy_technical", f"{SILVER}.slv_policy_business_v2")
count_check(f"{SILVER}.slv_weather_technical", f"{SILVER}.slv_weather_business_v2")
count_check(f"{SILVER}.slv_fraud_scores_technical", f"{SILVER}.slv_fraud_scores_business_v2")


# ---------------------------------------------------------
# 2. Null Profiling for Key Columns
# ---------------------------------------------------------

def null_profile(table, cols):
    df = spark.table(table)
    print(f"\n===== NULL PROFILE: {table} =====")
    for c in cols:
        print(f"{c}: {df.filter(F.col(c).isNull()).count():,} nulls")

null_profile(f"{SILVER}.slv_claims_header_business_v2",
             ["claim_id", "policy_id", "incident_date", "loss_postcode", "policy_active"])

null_profile(f"{SILVER}.slv_payments_business_v2",
             ["claim_id", "payment_date", "payment_delay_days"])

null_profile(f"{SILVER}.slv_weather_business_v2",
             ["postcode", "state", "event_date", "weather_claim_match", "is_severe_weather"])

null_profile(f"{SILVER}.slv_fraud_scores_business_v2",
             ["claim_id", "fraud_score", "fraud_severity"])

null_profile(f"{SILVER}.slv_policy_business_v2",
             ["policy_id", "product_code", "policy_start_date", "policy_end_date", "is_active"])

# ---------------------------------------------------------
# 3. Schema Check – Expect Clean Schemas (No Metadata)
# ---------------------------------------------------------

def schema_check(table):
    print(f"\n===== SCHEMA CHECK: {table} =====")
    spark.table(table).printSchema()

schema_check(f"{SILVER}.slv_claims_header_business_v2")
schema_check(f"{SILVER}.slv_payments_business_v2")
schema_check(f"{SILVER}.slv_weather_business_v2")
schema_check(f"{SILVER}.slv_fraud_scores_business_v2")
schema_check(f"{SILVER}.slv_policy_business_v2")


# ---------------------------------------------------------
# 4. Business Rule Sanity Checks
# ---------------------------------------------------------

print("\n===== BUSINESS RULE CHECKS =====")

print("\n-- Claim Reporting Delay Stats --")
spark.table(f"{SILVER}.slv_claims_header_business_v2") \
    .select("claim_reporting_delay_days") \
    .summary().show()

print("\n-- Payment Delay Stats --")
spark.table(f"{SILVER}.slv_payments_business_v2") \
    .select("payment_delay_days") \
    .summary().show()

print("\n-- Fraud Severity Distribution --")
spark.table(f"{SILVER}.slv_fraud_scores_business_v2") \
    .groupBy("fraud_severity").count().show()

print("\n-- Weather Severity Distribution --")
spark.table(f"{SILVER}.slv_weather_business_v2") \
    .groupBy("is_severe_weather").count().show()


# ---------------------------------------------------------
# 5. Join Quality Checks
# ---------------------------------------------------------

print("\n===== JOIN QUALITY CHECKS =====")

print("\n-- Claims without Policy --")
print(
    spark.table(f"{SILVER}.slv_claims_header_business_v2")
    .filter(F.col("policy_id").isNull())
    .count()
)

print("\n-- Weather-Claim Matches --")
print(
    spark.table(f"{SILVER}.slv_weather_business_v2")
    .filter(F.col("weather_claim_match") == True)
    .count()
)


# ---------------------------------------------------------
# 6. Duplicate Key Checks
# ---------------------------------------------------------

print("\n===== DUPLICATE KEY CHECKS =====")

print("\n-- Duplicate Claim IDs in Claims Header --")
print(
    spark.table(f"{SILVER}.slv_claims_header_business_v2")
    .groupBy("claim_id").count().filter("count > 1").count()
)

print("\n-- Duplicate Payments per Claim per Date --")
print(
    spark.table(f"{SILVER}.slv_payments_business_v2")
    .groupBy("claim_id", "payment_date").count().filter("count > 1").count()
)

print("\n===== VALIDATION COMPLETE =====")

