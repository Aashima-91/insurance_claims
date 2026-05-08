# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %run "../../../configs/common/common_config"

# COMMAND ----------

from pyspark.sql import functions as F

fraud = spark.table(f"{SILVER_DB}.slv_fraud_scores_technical")

# Drop or rename fraud_flag from claims to avoid duplicates
claims = (
    spark.table(f"{SILVER_DB}.slv_claims_header_business_v2")
    .drop("fraud_flag")   # <— THIS FIXES THE ERROR
)

fraud_biz = (
    fraud.alias("f")
    .join(claims.alias("c"), "claim_id", "left")
    .select(
        "f.claim_id",
        "f.fraud_score",
        "f.risk_category",
        "f.flags",
        "f.source_file",
        "f.source_system",

        # Derived fields
        (F.col("fraud_score") >= 60).alias("fraud_flag"),
        F.when(F.col("fraud_score") >= 60, "High")
         .when(F.col("fraud_score") >= 30, "Medium")
         .otherwise("Low")
         .alias("fraud_severity"),
        F.concat_ws(", ", "flags").alias("flags_str"),

        # Claim fields
        "c.policy_id",
        "c.loss_postcode",
        "c.incident_date"
    )
)

(
    fraud_biz.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{SILVER_DB}.slv_fraud_scores_business_v2")
)

