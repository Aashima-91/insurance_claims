# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Silver business — fraud scores
# MAGIC **Reads:** `slv_fraud_scores_technical`, `slv_claims_header_technical` → **Writes:** `silver.slv_fraud_scores_business`
# MAGIC
# MAGIC Thresholds now come from config (`FRAUD_HIGH_THRESHOLD`, `FRAUD_MEDIUM_THRESHOLD`, `FRAUD_SCORE_MAX`).

# COMMAND ----------

# MAGIC %run ../../../configs/common/common_config

# COMMAND ----------

fraud = drop_metadata(spark.table(f"{SILVER_DB}.slv_fraud_scores_technical"))
claims = spark.table(f"{SILVER_DB}.slv_claims_header_technical").select(
    "claim_id", "policy_id", "loss_postcode", "incident_date"
)

fraud_biz = (
    fraud.join(claims, "claim_id", "left")
    .withColumn("fraud_flag", F.col("fraud_score") >= FRAUD_HIGH_THRESHOLD)
    .withColumn("fraud_severity",
                F.when(F.col("fraud_score") >= FRAUD_HIGH_THRESHOLD, "High")
                 .when(F.col("fraud_score") >= FRAUD_MEDIUM_THRESHOLD, "Medium")
                 .otherwise("Low"))
    .withColumn("flags_str", F.concat_ws(", ", "flags"))
)

write_table(fraud_biz, SILVER_DB, "slv_fraud_scores_business")
