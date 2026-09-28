# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Silver technical — fraud scores
# MAGIC **Reads:** `bronze.brz_fraud_scores_raw` → **Writes:** `silver.slv_fraud_scores_technical` (1 row per `claim_id`)
# MAGIC
# MAGIC Fix: dedup — a claim re-scored in a later file used to create 2 rows and double the claim in `fact_claims`.

# COMMAND ----------

# MAGIC %run ../../../configs/common/common_config

# COMMAND ----------

df = spark.table(f"{BRONZE_DB}.brz_fraud_scores_raw")

fraud_tech = (
    trim_all_strings(df)
    .withColumn("fraud_score", to_double("fraud_score"))
    .withColumn("created_ts", to_ts_safe("created_ts"))
    .withColumn("updated_ts", to_ts_safe("updated_ts"))
    .filter(F.col("claim_id").isNotNull())
    .select("claim_id", "fraud_score", "risk_category", "flags",
            "created_ts", "updated_ts", "source_file", "source_system",
            "bronze_source_file", "bronze_ingest_ts", "bronze_ingest_batch_id")
)

fraud_tech = dedupe_latest(fraud_tech, ["claim_id"])

write_table(fraud_tech, SILVER_DB, "slv_fraud_scores_technical")
