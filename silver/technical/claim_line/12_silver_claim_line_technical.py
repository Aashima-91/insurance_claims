# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Silver technical — claims line
# MAGIC **Reads:** `bronze.brz_claim_line_raw` → **Writes:** `silver.slv_claims_line_technical` (1 row per `claim_line_id`)

# COMMAND ----------

# MAGIC %run ../../../configs/common/common_config

# COMMAND ----------

bronze_line = spark.table(f"{BRONZE_DB}.brz_claim_line_raw")

silver_line_tech = (
    trim_all_strings(bronze_line)
    .withColumn("claimed_amount", to_decimal("claimed_amount"))
    .withColumn("approved_amount", to_decimal("approved_amount"))
    .withColumn("deductible_amount", to_decimal("deductible_amount"))
    .withColumn("created_ts", to_ts_safe("created_ts"))
    .withColumn("updated_ts", to_ts_safe("updated_ts"))
    .withColumn("ingestion_ts", to_ts_safe("ingestion_ts"))
    .filter(F.col("claim_line_id").isNotNull())
)

silver_line_tech = dedupe_latest(silver_line_tech, ["claim_line_id"])

write_table(silver_line_tech, SILVER_DB, "slv_claims_line_technical")
