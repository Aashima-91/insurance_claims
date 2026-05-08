# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %run "../../../configs/common/common_config"

# COMMAND ----------

# DBTITLE 1,Cell 1
bronze_line = spark.table(f"{BRONZE_DB}.brz_claim_line_raw")

silver_line_tech = (
    bronze_line
    .withColumn("claim_line_id", F.trim("claim_line_id"))
    .withColumn("claim_id", F.trim("claim_id"))
    .withColumn("coverage_code", F.trim("coverage_code"))
    .withColumn("coverage_desc", F.trim("coverage_desc"))
    .withColumn("item_type", F.trim("item_type"))
    .withColumn("line_status", F.trim("line_status"))
    .withColumn("source_file", F.trim("source_file"))
    .withColumn("source_system", F.trim("source_system"))
    .withColumn("claimed_amount", F.col("claimed_amount").cast("double"))
    .withColumn("approved_amount", F.col("approved_amount").cast("double"))
    .withColumn("deductible_amount", F.col("deductible_amount").cast("double"))
    .withColumn("created_ts", F.to_timestamp("created_ts"))
    .withColumn("updated_ts", F.to_timestamp("updated_ts"))
    .withColumn("ingestion_ts", F.to_timestamp("ingestion_ts"))
    .filter(F.col("claim_line_id").isNotNull())
)

(
    silver_line_tech.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{SILVER_DB}.slv_claims_line_technical")
)
