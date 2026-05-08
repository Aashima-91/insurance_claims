# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %run "../../../configs/common/common_config"

# COMMAND ----------

bronze_pc = spark.table(f"{BRONZE_DB}.brz_postcode_region_raw")

silver_pc_tech = (
    bronze_pc
    .withColumn("postcode", F.col("postcode").cast("int"))
    .withColumn("suburb", F.trim("suburb"))
    .withColumn("state", F.trim("state"))
    .withColumn("region", F.trim("region"))
    .withColumn("risk_zone", F.trim("risk_zone"))
    .withColumn("created_ts", F.to_timestamp("created_ts"))
    .withColumn("updated_ts", F.to_timestamp("updated_ts"))
    .withColumn("ingestion_ts", F.to_timestamp("bronze_ingest_ts"))
    .filter(F.col("postcode").isNotNull())
)

(
    silver_pc_tech.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{SILVER_DB}.slv_postcode_region_technical")
)

