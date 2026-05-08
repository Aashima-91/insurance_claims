# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %run "../../../configs/common/common_config"

# COMMAND ----------

bronze_prod = spark.table(f"{BRONZE_DB}.brz_product_mapping_raw")

silver_prod_tech = (
    bronze_prod
    .withColumn("product_code", F.trim("product_code"))
    .withColumn("product_name", F.trim("product_name"))
    .withColumn("lob", F.trim("lob"))
    .withColumn("coverage_type", F.trim("coverage_type"))
    .withColumn("risk_category", F.trim("risk_category"))
    .withColumn("created_ts", F.to_timestamp("created_ts"))
    .withColumn("updated_ts", F.to_timestamp("updated_ts"))
    .withColumn("ingestion_ts", F.to_timestamp("bronze_ingest_ts"))
    .filter(F.col("product_code").isNotNull())
)

(
    silver_prod_tech.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{SILVER_DB}.slv_product_mapping_technical")
)

