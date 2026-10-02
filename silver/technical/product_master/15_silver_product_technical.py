# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Silver technical — product mapping (reference)
# MAGIC **Reads:** `bronze.brz_product_mapping_raw` → **Writes:** `silver.slv_product_mapping_technical` (1 row per `product_code`)

# COMMAND ----------

# MAGIC %run ../../../configs/common/common_config

# COMMAND ----------

bronze_prod = spark.table(f"{BRONZE_DB}.brz_product_mapping_raw")

silver_prod_tech = (
    trim_all_strings(bronze_prod)
    .withColumn("ingestion_ts", F.col("bronze_ingest_ts"))
    .filter(F.col("product_code").isNotNull())
)

# Reference file is re-sent in full: keep the row from the newest file
silver_prod_tech = dedupe_latest(silver_prod_tech, ["product_code"], ["bronze_ingest_ts"])

write_table(silver_prod_tech, SILVER_DB, "slv_product_mapping_technical")
