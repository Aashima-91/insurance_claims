# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Silver technical — postcode → region (reference)
# MAGIC **Reads:** `bronze.brz_postcode_region_raw` → **Writes:** `silver.slv_postcode_region_technical`
# MAGIC (1 row per `postcode` + `state`)
# MAGIC
# MAGIC Fix: postcode is a 4-char string (was INT → NT postcodes like 0800 lost the leading zero).

# COMMAND ----------

# MAGIC %run ../../../configs/common/common_config

# COMMAND ----------

bronze_pc = spark.table(f"{BRONZE_DB}.brz_postcode_region_raw")

silver_pc_tech = (
    trim_all_strings(bronze_pc)
    .withColumn("postcode", to_postcode("postcode"))
    .withColumn("ingestion_ts", F.col("bronze_ingest_ts"))
    .filter(F.col("postcode").isNotNull())
)

silver_pc_tech = dedupe_latest(silver_pc_tech, ["postcode", "state"], ["bronze_ingest_ts"])

write_table(silver_pc_tech, SILVER_DB, "slv_postcode_region_technical")
