# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Bronze — Weather events enrichment (API extract, JSON)
# MAGIC
# MAGIC **Reads:** `{LANDING_PATH}/api_enrichment/` files matching `weather*` (new files only — Auto Loader)
# MAGIC **Writes:** `bronze.brz_weather_raw` (append-only, all columns STRING + lineage columns)
# MAGIC
# MAGIC Replaces the old `dbutils.fs.head()` + manual parsing, which silently cut files at 10 MB.

# COMMAND ----------

# MAGIC %run ../../configs/common/common_config

# COMMAND ----------

ingest_to_bronze(
    source_folder="api_enrichment",
    file_glob="weather*",
    file_format="json",
    target_table="brz_weather_raw",
    reader_options={'multiLine': 'true'},
)

# COMMAND ----------

display(spark.table(f"{BRONZE_DB}.brz_weather_raw").orderBy(F.col("bronze_ingest_ts").desc()).limit(20))
