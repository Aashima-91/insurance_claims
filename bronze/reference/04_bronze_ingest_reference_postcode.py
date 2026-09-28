# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Bronze — Postcode → region reference (CSV)
# MAGIC
# MAGIC **Reads:** `{LANDING_PATH}/reference/` files matching `postcode_region_mapping*` (new files only — Auto Loader)
# MAGIC **Writes:** `bronze.brz_postcode_region_raw` (append-only, all columns STRING + lineage columns)
# MAGIC
# MAGIC Replaces the old `dbutils.fs.head()` + manual parsing, which silently cut files at 10 MB and broke on quoted commas.

# COMMAND ----------

# MAGIC %run ../../configs/common/common_config

# COMMAND ----------

ingest_to_bronze(
    source_folder="reference",
    file_glob="postcode_region_mapping*",
    file_format="csv",
    target_table="brz_postcode_region_raw",
    reader_options={'header': 'true'},
)

# COMMAND ----------

display(spark.table(f"{BRONZE_DB}.brz_postcode_region_raw").orderBy(F.col("bronze_ingest_ts").desc()).limit(20))
