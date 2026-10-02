# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Bronze — Claims header (cloud claims system, JSON array files)
# MAGIC
# MAGIC **Reads:** `{LANDING_PATH}/cloud_claims/` files matching `claims_header*` (new files only — Auto Loader)
# MAGIC **Writes:** `bronze.brz_claim_header_raw` (append-only, all columns STRING + lineage columns)
# MAGIC
# MAGIC Replaces the old `dbutils.fs.head()` + manual parsing, which silently cut files at 10 MB and dropped `source_file_name` (it was not in the schema).

# COMMAND ----------

# MAGIC %run ../../configs/common/common_config

# COMMAND ----------

ingest_to_bronze(
    source_folder="cloud_claims",
    file_glob="claims_header*",
    file_format="json",
    target_table="brz_claim_header_raw",
    reader_options={'multiLine': 'true'},
)

# COMMAND ----------

display(spark.table(f"{BRONZE_DB}.brz_claim_header_raw").orderBy(F.col("bronze_ingest_ts").desc()).limit(20))
