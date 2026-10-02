# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Bronze — Payments (CSV)
# MAGIC
# MAGIC **Reads:** `{LANDING_PATH}/payments/` files matching `payments_*` (new files only — Auto Loader)
# MAGIC **Writes:** `bronze.brz_payments_raw` (append-only, all columns STRING + lineage columns)
# MAGIC
# MAGIC Replaces the old `dbutils.fs.head()` + manual parsing, which silently cut files at 10 MB and broke on quoted commas.

# COMMAND ----------

# MAGIC %run ../../configs/common/common_config

# COMMAND ----------

ingest_to_bronze(
    source_folder="payments",
    file_glob="payments_*",
    file_format="csv",
    target_table="brz_payments_raw",
    reader_options={'header': 'true'},
)

# COMMAND ----------

display(spark.table(f"{BRONZE_DB}.brz_payments_raw").orderBy(F.col("bronze_ingest_ts").desc()).limit(20))
