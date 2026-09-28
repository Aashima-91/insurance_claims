# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Bronze — Policy master (on-prem system, CSV)
# MAGIC
# MAGIC **Reads:** `{LANDING_PATH}/onprem_policy/` files matching `policy_master_*` (new files only — Auto Loader)
# MAGIC **Writes:** `bronze.brz_policy_master_raw` (append-only, all columns STRING + lineage columns)
# MAGIC
# MAGIC Replaces the old `dbutils.fs.head()` + manual parsing, which silently cut files at 10 MB and broke on quoted commas.

# COMMAND ----------

# MAGIC %run ../../configs/common/common_config

# COMMAND ----------

ingest_to_bronze(
    source_folder="onprem_policy",
    file_glob="policy_master_*",
    file_format="csv",
    target_table="brz_policy_master_raw",
    reader_options={'header': 'true'},
)

# COMMAND ----------

display(spark.table(f"{BRONZE_DB}.brz_policy_master_raw").orderBy(F.col("bronze_ingest_ts").desc()).limit(20))
