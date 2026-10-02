# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Bronze — Fraud scores enrichment (API extract, JSON)
# MAGIC
# MAGIC **Reads:** `{LANDING_PATH}/api_enrichment/` files matching `fraud_scores*` (new files only — Auto Loader)
# MAGIC **Writes:** `bronze.brz_fraud_scores_raw` (append-only, all columns STRING + lineage columns)
# MAGIC
# MAGIC Replaces the old `dbutils.fs.head()` + manual parsing, which silently cut files at 10 MB.

# COMMAND ----------

# MAGIC %run ../../configs/common/common_config

# COMMAND ----------

ingest_to_bronze(
    source_folder="api_enrichment",
    file_glob="fraud_scores*",
    file_format="json",
    target_table="brz_fraud_scores_raw",
    reader_options={'multiLine': 'true'},
    schema_hints="flags ARRAY<STRING>",   # keep flags as an array (everything else stays STRING)
)

# COMMAND ----------

display(spark.table(f"{BRONZE_DB}.brz_fraud_scores_raw").orderBy(F.col("bronze_ingest_ts").desc()).limit(20))
