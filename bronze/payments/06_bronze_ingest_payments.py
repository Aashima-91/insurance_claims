# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
payments_dir = "/Workspace/Users/aashima91.gupta@gmail.com/insurance_claims/raw/payments/"

files = dbutils.fs.ls(payments_dir)

# Match your actual filenames
payment_files = [f.path for f in files if f.name.startswith("payments_")]

payment_files


# COMMAND ----------

import csv
from io import StringIO

all_records = []

for file_path in payment_files:
    # Read full file
    content = dbutils.fs.head(file_path, 10000000)

    # Remove BOM
    content = content.encode("utf-8").decode("utf-8-sig")

    # Normalize line endings
    content = content.replace("\r\n", "\n").replace("\r", "\n")

    # Split into lines
    lines = [l for l in content.split("\n") if l.strip() != ""]

    # Extract header + rows
    header = lines[0].split(",")
    rows = [l.split(",") for l in lines[1:]]

    # Fix rows with missing columns
    fixed_rows = []
    for r in rows:
        if len(r) < len(header):
            r = r + [""] * (len(header) - len(r))
        fixed_rows.append(r)

    # Convert to dicts
    for r in fixed_rows:
        rec = dict(zip(header, r))
        rec["source_file_name"] = file_path
        all_records.append(rec)


# COMMAND ----------

df_payments_raw = spark.createDataFrame(all_records)


# COMMAND ----------

from pyspark.sql.functions import current_timestamp, monotonically_increasing_id, lit

df_payments_bronze = (
    df_payments_raw
    .withColumn("bronze_ingest_ts", current_timestamp())
    .withColumn("bronze_source_file", lit("payments_batch"))
    .withColumn("bronze_ingest_batch_id", lit("batch_001"))
)


# COMMAND ----------

spark.sql("CREATE DATABASE IF NOT EXISTS bronze")

df_payments_bronze.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("bronze.brz_payments_raw")


# COMMAND ----------

spark.sql("SELECT * FROM bronze.brz_payments_raw LIMIT 20")

