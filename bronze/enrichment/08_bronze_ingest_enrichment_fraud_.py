# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
raw_fraud_path = "/Workspace/Users/aashima91.gupta@gmail.com/insurance_claims/raw/api_enrichment/"


# COMMAND ----------

files = dbutils.fs.ls(raw_fraud_path)
json_files = [f.path for f in files if f.name.startswith("fraud_scores")]
json_files


# COMMAND ----------

import json

all_records = []

for file_path in json_files:
    content = dbutils.fs.head(file_path, 10000000)  # up to ~10MB
    records = json.loads(content)  # JSON array → Python list
    for record in records:
        record["source_file_name"] = file_path
        all_records.append(record)


# COMMAND ----------

from pyspark.sql.types import *

fraud_schema = StructType([
    StructField("claim_id", StringType(), True),
    StructField("fraud_score", IntegerType(), True),
    StructField("risk_category", StringType(), True),
    StructField("flags", ArrayType(StringType()), True),
    StructField("created_ts", StringType(), True),
    StructField("updated_ts", StringType(), True),
    StructField("ingestion_ts", StringType(), True),
    StructField("source_file", StringType(), True),
    StructField("source_system", StringType(), True),
    StructField("source_file_name", StringType(), True)
])


# COMMAND ----------

df_fraud_raw = spark.createDataFrame(all_records, schema=fraud_schema)


# COMMAND ----------

from pyspark.sql.functions import current_timestamp, monotonically_increasing_id, lit

df_fraud_bronze = (
    df_fraud_raw
    .withColumn("bronze_ingest_ts", current_timestamp())
    .withColumn("bronze_source_file", lit(""))   # CE cannot use input_file_name()
    .withColumn("bronze_ingest_batch_id", lit("batch_001"))
)


# COMMAND ----------

spark.sql("CREATE DATABASE IF NOT EXISTS bronze")


# COMMAND ----------

df_fraud_bronze.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("bronze.brz_fraud_scores_raw")


# COMMAND ----------

spark.sql("SELECT * FROM bronze.brz_fraud_scores_raw LIMIT 20")

