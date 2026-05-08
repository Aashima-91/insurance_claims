# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
raw_claim_line_path = "/Workspace/Users/aashima91.gupta@gmail.com/insurance_claims/raw/cloud_claims/"


# COMMAND ----------

files = dbutils.fs.ls(raw_claim_line_path)
json_files = [f.path for f in files if f.name.startswith("claims_line")]
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

claims_line_schema = StructType([
    StructField("claim_line_id", StringType(), True),
    StructField("claim_id", StringType(), True),
    StructField("coverage_code", StringType(), True),
    StructField("coverage_desc", StringType(), True),
    StructField("item_type", StringType(), True),
    StructField("claimed_amount", IntegerType(), True),
    StructField("approved_amount", IntegerType(), True),
    StructField("deductible_amount", IntegerType(), True),
    StructField("line_status", StringType(), True),
    StructField("created_ts", StringType(), True),
    StructField("updated_ts", StringType(), True),
    StructField("ingestion_ts", StringType(), True),
    StructField("source_file", StringType(), True),
    StructField("source_system", StringType(), True)
])


# COMMAND ----------

df_claim_line_raw = spark.createDataFrame(all_records, schema=claims_line_schema)


# COMMAND ----------

from pyspark.sql.functions import current_timestamp, monotonically_increasing_id, lit

df_claim_line_bronze = (
    df_claim_line_raw
    .withColumn("bronze_ingest_ts", current_timestamp())
    .withColumn("bronze_source_file", lit(""))   # CE cannot use input_file_name()
    .withColumn("bronze_ingest_batch_id", lit("batch_001"))
)


# COMMAND ----------

spark.sql("CREATE DATABASE IF NOT EXISTS bronze")


# COMMAND ----------

# DBTITLE 1,Cell 8
df_claim_line_bronze.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("bronze.brz_claim_line_raw")

# COMMAND ----------

spark.sql("SELECT * FROM bronze.brz_claim_line_raw LIMIT 20")

