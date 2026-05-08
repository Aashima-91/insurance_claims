# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
from pyspark.sql.types import *
import json
from pyspark.sql.functions import current_timestamp, lit, monotonically_increasing_id

claims_header_schema = StructType([
    StructField("claim_id", StringType(), True),
    StructField("claim_number", StringType(), True),
    StructField("policy_id", StringType(), True),
    StructField("claim_type", StringType(), True),
    StructField("cause_of_loss_code", StringType(), True),
    StructField("cause_of_loss_desc", StringType(), True),
    StructField("cause_of_loss", StringType(), True),
    StructField("incident_date", StringType(), True),
    StructField("reported_date", StringType(), True),
    StructField("claim_status", StringType(), True),
    StructField("claimant_name", StringType(), True),
    StructField("claimant_contact", StringType(), True),
    StructField("loss_location", StringType(), True),
    StructField("loss_postcode", IntegerType(), True),
    StructField("loss_state", StringType(), True),
    StructField("estimated_loss_amount", IntegerType(), True),
    StructField("reported_channel", StringType(), True),
    StructField("fraud_flag", StringType(), True),
    StructField("created_ts", StringType(), True),
    StructField("updated_ts", StringType(), True),
    StructField("ingestion_ts", StringType(), True),
    StructField("source_file", StringType(), True),
    StructField("source_system", StringType(), True)
])


# COMMAND ----------

# Read JSON from workspace using Python, then convert to Spark DataFrame

raw_claim_header_path = "/Workspace/Users/aashima91.gupta@gmail.com/insurance_claims/raw/cloud_claims/"

# Read files using dbutils and load as Python objects
files = dbutils.fs.ls(raw_claim_header_path)
json_files = [f.path for f in files if f.name.startswith('claims_header')]

# Read JSON content via dbutils.fs.head (for small files)
all_records = []
for file_path in json_files:
    # Read entire file content (adjust maxBytes if files are larger)
    content = dbutils.fs.head(file_path, 10000000)  # Read up to ~10MB
    # Parse as JSON array
    records = json.loads(content)
    # Add source file info to each record
    for record in records:
        record['source_file_name'] = file_path
        all_records.append(record)

# Create DataFrame from Python list
df_from_json = spark.createDataFrame(all_records, schema=claims_header_schema)


# COMMAND ----------

# DBTITLE 1,Cell 5

df_claim_header_bronze = (
    df_from_json
    .withColumn("bronze_ingest_ts", current_timestamp())
    .withColumn("bronze_source_file", lit(""))
    .withColumn("bronze_ingest_batch_id", lit("batch_001"))
)

# COMMAND ----------

# Create database and write to table
spark.sql("CREATE DATABASE IF NOT EXISTS bronze")

# COMMAND ----------

df_claim_header_bronze.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("bronze.brz_claim_header_raw")

# COMMAND ----------

spark.sql("SELECT * FROM bronze.brz_claim_header_raw LIMIT 20")

