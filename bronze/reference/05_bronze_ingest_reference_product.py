# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
raw_product_path = "file:/Workspace/Users/aashima91.gupta@gmail.com/insurance_claims/raw/reference/product_mapping.csv"


# COMMAND ----------

content = dbutils.fs.head(raw_product_path, 10000000)  # read entire file
lines = content.split("\n")

# COMMAND ----------

import csv
from io import StringIO

reader = csv.DictReader(StringIO(content))
records = [row for row in reader]


# COMMAND ----------

from pyspark.sql.types import *
    
product_schema = StructType([
    StructField("product_code", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("lob", StringType(), True),
    StructField("coverage_type", StringType(), True),
    StructField("risk_category", StringType(), True),
    StructField("created_ts", StringType(), True),
    StructField("updated_ts", StringType(), True)
])

df_product_raw = spark.createDataFrame(records, schema=product_schema)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, monotonically_increasing_id, lit

df_product_bronze = (
    df_product_raw
    .withColumn("bronze_ingest_ts", current_timestamp())
    .withColumn("bronze_source_file", lit("product_mapping.csv"))
    .withColumn("bronze_ingest_batch_id", lit("batch_001"))
)


# COMMAND ----------

spark.sql("CREATE DATABASE IF NOT EXISTS bronze")


# COMMAND ----------

df_product_bronze.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("bronze.brz_product_mapping_raw")


# COMMAND ----------

spark.sql("SELECT * FROM bronze.brz_product_mapping_raw LIMIT 20")

