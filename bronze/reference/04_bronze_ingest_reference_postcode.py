# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
raw_reference_path = "/Workspace/Users/aashima91.gupta@gmail.com/insurance_claims/raw/reference/postcode_region_mapping.csv"


# COMMAND ----------

content = dbutils.fs.head(raw_reference_path, 10000000)  # read entire file
lines = content.split("\n")


# COMMAND ----------

import csv
from io import StringIO

reader = csv.DictReader(StringIO(content))
records = [row for row in reader]


# COMMAND ----------

# DBTITLE 1,Cell 4
from pyspark.sql.types import *
from datetime import datetime

timestamp_now = datetime.now()

clean_records = []
for r in records:
    clean_records.append({
        "postcode": int(r["postcode"].strip()),
        "suburb": r["suburb"].strip(),
        "region": r["region"].strip(),
        "state": r["state"].strip(),
        "risk_zone": r["risk_zone"].strip(),
        "created_ts": timestamp_now,
        "updated_ts": timestamp_now
    })
    
postcode_schema = StructType([
    StructField("postcode", IntegerType(), True),
    StructField("suburb", StringType(), True),
    StructField("region", StringType(), True),
    StructField("state", StringType(), True),
    StructField("risk_zone", StringType(), True),
    StructField("created_ts", TimestampType(), True),
    StructField("updated_ts", TimestampType(), True)
])

df_postcode_raw = spark.createDataFrame(clean_records, schema=postcode_schema)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, monotonically_increasing_id, lit

df_postcode_bronze = (
    df_postcode_raw
    .withColumn("bronze_ingest_ts", current_timestamp())
    .withColumn("bronze_source_file", lit("postcode_region_mapping.csv"))
    .withColumn("bronze_ingest_batch_id", lit("batch_001"))
)


# COMMAND ----------

spark.sql("CREATE DATABASE IF NOT EXISTS bronze")


# COMMAND ----------

df_postcode_bronze.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("bronze.brz_postcode_region_raw")

# COMMAND ----------

spark.sql("SELECT * FROM bronze.brz_postcode_region_raw LIMIT 20")

