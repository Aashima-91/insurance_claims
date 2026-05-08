# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC ### # **RAW** Path (Weather)

# COMMAND ----------

raw_weather_path = "/Workspace/Users/aashima91.gupta@gmail.com/insurance_claims/raw/api_enrichment/"


# COMMAND ----------

# MAGIC %md
# MAGIC ### List JSON **files**

# COMMAND ----------

files = dbutils.fs.ls(raw_weather_path)
json_files = [f.path for f in files if f.name.startswith("weather")]
json_files


# COMMAND ----------

# MAGIC %md
# MAGIC ### Load JSON content **manually**

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

# MAGIC %md
# MAGIC ### Define Weather **Schema**

# COMMAND ----------

from pyspark.sql.types import *

weather_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("event_date", StringType(), True),
    StructField("postcode", IntegerType(), True),
    StructField("state", StringType(), True),
    StructField("weather_event", StringType(), True),
    StructField("severity", StringType(), True),
    StructField("temperature_c", IntegerType(), True),
    StructField("rainfall_mm", IntegerType(), True),
    StructField("wind_speed_kmh", IntegerType(), True),
    StructField("created_ts", StringType(), True),
    StructField("updated_ts", StringType(), True),
    StructField("ingestion_ts", StringType(), True),
    StructField("source_file", StringType(), True),
    StructField("source_system", StringType(), True),
    StructField("source_file_name", StringType(), True)
])


# COMMAND ----------

# MAGIC %md
# MAGIC ### Convert Python list → Spark **DataFrame**

# COMMAND ----------

df_weather_raw = spark.createDataFrame(all_records, schema=weather_schema)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Add Bronze **Metadata**

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, monotonically_increasing_id, lit

df_weather_bronze = (
    df_weather_raw
    .withColumn("bronze_ingest_ts", current_timestamp())
    .withColumn("bronze_source_file", lit(""))   # CE cannot use input_file_name()
    .withColumn("bronze_ingest_batch_id", lit("batch_001"))
)


# COMMAND ----------

spark.sql("CREATE DATABASE IF NOT EXISTS bronze")


# COMMAND ----------

df_weather_bronze.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("bronze.brz_weather_raw")


# COMMAND ----------

spark.sql("SELECT * FROM bronze.brz_weather_raw LIMIT 20")

