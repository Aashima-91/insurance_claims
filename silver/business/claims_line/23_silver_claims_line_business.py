# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %run "../../../configs/common/common_config"

# COMMAND ----------

# 32_silver_claims_line_business

from pyspark.sql import functions as F

line = spark.table(f"{SILVER_DB}.slv_claims_line_technical").drop(
    "created_ts", "updated_ts", "ingestion_ts",
    "source_file", "source_system", "source_file_name",
    "bronze_ingest_ts", "bronze_ingest_batch_id", "bronze_source_file"
)
header = spark.table(f"{SILVER_DB}.slv_claims_header_business_v2").drop(
    "created_ts",
    "updated_ts",
    "ingestion_ts",
    "source_file",
    "source_system",
    "source_file_name",
    "bronze_ingest_ts",
    "bronze_ingest_batch_id",
    "bronze_source_file"
)

line_biz = (
    line.alias("l")
    .join(header.alias("h"), "claim_id", "left")
    .withColumn("is_approved", F.col("approved_amount") > 0)
    .withColumn("line_severity",
                F.when(F.col("approved_amount") > 2000, "High")
                 .when(F.col("approved_amount") > 500, "Medium")
                 .otherwise("Low"))
)

line_biz = line_biz.drop(
    "created_ts", "updated_ts", "ingestion_ts",
    "source_file", "source_system", "source_file_name",
    "bronze_ingest_ts", "bronze_ingest_batch_id", "bronze_source_file"
)

spark.sql("DROP TABLE IF EXISTS silver.slv_claims_line_business")

line_biz.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{SILVER_DB}.slv_claims_line_business_v2"
)

