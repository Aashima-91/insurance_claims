# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %run "../../../configs/common/common_config"

# COMMAND ----------

# 33_silver_policy_business

from pyspark.sql import functions as F

policy = spark.table(f"{SILVER_DB}.slv_policy_technical").drop(
    "created_ts", "updated_ts", "ingestion_ts",
    "source_file", "source_system", "source_file_name",
    "bronze_ingest_ts", "bronze_ingest_batch_id", "bronze_source_file"
)
product = spark.table(f"{SILVER_DB}.slv_product_mapping_technical").drop(
    "created_ts",
    "updated_ts",
    "ingestion_ts",
    "source_file",
    "source_system",
    "source_file_name",
    "bronze_ingest_ts",
    "bronze_ingest_batch_id",
    "bronze_source_file",
    "lob",
    "product_name"
)

policy_biz = (
    policy.alias("p")
    .join(product.alias("pr"), "product_code", "left")
    .withColumn("policy_term_days",
                F.datediff(F.col("policy_end_date"), F.col("policy_start_date")))
    .withColumn("is_active",
                F.current_date().between(F.col("policy_start_date"), F.col("policy_end_date")))
)

# Drop metadata from left table too
policy_biz = policy_biz.drop(
    "created_ts", "updated_ts", "ingestion_ts",
    "source_file", "source_system", "source_file_name",
    "bronze_ingest_ts", "bronze_ingest_batch_id", "bronze_source_file"
)

spark.sql("DROP TABLE IF EXISTS silver.slv_policy_business")
          
policy_biz.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{SILVER_DB}.slv_policy_business_v2"
)

