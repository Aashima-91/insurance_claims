# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %run "../../../configs/common/common_config"

# COMMAND ----------

# DBTITLE 1,Cell 2

from pyspark.sql import functions as F

claims = spark.table(f"{SILVER_DB}.slv_claims_header_technical").drop(
    "created_ts", "updated_ts", "ingestion_ts",
    "source_file", "source_system", "source_file_name",
    "bronze_ingest_ts", "bronze_ingest_batch_id", "bronze_source_file"
)
policy = spark.table(f"{SILVER_DB}.slv_policy_technical").drop(
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
postcode = spark.table(f"{SILVER_DB}.slv_postcode_region_technical").drop(
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

claims_biz = (
    claims.alias("c")
    .join(policy.alias("p"), "policy_id", "left")
    .join(
        postcode.alias("pc"),
        (F.col("c.loss_postcode") == F.col("pc.postcode")) &
        (F.col("c.loss_state") == F.col("pc.state")),
        "left"
    )
    .withColumn(
        "claim_reporting_delay_days",
        F.datediff(F.col("reported_date"), F.col("incident_date"))
    )
    .withColumn("is_large_claim", F.col("estimated_loss_amount") > 5000)
    .withColumn(
        "policy_active",
        (F.col("incident_date") >= F.col("policy_start_date")) &
        (F.col("incident_date") <= F.col("policy_end_date"))
    )
)

claims_biz = claims_biz.drop(
    "created_ts", "updated_ts", "ingestion_ts",
    "source_file", "source_system", "source_file_name",
    "bronze_ingest_ts", "bronze_ingest_batch_id", "bronze_source_file"
)

spark.sql("DROP TABLE IF EXISTS silver.slv_claims_header_business")

claims_biz.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{SILVER_DB}.slv_claims_header_business_v2"
)
