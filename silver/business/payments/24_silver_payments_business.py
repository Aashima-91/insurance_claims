# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %run "../../../configs/common/common_config"

# COMMAND ----------

from pyspark.sql import functions as F

pay = spark.table(f"{SILVER_DB}.slv_payments_technical").drop(
    "created_ts", "updated_ts", "ingestion_ts",
    "source_file", "source_system", "source_file_name",
    "bronze_ingest_ts", "bronze_ingest_batch_id", "bronze_source_file"
)

claims = spark.table(f"{SILVER_DB}.slv_claims_header_business_v2").drop(
    "created_ts", "updated_ts", "ingestion_ts",
    "source_file", "source_system", "source_file_name",
    "bronze_ingest_ts", "bronze_ingest_batch_id", "bronze_source_file"
)

pay_biz = (
    pay.alias("p")
    .join(claims.alias("c"), "claim_id", "left")
    .withColumn("is_settlement", F.col("payment_type") == "Settlement")
    .withColumn("is_reimbursement", F.col("payment_type") == "Reimbursement")
    .withColumn(
        "payment_delay_days",
        F.datediff(F.col("payment_date"), F.col("reported_date"))
    )
)

# Drop metadata from left table too
pay_biz = pay_biz.drop(
    "created_ts", "updated_ts", "ingestion_ts",
    "source_file", "source_system", "source_file_name",
    "bronze_ingest_ts", "bronze_ingest_batch_id", "bronze_source_file"
)

spark.sql("DROP TABLE IF EXISTS silver.slv_payments_business")

pay_biz.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{SILVER_DB}.slv_payments_business_v2"
)

