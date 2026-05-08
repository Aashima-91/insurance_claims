# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %run "../../../configs/common/common_config"

# COMMAND ----------

bronze_pay = spark.table(f"{BRONZE_DB}.brz_payments_raw")

silver_pay_tech = (
    bronze_pay
    .withColumn("payment_id", F.trim("payment_id"))
    .withColumn("claim_id", F.trim("claim_id"))
    .withColumn("payment_type", F.trim("payment_type"))
    .withColumn("payment_method", F.trim("payment_method"))
    .withColumn("currency", F.trim("currency"))
    .withColumn("payment_amount", F.col("payment_amount").cast("double"))
    .withColumn("payment_date", F.to_date("payment_date"))
    .withColumn("created_ts", F.to_timestamp("created_ts"))
    .withColumn("updated_ts", F.to_timestamp("updated_ts"))
    .withColumn("ingestion_ts", F.to_timestamp("bronze_ingest_ts"))
    .filter(F.col("payment_id").isNotNull())
)

(
    silver_pay_tech.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{SILVER_DB}.slv_payments_technical")
)

