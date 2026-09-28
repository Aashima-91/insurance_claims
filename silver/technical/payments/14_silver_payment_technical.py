# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Silver technical — payments
# MAGIC **Reads:** `bronze.brz_payments_raw` → **Writes:** `silver.slv_payments_technical` (1 row per `payment_id`)
# MAGIC
# MAGIC Fix: `ingestion_ts` now comes from the source column like every other table (it was overwritten with
# MAGIC `bronze_ingest_ts` here only).

# COMMAND ----------

# MAGIC %run ../../../configs/common/common_config

# COMMAND ----------

bronze_pay = spark.table(f"{BRONZE_DB}.brz_payments_raw")

silver_pay_tech = (
    trim_all_strings(bronze_pay)
    .withColumn("payment_amount", to_decimal("payment_amount"))
    .withColumn("payment_date", to_date_safe("payment_date"))
    .withColumn("created_ts", to_ts_safe("created_ts"))
    .withColumn("updated_ts", to_ts_safe("updated_ts"))
    .withColumn("ingestion_ts", F.coalesce(to_ts_safe("ingestion_ts"), F.col("bronze_ingest_ts"))
                if "ingestion_ts" in bronze_pay.columns else F.col("bronze_ingest_ts"))
    .filter(F.col("payment_id").isNotNull())
)

silver_pay_tech = dedupe_latest(silver_pay_tech, ["payment_id"])

write_table(silver_pay_tech, SILVER_DB, "slv_payments_technical")
