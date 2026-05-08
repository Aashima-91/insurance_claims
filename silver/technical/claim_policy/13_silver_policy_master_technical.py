# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %run "../../../configs/common/common_config"

# COMMAND ----------

bronze_pol = spark.table(f"{BRONZE_DB}.brz_policy_master_raw")

silver_pol_tech = (
    bronze_pol
    .withColumn("policy_id", F.trim("policy_id"))
    .withColumn("product_code", F.trim("product_code"))
    .withColumn("policy_number", F.trim("policy_number"))
    .withColumn("customer_id", F.trim("customer_id"))
    .withColumn("policy_status", F.trim("policy_status"))
    .withColumn("policy_start_date", F.to_date("policy_start_date"))
    .withColumn("policy_end_date", F.to_date("policy_end_date"))
    .withColumn("sum_insured", F.col("sum_insured").cast("double"))
    .withColumn("created_ts", F.to_timestamp("created_ts"))
    .withColumn("updated_ts", F.to_timestamp("updated_ts"))
    .withColumn("ingestion_ts", F.to_timestamp("ingestion_ts"))
    .filter(F.col("policy_id").isNotNull())
)

(
    silver_pol_tech.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{SILVER_DB}.slv_policy_technical")
)

