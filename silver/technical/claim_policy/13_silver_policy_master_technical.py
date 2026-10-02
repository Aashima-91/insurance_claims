# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Silver technical — policy master
# MAGIC **Reads:** `bronze.brz_policy_master_raw` → **Writes:** `silver.slv_policy_technical` (1 row per `policy_id`)
# MAGIC
# MAGIC Fix: `premium_amount` was never cast (stayed a string, so `premium_amount < 0` compared text).

# COMMAND ----------

# MAGIC %run ../../../configs/common/common_config

# COMMAND ----------

bronze_pol = ensure_columns(spark.table(f"{BRONZE_DB}.brz_policy_master_raw"))

silver_pol_tech = (
    trim_all_strings(bronze_pol)
    .withColumn("policy_start_date", to_date_safe("policy_start_date"))
    .withColumn("policy_end_date", to_date_safe("policy_end_date"))
    .withColumn("sum_insured", to_decimal("sum_insured"))
    .withColumn("premium_amount", to_decimal("premium_amount"))      # NEW: was left as string
    .withColumn("risk_postcode", to_postcode("risk_postcode"))
    .withColumn("created_ts", to_ts_safe("created_ts"))
    .withColumn("updated_ts", to_ts_safe("updated_ts"))
    .withColumn("ingestion_ts", to_ts_safe("ingestion_ts"))
    .filter(F.col("policy_id").isNotNull())
)

silver_pol_tech = dedupe_latest(silver_pol_tech, ["policy_id"])

write_table(silver_pol_tech, SILVER_DB, "slv_policy_technical")
