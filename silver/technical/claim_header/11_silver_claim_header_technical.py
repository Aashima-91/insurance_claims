# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Silver technical — claims header
# MAGIC **Reads:** `bronze.brz_claim_header_raw` → **Writes:** `silver.slv_claims_header_technical` (1 row per `claim_id`)
# MAGIC
# MAGIC Changes vs old version: no more DROP of the *business* table here; safe casts; postcode kept as 4-char
# MAGIC string; money as DECIMAL; latest version per claim kept (dedup).

# COMMAND ----------

# MAGIC %run ../../../configs/common/common_config

# COMMAND ----------

bronze_hdr = ensure_columns(spark.table(f"{BRONZE_DB}.brz_claim_header_raw"))

silver_hdr_tech = (
    trim_all_strings(bronze_hdr)
    .drop("fraud_flag")                       # fraud comes from the fraud feed, not the claim system
    .withColumn("loss_postcode", to_postcode("loss_postcode"))
    .withColumn("estimated_loss_amount", to_decimal("estimated_loss_amount"))
    .withColumn("incident_date", to_date_safe("incident_date"))
    .withColumn("reported_date", to_date_safe("reported_date"))
    .withColumn("created_ts", to_ts_safe("created_ts"))
    .withColumn("updated_ts", to_ts_safe("updated_ts"))
    .withColumn("ingestion_ts", to_ts_safe("ingestion_ts"))
    .filter(F.col("claim_id").isNotNull())
)

silver_hdr_tech = dedupe_latest(silver_hdr_tech, ["claim_id"])

write_table(silver_hdr_tech, SILVER_DB, "slv_claims_header_technical")
