# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %run "../../../configs/common/common_config"

# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS {SILVER_DB}.slv_claims_header_technical")
spark.sql(f"DROP TABLE IF EXISTS {SILVER_DB}.slv_claims_header_business_v2")


# COMMAND ----------

# DBTITLE 1,Cell 1
bronze_hdr = spark.table(f"{BRONZE_DB}.brz_claim_header_raw")

silver_hdr_tech = (
    bronze_hdr
    .drop("fraud_flag")   # <— REMOVE IT HERE IF BRONZE HAS IT
    .withColumn("claim_id", F.trim("claim_id"))
    .withColumn("claim_number", F.trim("claim_number"))
    .withColumn("policy_id", F.trim("policy_id"))
    .withColumn("claim_type", F.trim("claim_type"))
    .withColumn("cause_of_loss_code", F.trim("cause_of_loss_code"))
    .withColumn("cause_of_loss_desc", F.trim("cause_of_loss_desc"))
    .withColumn("claimant_name", F.trim("claimant_name"))
    .withColumn("claimant_contact", F.trim("claimant_contact"))
    .withColumn("loss_location", F.trim("loss_location"))
    .withColumn("loss_state", F.trim("loss_state"))
    .withColumn("reported_channel", F.trim("reported_channel"))
    .withColumn("loss_postcode", F.col("loss_postcode").cast("int"))
    .withColumn("estimated_loss_amount", F.col("estimated_loss_amount").cast("double"))
    .withColumn("incident_date", F.to_date("incident_date"))
    .withColumn("reported_date", F.to_date("reported_date"))
    .withColumn("created_ts", F.to_timestamp("created_ts"))
    .withColumn("updated_ts", F.to_timestamp("updated_ts"))
    .withColumn("ingestion_ts", F.to_timestamp("ingestion_ts"))
    .filter(F.col("claim_id").isNotNull())
)


spark.sql(f"DROP TABLE IF EXISTS {SILVER_DB}.slv_claims_header_technical")

(
    silver_hdr_tech.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{SILVER_DB}.slv_claims_header_technical")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN bronze;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE silver.slv_claims_header_technical
