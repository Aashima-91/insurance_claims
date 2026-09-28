# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Silver business — claims line
# MAGIC **Reads:** `slv_claims_line_technical`, `slv_claims_header_business` → **Writes:** `silver.slv_claims_line_business`
# MAGIC
# MAGIC Change: only the header columns a line needs are joined (the old version copied the whole wide header onto every line).

# COMMAND ----------

# MAGIC %run ../../../configs/common/common_config

# COMMAND ----------

line = drop_metadata(spark.table(f"{SILVER_DB}.slv_claims_line_technical"))
header = spark.table(f"{SILVER_DB}.slv_claims_header_business").select(
    "claim_id", "policy_id", "claim_type", "claim_status", "incident_date"
)

line_biz = (
    line.join(header, "claim_id", "left")
    .withColumn("is_approved", F.col("approved_amount") > 0)
    .withColumn("line_severity",
                F.when(F.col("approved_amount") > LINE_HIGH_SEVERITY, "High")
                 .when(F.col("approved_amount") > LINE_MEDIUM_SEVERITY, "Medium")
                 .otherwise("Low"))
)

write_table(line_biz, SILVER_DB, "slv_claims_line_business")
