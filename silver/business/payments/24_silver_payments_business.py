# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Silver business — payments
# MAGIC **Reads:** `slv_payments_technical`, `slv_claims_header_business` → **Writes:** `silver.slv_payments_business`

# COMMAND ----------

# MAGIC %run ../../../configs/common/common_config

# COMMAND ----------

pay = drop_metadata(spark.table(f"{SILVER_DB}.slv_payments_technical"))
claims = spark.table(f"{SILVER_DB}.slv_claims_header_business").select(
    "claim_id", "policy_id", "incident_date", "reported_date"
)
if "policy_id" in pay.columns:          # prefer the claim's policy if payments also carry one
    pay = pay.drop("policy_id")

pay_biz = (
    pay.join(claims, "claim_id", "left")
    .withColumn("is_settlement", F.col("payment_type") == "Settlement")
    .withColumn("is_reimbursement", F.col("payment_type") == "Reimbursement")
    .withColumn("payment_delay_days", F.datediff("payment_date", "reported_date"))
)

write_table(pay_biz, SILVER_DB, "slv_payments_business")
