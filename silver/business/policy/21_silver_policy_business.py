# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Silver business — policy
# MAGIC **Reads:** `slv_policy_technical`, `slv_product_mapping_technical` → **Writes:** `silver.slv_policy_business`
# MAGIC
# MAGIC Renamed from `slv_policy_business_v2` (the `_v2` suffix and the stray `DROP TABLE` are gone).

# COMMAND ----------

# MAGIC %run ../../../configs/common/common_config

# COMMAND ----------

policy = drop_metadata(spark.table(f"{SILVER_DB}.slv_policy_technical"))
# lob / product_name already come from the policy file, so take only the extra attributes from product
product = spark.table(f"{SILVER_DB}.slv_product_mapping_technical").select(
    "product_code", "coverage_type", "risk_category"
)

policy_biz = (
    policy.join(product, "product_code", "left")
    .withColumn("policy_term_days", F.datediff("policy_end_date", "policy_start_date"))
    # is_active depends on the day the pipeline runs — documented as "active as of load date"
    .withColumn("is_active", F.current_date().between(F.col("policy_start_date"), F.col("policy_end_date")))
)

write_table(policy_biz, SILVER_DB, "slv_policy_business")
