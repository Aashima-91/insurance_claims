# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %run "../../../configs/common/common_config"

# COMMAND ----------

from pyspark.sql import functions as F

df = spark.table(f"{BRONZE_DB}.brz_fraud_scores_raw")

fraud_tech = (
    df.select(
        "claim_id",
        F.col("fraud_score").cast("double"),
        "risk_category",
        "flags",
        "source_file",
        "source_system"
    )
)

fraud_tech.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{SILVER_DB}.slv_fraud_scores_technical"
)


# COMMAND ----------

# spark.table(f"{SILVER_DB}.slv_fraud_scores_technical").columns

