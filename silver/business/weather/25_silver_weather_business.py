# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Silver business — weather events
# MAGIC **Reads:** `slv_weather_technical`, `slv_claims_header_technical` → **Writes:** `silver.slv_weather_business`
# MAGIC
# MAGIC FIX: stays at **1 row per `event_id`**. The old version left-joined events to claims, so an event with
# MAGIC 5 claims became 5 rows and `dim_weather` / `fact_weather_events` had duplicate event_ids.
# MAGIC The claim link now lives on the claim (`slv_claims_header_business.weather_claim_match`); here we just count.

# COMMAND ----------

# MAGIC %run ../../../configs/common/common_config

# COMMAND ----------

weather = drop_metadata(spark.table(f"{SILVER_DB}.slv_weather_technical"))

claims_per_day = (
    spark.table(f"{SILVER_DB}.slv_claims_header_technical")
    .groupBy(F.col("loss_postcode").alias("c_postcode"), F.col("incident_date").alias("c_date"))
    .agg(F.count("*").alias("matched_claim_count"))
)

weather_biz = (
    weather
    .join(claims_per_day,
          (F.col("postcode") == F.col("c_postcode")) & (F.col("event_date") == F.col("c_date")),
          "left")
    .drop("c_postcode", "c_date")
    .withColumn("matched_claim_count", F.coalesce("matched_claim_count", F.lit(0)))
    .withColumn("weather_claim_match", F.col("matched_claim_count") > 0)
    .withColumn("severity_norm", F.initcap(F.trim("severity")))
    .withColumn("is_severe_weather", F.col("severity_norm").isin(SEVERE_WEATHER_LEVELS))
)

write_table(weather_biz, SILVER_DB, "slv_weather_business")
