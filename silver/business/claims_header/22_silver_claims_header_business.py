# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Silver business — claims header
# MAGIC **Reads:** `slv_claims_header_technical`, `slv_policy_technical`, `slv_postcode_region_technical`,
# MAGIC `slv_weather_technical` → **Writes:** `silver.slv_claims_header_business` (1 row per claim)
# MAGIC
# MAGIC NEW: weather matching happens here, per claim (postcode + incident date), aggregated so a claim can never
# MAGIC be duplicated. Previously weather was joined the other way round and duplicated weather events.

# COMMAND ----------

# MAGIC %run ../../../configs/common/common_config

# COMMAND ----------

claims = drop_metadata(spark.table(f"{SILVER_DB}.slv_claims_header_technical"))

# Only the policy attributes a claim needs (full policy detail lives in dim_policy)
policy = spark.table(f"{SILVER_DB}.slv_policy_technical").select(
    "policy_id", "policy_number", "product_code", "customer_id",
    "policy_start_date", "policy_end_date"
)

postcode = spark.table(f"{SILVER_DB}.slv_postcode_region_technical").select(
    F.col("postcode").alias("pc_postcode"), F.col("state").alias("pc_state"),
    "suburb", "region", "risk_zone"
)

# One row per postcode + day, so the join below is at most 1:1
weather_daily = (
    spark.table(f"{SILVER_DB}.slv_weather_technical")
    .groupBy(F.col("postcode").alias("w_postcode"), F.col("event_date").alias("w_event_date"))
    .agg(
        F.count("*").alias("weather_event_count"),
        (F.max(F.col("severity").isin(SEVERE_WEATHER_LEVELS).cast("int")) == 1).alias("is_severe_weather"),
        F.concat_ws(", ", F.array_sort(F.collect_set("weather_event"))).alias("weather_events"),
    )
)

claims_biz = (
    claims
    .join(policy, "policy_id", "left")
    .join(postcode,
          (F.col("loss_postcode") == F.col("pc_postcode")) & (F.col("loss_state") == F.col("pc_state")),
          "left")
    .join(weather_daily,
          (F.col("loss_postcode") == F.col("w_postcode")) & (F.col("incident_date") == F.col("w_event_date")),
          "left")
    .drop("pc_postcode", "pc_state", "w_postcode", "w_event_date")
    .withColumn("claim_reporting_delay_days", F.datediff("reported_date", "incident_date"))
    .withColumn("is_large_claim", F.col("estimated_loss_amount") > LARGE_CLAIM_THRESHOLD)
    .withColumn("policy_active",
                F.col("incident_date").between(F.col("policy_start_date"), F.col("policy_end_date")))
    .withColumn("weather_claim_match", F.col("weather_event_count").isNotNull())
    .withColumn("weather_event_count", F.coalesce("weather_event_count", F.lit(0)))
    .withColumn("is_severe_weather", F.coalesce("is_severe_weather", F.lit(False)))
)

write_table(claims_biz, SILVER_DB, "slv_claims_header_business")
