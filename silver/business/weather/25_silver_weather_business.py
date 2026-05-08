# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %run "../../../configs/common/common_config"

# COMMAND ----------

from pyspark.sql import functions as F

weather = spark.table(f"{SILVER_DB}.slv_weather_technical")

claims = (
    spark.table(f"{SILVER_DB}.slv_claims_header_business_v2")
    .withColumnRenamed("loss_postcode", "claim_loss_postcode")
    .withColumnRenamed("incident_date", "claim_incident_date")
)

joined = (
    weather.alias("w")
    .join(
        claims.alias("c"),
        (F.col("w.postcode") == F.col("c.claim_loss_postcode")) &
        (F.col("w.event_date") == F.col("c.claim_incident_date")),
        "left"
    )
)

weather_biz = (
    joined.select(
        # Weather columns
        "w.event_id",
        "w.event_date",
        "w.state",
        "w.postcode",
        "w.weather_event",
        "w.severity",
        "w.temperature_c",
        "w.rainfall_mm",
        "w.wind_speed_kmh",
        "w.source_file",
        "w.source_system",

        # Claim columns (renamed)
        "c.claim_id",
        "c.claim_loss_postcode",
        "c.claim_incident_date",

        # Derived columns
        F.initcap(F.trim(F.col("w.severity"))).alias("severity_norm"),
        F.col("w.severity").isin("Severe", "Extreme").alias("is_severe_weather"),
        F.col("c.claim_id").isNotNull().alias("weather_claim_match")
    )
)

(
    weather_biz.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{SILVER_DB}.slv_weather_business_v2")
)

