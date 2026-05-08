# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %run "../../../configs/common/common_config"

# COMMAND ----------

from pyspark.sql import functions as F

bronze_weather = spark.table(f"{BRONZE_DB}.brz_weather_raw")

silver_weather_tech = (
    bronze_weather.select(
        "event_id",
        F.to_date("event_date").alias("event_date"),
        "state",
        F.col("postcode").cast("string").alias("postcode"),
        "weather_event",
        F.col("wind_speed_kmh").cast("double"),
        F.col("rainfall_mm").cast("double"),
        F.col("temperature_c").cast("double"),
        "severity",
        "source_file",
        "source_system"
    )
)

(
    silver_weather_tech.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{SILVER_DB}.slv_weather_technical")
)

