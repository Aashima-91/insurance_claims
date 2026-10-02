# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Silver technical — weather events
# MAGIC **Reads:** `bronze.brz_weather_raw` → **Writes:** `silver.slv_weather_technical` (1 row per `event_id`)

# COMMAND ----------

# MAGIC %run ../../../configs/common/common_config

# COMMAND ----------

bronze_weather = ensure_columns(spark.table(f"{BRONZE_DB}.brz_weather_raw"))

silver_weather_tech = (
    trim_all_strings(bronze_weather)
    .withColumn("event_date", to_date_safe("event_date"))
    .withColumn("postcode", to_postcode("postcode"))           # same type as claims.loss_postcode now
    .withColumn("wind_speed_kmh", to_double("wind_speed_kmh"))
    .withColumn("rainfall_mm", to_double("rainfall_mm"))
    .withColumn("temperature_c", to_double("temperature_c"))
    .withColumn("severity", F.initcap("severity"))
    .withColumn("created_ts", to_ts_safe("created_ts"))
    .withColumn("updated_ts", to_ts_safe("updated_ts"))
    .filter(F.col("event_id").isNotNull())
    .select("event_id", "event_date", "state", "postcode", "weather_event", "severity",
            "wind_speed_kmh", "rainfall_mm", "temperature_c",
            "created_ts", "updated_ts", "source_file", "source_system",
            "bronze_source_file", "bronze_ingest_ts", "bronze_ingest_batch_id")
)

silver_weather_tech = dedupe_latest(silver_weather_tech, ["event_id"])

write_table(silver_weather_tech, SILVER_DB, "slv_weather_technical")
