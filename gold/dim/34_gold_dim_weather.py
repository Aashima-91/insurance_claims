# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %sql
# MAGIC CREATE TABLE gold.dim_weather AS
# MAGIC SELECT *
# MAGIC FROM silver.slv_weather_business_v2;
# MAGIC
