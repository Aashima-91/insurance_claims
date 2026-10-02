# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Common config (shared by every notebook via `%run`)
# MAGIC
# MAGIC One place for environment, catalog, paths, business thresholds and helper functions.
# MAGIC
# MAGIC | env    | Where it runs                 | Unity Catalog catalog | Landing files                                   |
# MAGIC |--------|-------------------------------|-----------------------|-------------------------------------------------|
# MAGIC | `dev`  | Databricks Free Edition       | `workspace`           | `/Volumes/workspace/raw/landing` (managed volume) |
# MAGIC | `prod` | Azure Databricks (your trial) | `insurance`           | `/Volumes/insurance/raw/landing` → ADLS `landing` container |
# MAGIC
# MAGIC Set the `env` widget at the top of a notebook (or pass `env` as a job parameter). Default is `dev`.

# COMMAND ----------

from datetime import datetime

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import *

# ---------------------------------------------------------------------------
# 1. Environment
# ---------------------------------------------------------------------------
dbutils.widgets.dropdown("env", "dev", ["dev", "prod"], "Environment")
dbutils.widgets.text("batch_id", "", "Batch id (job run id)")

ENV = dbutils.widgets.get("env").strip().lower()

ENV_CONFIG = {
    "dev":  {"catalog": "workspace"},   # Databricks Free Edition default catalog
    "prod": {"catalog": "insurance"},   # created by setup/00_setup_environment on Azure
}
CATALOG = ENV_CONFIG[ENV]["catalog"]

# Schemas (databases) inside the catalog
RAW_DB    = "raw"
BRONZE_DB = "bronze"
SILVER_DB = "silver"
GOLD_DB   = "gold"

# Files land here (UC volumes). On Azure `landing` is an EXTERNAL volume on the ADLS landing container.
LANDING_PATH    = f"/Volumes/{CATALOG}/{RAW_DB}/landing"
CHECKPOINT_PATH = f"/Volumes/{CATALOG}/{RAW_DB}/checkpoints"

# Every unqualified table name (bronze.x, silver.y, gold.z) now resolves inside CATALOG,
# including in %sql cells of the calling notebook.
spark.sql(f"USE CATALOG {CATALOG}")

# Job passes {{job.run_id}}; manual runs get a timestamp so every load is traceable.
BATCH_ID = dbutils.widgets.get("batch_id").strip() or datetime.now().strftime("manual_%Y%m%d_%H%M%S")

# ---------------------------------------------------------------------------
# 2. Business thresholds (were hard-coded inside notebooks)
# ---------------------------------------------------------------------------
LARGE_CLAIM_THRESHOLD   = 5000   # estimated_loss_amount above this = large claim
LINE_HIGH_SEVERITY      = 2000   # approved_amount thresholds for claim line severity
LINE_MEDIUM_SEVERITY    = 500
FRAUD_SCORE_MAX         = 100    # fraud scores arrive on a 0-100 scale (check your raw files!)
FRAUD_HIGH_THRESHOLD    = 60
FRAUD_MEDIUM_THRESHOLD  = 30
SEVERE_WEATHER_LEVELS   = ["Severe", "Extreme"]
VALID_WEATHER_SEVERITY  = ["Minor", "Moderate", "Severe", "Extreme"]

# ---------------------------------------------------------------------------
# 3. Column lists
# ---------------------------------------------------------------------------
# Technical/lineage columns that business tables should not carry.
METADATA_COLS = [
    "created_ts", "updated_ts", "ingestion_ts",
    "source_file", "source_system", "source_file_name",
    "bronze_ingest_ts", "bronze_ingest_batch_id", "bronze_source_file",
    "_rescued_data",
]

print(f"ENV={ENV} | CATALOG={CATALOG} | LANDING_PATH={LANDING_PATH} | BATCH_ID={BATCH_ID}")

# COMMAND ----------

# ---------------------------------------------------------------------------
# 4. Helper functions
# ---------------------------------------------------------------------------

def drop_metadata(df: DataFrame, extra: list = None) -> DataFrame:
    """Drop lineage/technical columns (replaces the 9-column drop list copied into every notebook)."""
    return df.drop(*(METADATA_COLS + (extra or [])))

SOURCE_AUDIT_COLS = ["created_ts", "updated_ts", "ingestion_ts", "source_file", "source_system"]


def ensure_columns(df: DataFrame, cols: list = None) -> DataFrame:
    """Add expected columns the source files don't contain, as NULL strings."""
    for c in (cols or SOURCE_AUDIT_COLS):
        if c not in df.columns:
            print(f"note: column `{c}` not in source files — added as NULL")
            df = df.withColumn(c, F.lit(None).cast("string"))
    return df
    
def trim_all_strings(df: DataFrame) -> DataFrame:
    """Trim every string column; turn empty strings into NULL."""
    for field in df.schema.fields:
        if isinstance(field.dataType, StringType):
            c = F.trim(F.col(field.name))
            df = df.withColumn(field.name, F.when(c == "", None).otherwise(c))
    return df


def to_decimal(col_name: str, precision: str = "18,2"):
    """Safe cast for money: bad values become NULL instead of failing the job (ANSI mode is on in serverless)."""
    return F.expr(f"try_cast(`{col_name}` AS DECIMAL({precision}))")


def to_double(col_name: str):
    return F.expr(f"try_cast(`{col_name}` AS DOUBLE)")


def to_int(col_name: str):
    return F.expr(f"try_cast(`{col_name}` AS INT)")


def to_date_safe(col_name: str):
    """Accepts yyyy-MM-dd and ISO timestamps; anything else becomes NULL (caught by DQ checks)."""
    return F.expr(f"try_cast(`{col_name}` AS DATE)")


def to_ts_safe(col_name: str):
    return F.expr(f"try_cast(`{col_name}` AS TIMESTAMP)")


def to_postcode(col_name: str):
    """Australian postcodes are 4-char strings: keep leading zeros (NT = 08xx)."""
    c = F.trim(F.col(col_name).cast("string"))
    return F.when(c.isNull() | (c == ""), None).otherwise(F.lpad(c, 4, "0"))


def dedupe_latest(df: DataFrame, keys: list, order_cols: list = None) -> DataFrame:
    """Keep one row per key: the most recent by updated_ts, then by bronze_ingest_ts.
    Bronze is append-only, so re-sent files would otherwise double-count in gold."""
    order_cols = order_cols or ["updated_ts", "bronze_ingest_ts"]
    order = [F.col(c).desc_nulls_last() for c in order_cols if c in df.columns]
    w = Window.partitionBy(*keys).orderBy(*order) if order else Window.partitionBy(*keys).orderBy(F.lit(1))
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def write_table(df: DataFrame, schema: str, table: str) -> None:
    """Full rebuild of a silver/gold table (idempotent: safe to re-run)."""
    (df.write.format("delta")
       .mode("overwrite")
       .option("overwriteSchema", "true")
       .saveAsTable(f"{schema}.{table}"))
    print(f"Wrote {schema}.{table}: {spark.table(f'{schema}.{table}').count():,} rows")


def ingest_to_bronze(source_folder: str, file_glob: str, file_format: str, target_table: str,
                     reader_options: dict = None, schema_hints: str = None) -> None:
    """Incremental, append-only bronze load with Auto Loader.

    - Only NEW files in LANDING_PATH/source_folder are read (checkpoint remembers processed files).
    - All columns land as STRING (inferColumnTypes=false): bronze = raw, typing happens in silver.
    - `bronze_source_file` is the real file path via `_metadata.file_path`.
    - trigger(availableNow=True) = process everything new, then stop (works on serverless + job clusters).
    """
    checkpoint = f"{CHECKPOINT_PATH}/{target_table}"
    reader = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", file_format)
        .option("cloudFiles.schemaLocation", f"{checkpoint}/_schema")
        .option("cloudFiles.inferColumnTypes", "false")
        .option("pathGlobFilter", file_glob)
    )
    if schema_hints:
        reader = reader.option("cloudFiles.schemaHints", schema_hints)
    for k, v in (reader_options or {}).items():
        reader = reader.option(k, v)

    # Read _metadata.file_path straight off the source (before any rename/projection).
    raw = (reader.load(f"{LANDING_PATH}/{source_folder}")
           .select("*", F.col("_metadata.file_path").alias("bronze_source_file")))
    # Strip a UTF-8 BOM / stray spaces from CSV header names (old code did this by hand).
    raw = raw.toDF(*[c.replace("﻿", "").strip() for c in raw.columns])

    df = (
        raw
        .withColumn("bronze_ingest_ts", F.current_timestamp())
        .withColumn("bronze_ingest_batch_id", F.lit(BATCH_ID))
    )

    query = (
        df.writeStream
        .option("checkpointLocation", checkpoint)
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .toTable(f"{BRONZE_DB}.{target_table}")
    )
    query.awaitTermination()

    new_rows = spark.table(f"{BRONZE_DB}.{target_table}").filter(F.col("bronze_ingest_batch_id") == BATCH_ID).count()
    total = spark.table(f"{BRONZE_DB}.{target_table}").count()
    print(f"{BRONZE_DB}.{target_table}: +{new_rows:,} new rows this batch, {total:,} total")


class DQ:
    """Collect data-quality results; call .raise_if_failed() at the end so a failed check FAILS the job.

    level="error" -> a failure stops the pipeline (grain, keys, row counts).
    level="warn"  -> printed and counted, but the run continues (business rules, orphans in synthetic data).
    Promote a check to "error" once your data is clean.
    """

    def __init__(self, layer: str):
        self.layer = layer
        self.failures = []
        self.warnings = []

    def check(self, name: str, query: str, level: str = "error", show: int = 20) -> None:
        df = spark.sql(query)
        n = df.count()
        if n == 0:
            print(f"PASS  {name}")
            return
        tag = "FAIL" if level == "error" else "WARN"
        print(f"{tag}  {name}  ({n:,} rows)")
        df.show(min(n, show), truncate=False)
        (self.failures if level == "error" else self.warnings).append(f"{name} ({n:,} rows)")

    def raise_if_failed(self) -> None:
        if self.warnings:
            print(f"\n{len(self.warnings)} warning(s):\n  - " + "\n  - ".join(self.warnings))
        if self.failures:
            raise AssertionError(f"{self.layer} DQ failed {len(self.failures)} check(s): " + "; ".join(self.failures))
        print(f"\nNo blocking {self.layer} DQ failures.")
