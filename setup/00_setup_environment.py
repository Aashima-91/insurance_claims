# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 00 — One-time environment setup
# MAGIC
# MAGIC Run this **once per environment** before the pipeline (it is safe to re-run).
# MAGIC
# MAGIC **dev (Free Edition)**: creates schemas `raw/bronze/silver/gold` + volumes `raw.landing` and `raw.checkpoints`
# MAGIC in the `workspace` catalog, and (optionally) copies your existing raw files from the Workspace folder into the volume.
# MAGIC
# MAGIC **prod (Azure Databricks)**: before running, create the **storage credential** in Catalog Explorer
# MAGIC (see `docs/AZURE_SETUP.md`, step 5). This notebook then creates the external locations, the `insurance`
# MAGIC catalog on your ADLS account, the schemas and the volumes.
# MAGIC
# MAGIC `reset = yes` drops bronze/silver/gold (and Auto Loader checkpoints) so everything reloads from scratch.
# MAGIC You need this **once** when switching from the old notebooks, because the old bronze tables have typed
# MAGIC columns and the new Auto Loader bronze tables are all-string.

# COMMAND ----------

dbutils.widgets.dropdown("env", "dev", ["dev", "prod"], "Environment")
dbutils.widgets.dropdown("reset", "no", ["no", "yes"], "Drop bronze/silver/gold first?")
# dev only: where your raw files live today (the folder the old bronze notebooks read from)
dbutils.widgets.text("workspace_raw_path", "/Workspace/Users/<your-email>/insurance_claims/raw", "dev: raw folder to copy")
# prod only
dbutils.widgets.text("storage_account", "<storageaccountname>", "prod: ADLS account name")
dbutils.widgets.text("storage_credential", "insurance_adls_cred", "prod: storage credential name")

ENV = dbutils.widgets.get("env")
RESET = dbutils.widgets.get("reset") == "yes"
CATALOG = {"dev": "workspace", "prod": "insurance"}[ENV]
print(f"ENV={ENV}, CATALOG={CATALOG}, RESET={RESET}")

# COMMAND ----------

# MAGIC %md ## prod only — external locations + catalog on ADLS

# COMMAND ----------

if ENV == "prod":
    acct = dbutils.widgets.get("storage_account").strip()
    cred = dbutils.widgets.get("storage_credential").strip()
    lakehouse_url = f"abfss://lakehouse@{acct}.dfs.core.windows.net/"
    landing_url   = f"abfss://landing@{acct}.dfs.core.windows.net/"

    spark.sql(f"CREATE EXTERNAL LOCATION IF NOT EXISTS insurance_lakehouse URL '{lakehouse_url}' "
              f"WITH (STORAGE CREDENTIAL `{cred}`) COMMENT 'Managed storage for the insurance catalog'")
    spark.sql(f"CREATE EXTERNAL LOCATION IF NOT EXISTS insurance_landing URL '{landing_url}' "
              f"WITH (STORAGE CREDENTIAL `{cred}`) COMMENT 'Files landed by Azure Data Factory'")
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG} MANAGED LOCATION '{lakehouse_url}' "
              f"COMMENT 'Insurance claims lakehouse'")
    print("External locations and catalog ready.")
else:
    print("dev: using the built-in `workspace` catalog — nothing to do here.")

# COMMAND ----------

# MAGIC %md ## Schemas + volumes (both environments)

# COMMAND ----------

if RESET:
    for schema in ["gold", "silver", "bronze"]:
        spark.sql(f"DROP SCHEMA IF EXISTS {CATALOG}.{schema} CASCADE")
    # checkpoints must be cleared together with bronze, or Auto Loader will skip already-seen files
    spark.sql(f"DROP VOLUME IF EXISTS {CATALOG}.raw.checkpoints")
    print("Dropped gold/silver/bronze and Auto Loader checkpoints.")

for schema in ["raw", "bronze", "silver", "gold"]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{schema}")

if ENV == "prod":
    # landing = EXTERNAL volume over the ADLS container ADF writes to
    spark.sql(f"CREATE EXTERNAL VOLUME IF NOT EXISTS {CATALOG}.raw.landing "
              f"LOCATION 'abfss://landing@{dbutils.widgets.get('storage_account').strip()}.dfs.core.windows.net/'")
else:
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.raw.landing")

spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.raw.checkpoints")

display(spark.sql(f"SHOW SCHEMAS IN {CATALOG}"))
display(spark.sql(f"SHOW VOLUMES IN {CATALOG}.raw"))

# COMMAND ----------

# MAGIC %md ## dev only — copy your existing raw files into the landing volume
# MAGIC Keeps the same sub-folders the old notebooks used:
# MAGIC `cloud_claims/`, `onprem_policy/`, `payments/`, `reference/`, `api_enrichment/`.
# MAGIC (Alternatively upload them through Catalog Explorer → workspace → raw → landing → *Upload to this volume*.)

# COMMAND ----------

if ENV == "dev":
    src = dbutils.widgets.get("workspace_raw_path").rstrip("/")
    dst = f"/Volumes/{CATALOG}/raw/landing"
    if "<your-email>" in src:
        print("Set the `workspace_raw_path` widget first (or upload files manually) — skipping copy.")
    else:
        for folder in ["cloud_claims", "onprem_policy", "payments", "reference", "api_enrichment"]:
            try:
                # same path style your old bronze notebooks used with dbutils.fs.ls
                dbutils.fs.cp(f"{src}/{folder}", f"{dst}/{folder}", recurse=True)
                print(f"copied {folder}/")
            except Exception as e:
                print(f"could not copy {folder}/: {e}")
    for f in dbutils.fs.ls(dst):
        print(f.path)
