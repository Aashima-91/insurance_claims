# Insurance Claims Analytics Lakehouse
An end-to-end **Medallion Architecture** on Databricks + Azure that ingests, enriches, validates and models insurance claims data for analytics, reporting and fraud detection.

The same code runs in two environments:

| | dev | prod |
|---|---|---|
| Platform | Databricks **Free Edition** | **Azure Databricks** (Premium, Unity Catalog) |
| Catalog | `workspace` | `insurance` (stored on ADLS Gen2) |
| Files arrive in | volume `workspace.raw.landing` (manual upload) | ADLS `landing` container, landed by **Azure Data Factory** |
| Compute | serverless | single-node job cluster |

---

## 1. Architecture

```
Source systems                Azure Data Factory              Databricks (Unity Catalog)                          Consumers
────────────────              ──────────────────              ─────────────────────────────────────────           ─────────
claims (JSON)     ─┐                                           BRONZE  Auto Loader, append-only, all-string
policy (CSV)      ─┤  Copy → ADLS landing/<src>/yyyy/MM/dd ──►  SILVER  technical: typed, trimmed, deduplicated     Power BI
payments (CSV)    ─┤  then Databricks Job activity                      business: joins, weather match, fraud bands  SQL
reference (CSV)   ─┤                                           GOLD    star schema: dim_* / fact_* (surrogate keys)  ML
weather, fraud    ─┘                                           DQ      silver + gold checks fail the run on errors
```

Orchestration: one Databricks job with 32 dependent tasks, defined as code in `databricks.yml` (Asset Bundle),
triggered by ADF on Azure. CI/CD: GitHub Actions deploys the bundle.

## 2. Business domains
Claims (header + line) · Policy · Payments · Reference (postcode→region, product) · Weather enrichment · Fraud enrichment.

## 3. Key features
- **Incremental ingestion** with Auto Loader: only new files, real file lineage (`_metadata.file_path`), batch id = job run id
- **Technical vs business** separation in silver; deduplication to the latest version of each record
- **Weather ↔ claim matching** on postcode + incident date, without duplicating either side
- **Star schema** with stable surrogate keys (`xxhash64`), continuous `dim_date` (incl. Australian financial year), PII hashed
- **Data quality framework**: `DQ` helper; `error` checks fail the job, `warn` checks are reported
- **One config** (`configs/common/common_config.py`) switches dev/prod via an `env` parameter

## 4. Folder structure
```
insurance_claims/
├── configs/common/common_config.py   # env, catalog, paths, thresholds, helpers (Auto Loader, dedup, DQ)
├── setup/00_setup_environment.py     # one-time: schemas, volumes; on Azure also external locations + catalog
├── bronze/     01–08  Auto Loader ingestion
├── silver/     11–18 technical · 21–26 business · validation/
├── gold/       31–34 dims · 41–44 facts · validation/
├── databricks.yml, resources/        # the pipeline as a job (Asset Bundle) — dev + prod targets
├── .github/workflows/deploy.yml      # CI/CD to Azure
└── docs/       AZURE_SETUP.md (Azure build + costs) · CHANGES.md (what changed and why)
```

## 5. How to run — dev (Databricks Free Edition)
1. Clone this repo as a **Git folder** in your workspace.
2. Run `setup/00_setup_environment` with `env=dev`. Set `workspace_raw_path` to copy your raw files into
   `/Volumes/workspace/raw/landing/` (sub-folders `cloud_claims/`, `onprem_policy/`, `payments/`, `reference/`, `api_enrichment/`),
   or upload them through Catalog Explorer. Use `reset=yes` once when upgrading from the old notebooks.
3. Run the notebooks in number order (bronze → silver → validation → gold → gold DQ), **or** deploy and run the job:
   ```bash
   databricks bundle deploy -t dev
   databricks bundle run    -t dev insurance_claims_pipeline
   ```

## 6. How to run — prod (Azure)
Follow **[docs/AZURE_SETUP.md](docs/AZURE_SETUP.md)**: resource group, ADLS Gen2, Access Connector, Unity Catalog credential,
`00_setup_environment` with `env=prod`, `databricks bundle deploy -t prod`, ADF pipeline + trigger. It includes a cost
estimate and tear-down steps.

## 7. Gold model
| Table | Grain |
|---|---|
| `fact_claims` | 1 row per claim — loss, claimed/approved/paid totals, delays, weather + fraud flags |
| `fact_payments` | 1 row per payment |
| `fact_weather_events` | 1 row per weather event, with number of matching claims |
| `fact_fraud_scores` | 1 row per scored claim |
| `dim_claim`, `dim_policy`, `dim_weather`, `dim_date` | 1 row per entity / day |

## 8. Purpose
A portfolio project showing Databricks + Azure data-engineering skills in an insurance (BFSI) context:
medallion design, incremental ingestion, dimensional modelling, data quality, orchestration-as-code and CI/CD.
