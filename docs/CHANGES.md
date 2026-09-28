# What changed and where — review fixes + Azure-ready refactor

This file lists **every file that changed**, what changed inside it, and why.
The updated files are complete, so you don't have to edit line by line: copy them over your repo (see *How to apply* at the bottom).

Legend: 🆕 new file · ✏️ rewritten · 🗑️ deleted · 🐞 fixes a bug that produced wrong numbers or failed runs

---

## 0. Big picture — what's different after this change

| Area | Before | After |
|---|---|---|
| Where raw files live | `/Workspace/Users/<email>/insurance_claims/raw/...` (a code area) | Unity Catalog volume `/Volumes/<catalog>/raw/landing/...` (same sub-folders). On Azure this volume points at your ADLS `landing` container. |
| Environment | Email + paths hard-coded in 8 notebooks | One `env` widget (`dev` = Free Edition, `prod` = Azure). Everything else is derived in `configs/common/common_config.py`. |
| Bronze load | `dbutils.fs.head()` (cuts files at 10 MB), hand-written CSV split, full overwrite, `batch_001` forever | **Auto Loader**: only new files, proper JSON/CSV parsing, append-only, real file path per row, batch id = job run id |
| Silver | No dedup, `_v2` table names, a technical notebook dropping a business table | Dedup (latest version per key), clean names, safe casts, postcodes as 4-char strings |
| Weather | Joined events→claims, so each event was repeated once per claim | Weather stays 1 row per event; each claim gets `weather_claim_match` / `weather_event_count` / `is_severe_weather` |
| Gold | Natural keys only, facts repeat text columns, `dim_date` only had dates found in data, `dim_weather` failed on re-run | Surrogate keys (`*_sk`), date keys → continuous `dim_date` (with `-1` = unknown), measures rolled up into `fact_claims`, PII hashed |
| Data quality | Printed results only; fraud check used the wrong scale | Checks fail the run (`error`) or report (`warn`); scale and boolean checks fixed; gold↔silver reconciliation |
| Orchestration | Run notebooks by hand in order | One job with 32 tasks in dependency waves, defined in code (`databricks.yml`), deployable to Free Edition **and** Azure |

**Table renames** (update any saved queries or dashboards):

| Old | New |
|---|---|
| `silver.slv_claims_header_business_v2` | `silver.slv_claims_header_business` |
| `silver.slv_claims_line_business_v2` | `silver.slv_claims_line_business` |
| `silver.slv_payments_business_v2` | `silver.slv_payments_business` |
| `silver.slv_policy_business_v2` | `silver.slv_policy_business` |
| `silver.slv_weather_business_v2` | `silver.slv_weather_business` |
| `silver.slv_fraud_scores_business_v2` | `silver.slv_fraud_scores_business` |
| `gold.dim_fraud` | removed; its columns are in `gold.fact_claims` and `gold.fact_fraud_scores` |

---

## 1. Config and setup

### ✏️ `configs/common/common_config.py`
Every notebook `%run`s this first. It used to hold only `BRONZE_DB` and `SILVER_DB`. It now contains:

- **`env` widget** → `CATALOG` (`workspace` on Free Edition, `insurance` on Azure) and runs `USE CATALOG`. The existing `bronze.x` / `silver.y` / `gold.z` names then work unchanged in both environments, including in `%sql` cells.
- **Paths**: `LANDING_PATH = /Volumes/<catalog>/raw/landing`, `CHECKPOINT_PATH = /Volumes/<catalog>/raw/checkpoints`.
- **`BATCH_ID`**: comes from the job (`{{job.run_id}}`); a manual run gets `manual_<timestamp>`.
- **Business thresholds** that were hard-coded in notebooks: `LARGE_CLAIM_THRESHOLD`, `LINE_HIGH/MEDIUM_SEVERITY`, `FRAUD_HIGH/MEDIUM_THRESHOLD`, `FRAUD_SCORE_MAX`, `SEVERE_WEATHER_LEVELS`.
- **Helpers** (these replace code that was copy-pasted everywhere):
  - `drop_metadata(df)`: replaces the 9-column `.drop(...)` list that appeared about 12 times.
  - `trim_all_strings(df)`: trims every string column and turns `""` into NULL.
  - `to_decimal / to_double / to_date_safe / to_ts_safe`: `try_cast`, so one bad value becomes NULL instead of failing the job. Serverless runs with ANSI mode on, where a plain cast of bad data throws an error.
  - `to_postcode(col)`: 4-character string with leading zeros kept. 🐞 NT postcodes (08xx) were stored as INT and lost the zero.
  - `dedupe_latest(df, keys)`: keeps the newest row per key, by `updated_ts`, then by `bronze_ingest_ts`.
  - `write_table(df, schema, table)`: overwrite + `overwriteSchema`, then prints the row count.
  - `ingest_to_bronze(...)`: the Auto Loader function all 8 bronze notebooks call (details in section 2).
  - `DQ` class: collects check results, prints PASS/WARN/FAIL, and `raise_if_failed()` fails the run if any `error`-level check failed.

> ⚠️ **Check one thing in your data:** `FRAUD_SCORE_MAX = 100`. Your silver logic treats a score of 60 or more as high risk, which implies a 0–100 scale. The old gold check expected 0–1. Open one `fraud_scores*.json`. If the scores look like `0.73`, set `FRAUD_SCORE_MAX = 1`, `FRAUD_HIGH_THRESHOLD = 0.6` and `FRAUD_MEDIUM_THRESHOLD = 0.3`.

### 🆕 `setup/00_setup_environment.py`
A one-time notebook, safe to re-run.
- **dev**: creates schemas `raw, bronze, silver, gold` and volumes `raw.landing` + `raw.checkpoints` in the `workspace` catalog. Set the `workspace_raw_path` widget to your old raw folder (`/Workspace/Users/<you>/insurance_claims/raw`) and it copies `cloud_claims/`, `onprem_policy/`, `payments/`, `reference/`, `api_enrichment/` into the volume.
- **prod**: creates the two external locations (ADLS `lakehouse` and `landing` containers), the `insurance` catalog stored on your ADLS account, the schemas, and an **external** volume over `landing`.
- `reset = yes`: drops bronze/silver/gold and the Auto Loader checkpoints. **Run it once with `reset = yes` when you switch to the new code.** The old bronze tables have typed columns and the new ones are all-string, so appending to the old tables would fail.

### 🆕 `databricks.yml` + `resources/insurance_claims_job.yml`
A **Databricks Asset Bundle**. It defines the whole pipeline as one job with 32 tasks. The tasks run in **waves of at most 4**, because Free Edition allows at most 5 job tasks running at the same time:

```
wave 1  bronze 01-04        wave 2  bronze 05-08
wave 3  silver tech 11-14   wave 4  silver tech 15-18
wave 5  silver biz 21-22    wave 6  silver biz 23-26
wave 7  silver_validation
wave 8  gold dims 31-34     wave 9  gold facts 41-44    wave 10  gold_dq
```

- Target **dev** runs on serverless (Free Edition). Target **prod** runs on one single-node `Standard_DS3_v2` job cluster that shuts down when the run ends.
- **You must change** the two `host:` lines in `databricks.yml` to your workspace URLs.
- Job parameters: `env` (set per target) and `batch_id = {{job.run_id}}`. Failure emails go to you.

### 🆕 `.github/workflows/deploy.yml`
GitHub Actions. Every pull request runs `bundle validate`. The **Run workflow** button deploys to Azure, and can optionally start the pipeline. It needs the repo secrets `DATABRICKS_HOST` and `DATABRICKS_TOKEN`; that host must match the prod `host` in `databricks.yml`.

---

## 2. Bronze — all 8 notebooks ✏️ (same pattern)

| Notebook | Reads (inside `LANDING_PATH`) | Writes |
|---|---|---|
| `bronze/policy/01_bronze_ingest_policy.py` | `onprem_policy/policy_master_*` (CSV) | `bronze.brz_policy_master_raw` |
| `bronze/claims/02_bronze_ingest_claims_header.py` | `cloud_claims/claims_header*` (JSON array) | `bronze.brz_claim_header_raw` |
| `bronze/claims/03_bronze_ingest_claims_line.py` | `cloud_claims/claims_line*` (JSON array) | `bronze.brz_claim_line_raw` |
| `bronze/reference/04_bronze_ingest_reference_postcode.py` | `reference/postcode_region_mapping*` (CSV) | `bronze.brz_postcode_region_raw` |
| `bronze/reference/05_bronze_ingest_reference_product.py` | `reference/product_mapping*` (CSV) | `bronze.brz_product_mapping_raw` |
| `bronze/payments/06_bronze_ingest_payments.py` | `payments/payments_*` (CSV) | `bronze.brz_payments_raw` |
| `bronze/enrichment/07_bronze_ingest_enrichment_weather.py` | `api_enrichment/weather*` (JSON array) | `bronze.brz_weather_raw` |
| `bronze/enrichment/08_bronze_ingest_enrichment_fraud_.py` | `api_enrichment/fraud_scores*` (JSON array) | `bronze.brz_fraud_scores_raw` |

Each notebook is now 3 cells: `%run` the config, call `ingest_to_bronze(...)`, preview.

What `ingest_to_bronze` fixes:
- 🐞 `dbutils.fs.head(path, 10000000)` returned only the first 10 MB of a file, so a larger file was silently cut off or failed with a JSON error. Auto Loader reads whole files.
- 🐞 The CSV was split with `line.split(",")`, which breaks on any quoted value containing a comma (for example `"Unit 2, 5 Main St"`). Spark's CSV reader handles quotes; the BOM in the header is still stripped.
- 🐞 In notebooks 02 and 03 you added `record['source_file_name']`, but that field wasn't in the `StructType`, so Spark dropped it. `bronze_source_file` was `""` everywhere. Now `bronze_source_file = _metadata.file_path` holds the real path for every row.
- **Incremental and append-only**, as the bronze README promises. The checkpoint remembers which files were already loaded, so re-running the job doesn't duplicate data; new files are picked up next run.
- **All columns land as STRING** (`inferColumnTypes=false`). Bronze stays raw, and silver does the typing. This avoids failures like "IntegerType can not accept 1500.75". The exception is `flags` in fraud, kept as `ARRAY<STRING>` via a schema hint.
- New columns in a source file are added automatically (`mergeSchema`). When that happens, the first run stops with a message telling you so; just re-run it.

---

## 3. Silver technical — 11 to 18 ✏️

All eight now use `trim_all_strings` → safe casts → `dedupe_latest` → `write_table`.

| File | Specific changes |
|---|---|
| `silver/technical/claim_header/11_silver_claim_header_technical.py` | 🐞 Removed `DROP TABLE silver.slv_claims_header_business_v2`: re-running 11 on its own deleted a downstream table. Removed the duplicated DROP of its own table. `loss_postcode` is now a string; `estimated_loss_amount` is `DECIMAL(18,2)`. Dedup on `claim_id`: a re-sent claim keeps its latest version. |
| `silver/technical/claim_line/12_silver_claim_line_technical.py` | Money columns as `DECIMAL`. Dedup on `claim_line_id`. |
| `silver/technical/claim_policy/13_silver_policy_master_technical.py` | 🐞 `premium_amount` was never cast, so it stayed a string and the gold `premium_amount < 0` check compared text. `risk_postcode` as a string. Dedup on `policy_id`. |
| `silver/technical/payments/14_silver_payment_technical.py` | `ingestion_ts` now comes from the source column, as in every other table (it used to be overwritten with `bronze_ingest_ts` here only). Dedup on `payment_id`. |
| `silver/technical/product_master/15_silver_product_technical.py` | Dedup on `product_code`, keeping the newest file. No longer relies on `created_ts`, which the old bronze notebook invented. |
| `silver/technical/postcode_mapping/16_silver_postcode_technical.py` | 🐞 Postcode is a 4-char string (was INT). Dedup on `postcode + state`. |
| `silver/technical/weather/17_silver_weather_technical.py` | Postcode uses the **same type as claims** (it was string here and INT in claims). Severity is initcapped. Dedup on `event_id`. |
| `silver/technical/fraud/18_silver_fraud_scores_technical.py` | 🐞 Dedup on `claim_id`: a claim re-scored in a later file used to appear twice in `fact_claims`. |

---

## 4. Silver business — 21 to 26 ✏️

All tables lose the `_v2` suffix, and the stray `DROP TABLE silver.slv_*_business` lines are gone.

| File | Changes |
|---|---|
| `silver/business/policy/21_silver_policy_business.py` | Takes only `coverage_type` and `risk_category` from product (you were already dropping `lob` and `product_name`). Uses `drop_metadata`. |
| `silver/business/claims_header/22_silver_claims_header_business.py` | Joins only the policy columns a claim needs; the full policy is in `dim_policy`. **New weather match**: weather is first aggregated to one row per `postcode + day`, then left-joined to claims on `loss_postcode = postcode AND incident_date = event_date`. Adds `weather_claim_match`, `weather_event_count`, `weather_events` and `is_severe_weather`. A claim can never be duplicated by this join. `5000` comes from `LARGE_CLAIM_THRESHOLD`. |
| `silver/business/claims_line/23_silver_claims_line_business.py` | Joins just 5 header columns instead of the whole wide header on every line. Thresholds come from config. |
| `silver/business/payments/24_silver_payments_business.py` | Joins only `policy_id, incident_date, reported_date` from claims. |
| `silver/business/weather/25_silver_weather_business.py` | 🐞 **Main grain fix.** It used to start from weather and left-join claims, so an event with 5 claims became 5 rows, and `dim_weather` / `fact_weather_events` had duplicate `event_id`s. It now stays **one row per event** and adds `matched_claim_count` (claims lodged for that postcode on that day). |
| `silver/business/fraud_scores/26_silver_fraud_scores_business.py` | Thresholds from config. Reads the claims technical table, so it no longer has to wait for notebook 22. |

### ✏️ `silver/validation/silver_validation_all.py`
- **error** (fails the run): duplicate keys in every business table, and business row count ≠ technical row count.
- **warn** (reported only): nulls in mandatory columns, incident after reported date, fraud score out of range, unknown weather severity, negative payments, and orphan keys (claims without a policy, payments without a claim, lines without a claim).
- Informational profiles are still printed.
- When your data is clean, change `level="warn"` to `level="error"` on the checks you want to enforce.

---

## 5. Gold ✏️

**Surrogate keys** are `xxhash64(natural_key)`. They're deterministic, so the same claim gets the same key on every rebuild and facts and dims always line up. **Date keys** are `yyyyMMdd` integers, with `-1` when the date is missing.

| File | Changes |
|---|---|
| `gold/dim/31_gold_dim_date.py` | Continuous calendar from 1 Jan of the earliest year to 31 Dec of the latest year, capped at today + 10 years so a `9999-12-31` policy end date can't explode it. Adds `month_name`, `au_financial_year` (July to June) and the unknown row `-1`. It used to contain only dates present in the data. |
| `gold/dim/32_gold_dim_claim.py` | `claim_sk`. Policy columns removed (they're reachable through `fact_claims.policy_sk`). **PII**: `claimant_name` and `claimant_contact` removed; `claimant_key` = SHA-256 of the name, so repeat claimants can still be found. Adds `loss_suburb`, `loss_region`, `loss_risk_zone`. |
| `gold/dim/33_gold_dim_policy.py` | `policy_sk`. `customer_id` and `agent_id` moved here from dim_claim. Street address lines dropped (city, state and postcode kept). `is_active` renamed `is_active_as_of_load`, because it depends on the run date. |
| `gold/dim/34_gold_dim_weather.py` | 🐞 `CREATE TABLE` → `CREATE OR REPLACE TABLE` (the second run always failed). One row per event, `weather_sk`. |
| `gold/dim/35_gold_dim_fraud.py` | 🗑️ **Delete this file.** A fraud score is a measurement of a claim, not a descriptive dimension; its columns are in `fact_claims` and `fact_fraud_scores`. |
| `gold/fact/41_gold_fact_claims.py` | Grain: 1 row per claim. Keys `claim_sk`, `policy_sk`, `incident_date_key`, `reported_date_key`. **New measures** rolled up from lines and payments: `line_count`, `total_claimed_amount`, `total_approved_amount`, `total_deductible_amount`, `payment_count`, `total_paid_amount`. Weather flags come from the claim itself, so the weather join can't fan out. Fraud columns with defaults (`Not scored`, `FALSE`). |
| `gold/fact/42_gold_fact_payments.py` | Keys and `payment_date_key`, plus payment measures and flags. Claim descriptions removed (they're in `dim_claim`). |
| `gold/fact/43_gold_fact_weather_events.py` | One row per event: measurements plus `matched_claim_count`. |
| `gold/fact/44_gold_fact_fraud_scores.py` | Keys plus fraud measures; `risk_category` renamed `fraud_risk_category` so it isn't confused with the product's risk category. |

### ✏️ `gold/validation/gold_data_quality_checks.py`
- 🐞 `fraud_score` range was checked against `0..1` while silver uses `0..100`, so every scored claim failed. It now uses `FRAUD_SCORE_MAX`.
- 🐞 `fraud_flag NOT IN ('Y','N')` and `is_severe_weather NOT IN ('Y','N')` checked booleans against text. They only passed because Spark casts `'Y'` to TRUE. They're replaced with **consistency** checks: is the flag true exactly when score ≥ threshold, and when severity is Severe or Extreme?
- New: `dim_date` gap check, FK checks for every `*_sk` and `*_date_key`, and gold↔silver reconciliation (claim count and total paid).

---

## 6. Docs
- ✏️ `README.md`: updated architecture (Free Edition + Azure), how to run, table list. Community Edition references removed, and the pointer to the missing `configs/paths.py` is gone.
- ✏️ `bronze/README.md`, `silver/README.md`, `gold/README.md`: output locations and new table names.
- 🆕 `docs/AZURE_SETUP.md`: the Azure build, click by click, with costs.
- 🆕 `docs/CHANGES.md`: this file.

---

## How to apply (your code lives in a Databricks Git folder, pushed to GitHub)

**Option A — through GitHub (recommended, keeps history clean)**
1. On github.com open your repo → branch dropdown → type `azure-refactor` → **Create branch**.
2. On that branch: **Add file → Upload files**, and drag in the *contents* of the updated `insurance_claims` folder (not the folder itself). Commit.
3. On that branch, delete `gold/dim/35_gold_dim_fraud.py` (open it → ⋯ → Delete file).
4. In Databricks, open your Git folder → click the branch name → switch to `azure-refactor` → **Pull**.
5. When you're happy, open a pull request `azure-refactor` → `main` and merge.

**Option B — inside Databricks.** In your Git folder, create the branch `azure-refactor` from the Git dialog. Open each notebook listed above, replace its contents with the new file, create the new files (`setup/…`, `databricks.yml`, `resources/…`, `docs/…`), delete `35_gold_dim_fraud`, then commit and push from the Git dialog.

**Then run, in this order (dev / Free Edition):**
1. `setup/00_setup_environment`: `env=dev`, `reset=yes` (first time only), `workspace_raw_path=/Workspace/Users/<you>/insurance_claims/raw`.
2. Either run the bronze → silver → gold notebooks in number order, **or** create the job with `databricks bundle deploy -t dev` and run it (see README).
3. Look at the WARN lines in the two validation notebooks: they show what's wrong in the sample data.
