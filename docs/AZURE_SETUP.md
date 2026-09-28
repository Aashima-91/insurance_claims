# Azure layer — build guide and cost plan

Target architecture:

```
                      Azure subscription (Pay-As-You-Go, using your trial credit) — region: Australia East
 ┌───────────────────────────────────────────── rg-insurance-claims ─────────────────────────────────────────────┐
 │                                                                                                               │
 │  ADLS Gen2  st<insclaims>                                                                                     │
 │   ├─ source/     ← "source systems" drop files here (you upload, or Azure SQL / REST as stretch goals)        │
 │   ├─ landing/    ← ADF copies here: landing/<folder>/yyyy/MM/dd/…  = UC external volume insurance.raw.landing │
 │   └─ lakehouse/  ← Unity Catalog managed storage for catalog `insurance` (bronze / silver / gold Delta)       │
 │                                                                                                               │
 │  Data Factory  adf-<insclaims>                                                                                │
 │   pl_ingest_insurance:  ForEach source folder → Copy (source → landing, date folders)                         │
 │                         → Databricks **Job** activity → job "insurance-claims-pipeline-prod"                  │
 │                                                                                                               │
 │  Access Connector for Azure Databricks (managed identity) ──Storage Blob Data Contributor──► ADLS              │
 │  Azure Databricks workspace (Premium, Unity Catalog)                                                          │
 │   job: 8 bronze (Auto Loader) → 8 silver tech → 6 silver biz → validation → 4 dims → 4 facts → gold DQ        │
 │  Key Vault kv-<insclaims> (optional: SQL password / API keys for ADF)                                         │
 └───────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
      GitHub repo ── Actions (databricks bundle deploy -t prod) ──► workspace        ADF Git integration ──► /adf
      Power BI Desktop ──► Databricks (cluster or small SQL warehouse) ──► gold.*
```

Everything goes in **one resource group**, so tearing down on the last day is a single delete.

---

## 0. Before you create anything (15 min) — read this first

### 0.1 You must upgrade the trial to Pay-As-You-Go
Microsoft states that Azure Databricks **clusters cannot start on a Free Trial subscription**: the vCPU quota is 0 and trial subscriptions can't request more. The fix is to **upgrade to Pay-As-You-Go**.
When you upgrade, **you keep your remaining credit** until the original 30-day end date, and usage keeps drawing from the credit first.
**After that date, anything still running is billed to your card.** That is why steps 0.2 and 11 matter.

Portal → **Subscriptions** → your subscription → **Upgrade** (or the banner at the top of the portal). Note the **credit end date** shown under Cost Management → Credits.

### 0.2 Budget alert (5 min)
Portal → **Cost Management → Budgets → + Add**: scope = your subscription, amount **100** (currency as shown), alerts at 50 %, 80 % and 100 % **Actual**, plus 100 % **Forecasted** → your email.
A budget only **alerts**; it doesn't stop resources. Check **Cost Management → Cost analysis** every couple of days. Costs appear with a delay of up to about a day.

### 0.3 vCPU quota
Portal → **Subscriptions → Usage + quotas** → filter **Region = Australia East** (or your workspace's region):
- **Total Regional vCPUs** ≥ 4
- **Standard DSv2 Family vCPUs** ≥ 4 (for `Standard_DS3_v2`)

If either shows 0 after the upgrade, click the pencil icon → request 8. These are usually approved automatically within minutes.

### 0.4 Your existing Azure Databricks workspace
Open it in the portal and check:
- **Pricing tier** must be **Premium** or **Trial (Premium – 14 days free DBUs)**. Unity Catalog needs Premium.
  The free-DBU trial clock started when you created the workspace, so note the date.
- **Region**: create *everything else* in the same region to avoid data-transfer charges.
- **Resource group**: fine to keep where it is; you'll delete it separately at the end.
- In the workspace, open **Catalog**. If you see a catalog named after your workspace, Unity Catalog is already enabled (the default for new workspaces).

> **Hidden cost:** new workspaces use *secure cluster connectivity* by default. Azure Databricks then **automatically creates a NAT gateway** in the workspace's managed resource group, and Microsoft notes it "incurs additional cost". It bills **every hour the workspace exists, even with no clusters running**, roughly $1 a day. This is the main reason to delete the workspace as soon as you're done.

---

## 1. Resource group (2 min)
Portal → **Resource groups → Create** → name `rg-insurance-claims`, region **Australia East** → tag `project = insurance-claims`.

## 2. Storage account — ADLS Gen2 (5 min)
**Storage accounts → Create**
- Resource group `rg-insurance-claims`; name e.g. `stinsclaims<your initials>` (3–24 lowercase letters/numbers, globally unique)
- Region: same as the workspace. Performance **Standard**, Redundancy **LRS** (cheapest)
- **Advanced tab → tick "Enable hierarchical namespace"** (this is what makes it ADLS Gen2; it can't be changed later)
- Create, then **Data storage → Containers → + Container** three times: `source`, `landing`, `lakehouse`
- Give yourself data access: storage account → **Access control (IAM) → Add role assignment → Storage Blob Data Contributor → User → you**. The Owner role alone doesn't let you browse data.

## 3. Access Connector for Azure Databricks (5 min)
This is the managed identity Unity Catalog uses to reach ADLS, so no keys or secrets are needed.
- Search **Access Connector for Azure Databricks → Create** → RG `rg-insurance-claims`, same region, name `ac-insurance-claims` → Create.
- Open it → **Overview** → copy the **Resource ID** (`/subscriptions/…/accessConnectors/ac-insurance-claims`).
- Storage account → **IAM → Add role assignment → Storage Blob Data Contributor → Managed identity → Access Connector for Azure Databricks → ac-insurance-claims**.

## 4. Compute for development (5 min)
Databricks workspace → **Compute → Create compute**:
- **Single node**, access mode **Dedicated (single user)**, runtime **15.4 LTS**, node **Standard_DS3_v2**
- **Terminate after 20 minutes of inactivity** ← the most important setting on this page
- Tag `project = insurance-claims`

## 5. Unity Catalog storage credential (5 min)
Workspace → **Catalog → External data → Credentials → Create credential**:
- Type **Azure Managed Identity**, name `insurance_adls_cred`, Access connector ID = the Resource ID from step 3 → Create.

If **Create** is greyed out, you're not a metastore admin. Sign in to `https://accounts.azuredatabricks.net` with the same email; as the tenant creator you're an account admin. Go to **Catalog → your metastore → Metastore admin → set yourself**, then retry.

## 6. Bring the code into the Azure workspace and run setup (10 min)
1. Workspace → **Workspace → Create → Git folder** → paste your GitHub repo URL → branch `azure-refactor`. To push from Azure too, add a GitHub token under *User settings → Linked accounts*.
2. Open `setup/00_setup_environment`, attach your cluster, set the widgets: `env = prod`, `storage_account = stinsclaims…`, `storage_credential = insurance_adls_cred`, `reset = no` → **Run all**.
   It creates the external locations, catalog `insurance` (stored in the `lakehouse` container), schemas `raw/bronze/silver/gold`, the external volume `insurance.raw.landing` (the `landing` container) and `insurance.raw.checkpoints`.
3. **First test without ADF:** portal → storage account → **Storage browser → landing** → create folders `cloud_claims`, `onprem_policy`, `payments`, `reference`, `api_enrichment` → upload your raw files into them.
4. Run notebooks 01 → 44 with widget `env = prod`, or deploy the job (step 7). Check the `insurance` catalog in Catalog Explorer.

## 7. Deploy the job (10 min)
On your laptop, [install the Databricks CLI](https://docs.databricks.com/dev-tools/cli/install.html), then:
```bash
databricks auth login --host https://adb-<id>.<n>.azuredatabricks.net     # opens a browser
# edit databricks.yml: put that URL in targets.prod.workspace.host
databricks bundle validate -t prod
databricks bundle deploy   -t prod
databricks bundle run      -t prod insurance_claims_pipeline
```
The job `insurance-claims-pipeline-prod` appears under **Jobs & Pipelines**. It runs on one single-node DS3_v2 job cluster that stops when the run ends.
**Copy its Job ID** (Job page → right-hand panel); ADF needs it.

Optional CI/CD: add repo secrets `DATABRICKS_HOST` and `DATABRICKS_TOKEN` (workspace → User settings → Developer → Access tokens, lifetime 30 days). GitHub → **Actions → databricks-bundle → Run workflow** then deploys `main`.

## 8. Azure Data Factory (45–60 min)
### 8.1 Create
**Data factories → Create** → RG `rg-insurance-claims`, same region, name `adf-insclaims-<initials>`, version V2.
On the **Git configuration** tab tick **Configure Git later**; you'll connect it in 8.6.

### 8.2 Permissions for ADF's managed identity
- Storage account → IAM → **Storage Blob Data Contributor** → Managed identity → **Data factory** → yours.
- Databricks workspace (the Azure resource) → IAM → **Contributor** → Managed identity → your data factory. This lets ADF call the workspace with its identity, with no tokens stored.

### 8.3 Linked services (ADF Studio → Manage → Linked services → + New)
- **Azure Data Lake Storage Gen2** → name `ls_adls` → Authentication **System-assigned managed identity** → pick your storage account → Test connection.
- **Azure Databricks** (under Compute) → name `ls_databricks` → pick your workspace → Authentication **Managed service identity** → choose the **Serverless** cluster option (the Job activity uses the job's own cluster, so ADF doesn't create one).

### 8.4 Datasets (Author → + → Dataset → ADLS Gen2 → **Binary**)
Binary means files are copied byte-for-byte, so any format works.
- `ds_source_folder`: linked service `ls_adls`; add parameter `folder`; File path → container `source`, directory `@dataset().folder`, file empty.
- `ds_landing_folder`: parameters `folder`; container `landing`, directory
  `@concat(dataset().folder, '/', formatDateTime(utcNow(), 'yyyy'), '/', formatDateTime(utcNow(), 'MM'), '/', formatDateTime(utcNow(), 'dd'))`

### 8.5 Pipeline `pl_ingest_insurance`
1. Pipeline **Parameters**: `folders` (Array), default `["cloud_claims","onprem_policy","payments","reference","api_enrichment"]`.
2. Drag in **ForEach** → Settings → Items `@pipeline().parameters.folders`, leave Sequential unticked, batch count 5.
3. Inside the ForEach, add a **Copy data** activity:
   - Source: `ds_source_folder`, folder = `@item()`, *Wildcard file path* `*`, tick **Recursively**, and tick **Delete files after completion** (so each file is ingested once, as if moved).
   - Sink: `ds_landing_folder`, folder = `@item()`, Copy behaviour **Preserve hierarchy**.
4. After the ForEach (green "on success" arrow), add **Job** (search "Job" in Activities; it's under *Databricks*) → linked service `ls_databricks` → Settings → **Job ID** = from step 7. No parameters are needed: the job defaults to `env=prod` and `batch_id={{job.run_id}}`.
5. **Validate → Debug**. First upload your raw files into the `source` container (same 5 sub-folders), then watch the Copy runs and the Databricks job run.
6. **Publish all**.

Auto Loader finds files in the new `yyyy/MM/dd` sub-folders automatically (it lists recursively, and the file-name filters still apply). Files it has already processed are never loaded twice.

### 8.6 Trigger + Git (10 min)
- **Add trigger → New/Edit → Schedule** → daily 06:00, time zone *AUS Eastern Standard Time*. For the demo, run it a couple of times, then **stop the trigger** so it doesn't cost money while you're not looking.
- **Manage → Git configuration → Configure** → GitHub → your repo → collaboration branch `main`, **root folder `/adf`**. The ADF JSON (pipelines, datasets, linked services) then lives in your repo next to the Databricks code, which is good for the portfolio and survives the trial.

### 8.7 Stretch goals (only if time and credit allow)
- **"On-prem" policy from Azure SQL Database**: create an Azure SQL DB with the *free offer* (serverless, auto-pause). Load `policy_master_*.csv` into a table, store the SQL password in **Key Vault**, and point an ADF linked service at Key Vault. Then add a Copy activity: SQL table → `landing/onprem_policy/…` as **DelimitedText** with header, file name `policy_master_<date>.csv`.
- **Real weather API**: ADF **REST** linked service to a free weather API → JSON into `landing/api_enrichment/`. Its fields differ from your sample weather feed, so give it its own bronze table and file prefix rather than mixing it into `brz_weather_raw`.

## 9. Power BI (optional, 30 min)
Power BI Desktop (free) → **Get data → Azure Databricks** → Server hostname and HTTP path:
- **Cheapest:** your single-node cluster (Compute → your cluster → Advanced → JDBC/ODBC). It auto-stops after 20 minutes.
- Or a **SQL warehouse**: size **2X-Small**, **Auto stop 5 minutes**, scaling min = max = 1.

Import mode: `gold.fact_*` + `gold.dim_*`. Relationships: `fact_claims.claim_sk → dim_claim.claim_sk`, `policy_sk → dim_policy`, `incident_date_key → dim_date.date_key`.

## 10. Portfolio evidence (before tear-down!)
Screenshots of: resource group overview, ADF pipeline and a successful monitor run, the Databricks job DAG with a green run, Catalog Explorer lineage for `gold.fact_claims`, the DQ output, and the Power BI page. Put them in `docs/images/` and link them from the README.

## 11. Tear down (last day, at least 2 days before the credit end date)
1. Stop the ADF trigger.
2. Delete the **Databricks workspace** (this also deletes its managed resource group, the NAT gateway and the VMs).
3. Delete **`rg-insurance-claims`**.
4. After a day, Cost Management → Cost analysis should show nothing new. If you won't use Azure again, **Subscriptions → Cancel subscription**.

The code, ADF JSON and screenshots stay in GitHub, and the dev target keeps running on Free Edition.

---

## Cost estimate for ~20 days

Prices are **approximate USD list prices** (Premium tier; published DBU rates are roughly Jobs $0.30, All-purpose $0.55, Serverless SQL $0.70 per DBU). `Standard_DS3_v2` = 0.75 DBU/hour, plus the VM itself, about $0.30–0.40/hour in Australian regions. Australian regions are usually a bit more expensive than US ones. **Before you commit, check the exact numbers for your region in the [Azure pricing calculator](https://azure.microsoft.com/en-us/pricing/calculator/)**, and check whether your credit is in USD or AUD.

| Item | Usage assumed | ≈ Cost |
|---|---|---|
| NAT gateway + its public IP (created automatically by the workspace) | 24/7 while the workspace exists, 20 days | $20–30 |
| Interactive single-node cluster (DS3_v2) | 25 hours of development. DBUs are free while the 14-day Premium trial lasts, so you pay the VM only | $9–20 |
| Pipeline job runs (single-node job cluster) | 20 runs × ~30 min incl. start-up | $5–8 |
| ADF | 20 pipeline runs × 5 copies + 1 job activity, plus debug runs | $3–6 |
| SQL warehouse 2X-Small (optional) | 4 hours of Power BI work | ~$11 (or $0 if you use the cluster) |
| ADLS Gen2, Key Vault, Access Connector | <1 GB of data | <$1 |
| Azure SQL DB free offer (stretch) | within the free monthly allowance | $0 |
| **Total** | | **≈ $40–75** |

That leaves most of a 200 credit untouched **if** you follow the guardrails below.

### What could blow the budget

| Mistake | Cost |
|---|---|
| Interactive cluster left running over a weekend (no auto-terminate) | ~$35–45 |
| SQL warehouse left on for a day (auto-stop off), or a larger size | ~$65 per day at 2X-Small |
| Multi-node or larger clusters ("just to make it faster") | 2–10× the table above |
| Workspace kept after you finish | ~$1 per day for the NAT gateway, even when idle |
| Anything still running after the credit end date | billed to your card |

### Guardrails
1. Auto-terminate **20 min** on every cluster; auto-stop **5 min** on any SQL warehouse.
2. Single node only, and only `Standard_DS3_v2` (or `Standard_D4ds_v5`). The job's cluster is already defined that way in `databricks.yml`.
3. Tag everything `project = insurance-claims` and look at *Cost analysis → group by tag* every 2–3 days.
4. Keep the ADF trigger **stopped** except when demonstrating it.
5. Delete the workspace and resource group before the credit end date (step 11).
