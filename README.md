# Insurance Claims Analytics Lakehouse  
A production‑grade, end‑to‑end **Medallion Architecture** built on Databricks to process, enrich, validate, and model insurance claims data for analytics, reporting, and fraud detection.

---

## 🚀 **1. Project Overview**
This project demonstrates a full **enterprise data engineering pipeline** using Databricks:

- Ingest raw insurance data (claims, policy, payments, reference)
- Enrich with external datasets (weather, fraud signals)
- Apply technical + business transformations
- Build dimensional models and fact tables
- Run data quality checks across layers
- Prepare analytics‑ready datasets for BI and ML

It is designed to reflect **real‑world insurance data platforms** used in BFSI organizations.

---

## 🧱 **2. Architecture Summary**
This project follows the **Medallion Architecture**:

- **Bronze** → Raw ingestion  
- **Silver** → Cleansed, conformed, enriched  
- **Gold** → Dimensional models + fact tables  

Each layer is modular, domain‑driven, and production‑ready.

If you want to explore any layer in detail:  
- Bronze Layer  
- Silver Layer  
- Gold Layer

---

## 📊 **3. Business Domains Covered**
This Lakehouse processes multiple insurance domains:

- **Claims** — header + line level  
- **Policy** — customer, coverage, premiums  
- **Payments** — settlement transactions  
- **Reference Data** — postcode, product mapping  
- **Weather Enrichment** — event severity, matching  
- **Fraud Enrichment** — fraud scores, flags  

This makes the project **multi‑domain**, **cross‑functional**, and **interview‑ready**.

---

## 🧠 **4. Key Features**
### ✔ End‑to‑end ETL/ELT pipeline  
From raw ingestion to analytics‑ready Gold tables.

### ✔ Weather + Fraud enrichment  
Realistic external data integration.

### ✔ Clean separation of technical vs business logic  
A hallmark of senior‑level engineering.

### ✔ Data Quality Framework  
Silver + Gold validation notebooks with logging hooks.

### ✔ Modular folder structure  
Easy to extend, maintain, and onboard new engineers.

### ✔ CE‑compatible implementation  
Runs fully on Databricks Community Edition.

---

## 🗂 **5. Folder Structure (High‑Level)**
```
insurance_claims/
   raw/
   bronze/
   silver/
   gold/
   configs/
   
```

Each folder has its own README for deeper documentation.

---

## ⚙️ **6. How to Run the Project**
### Step 1 — Clone or import the repo into Databricks  
Use the Workspace UI to import the project folder.

### Step 2 — Configure paths  
Update `configs/paths.py` with your CE workspace path.

### Step 3 — Run Bronze ingestion  
Execute notebooks in numerical order.

### Step 4 — Run Silver technical → business  
Ensures clean, enriched, conformed data.

### Step 5 — Run Gold modeling  
Build DIMs and FACTs.

### Step 6 — Run DQ validation  
Check data quality across layers.

---

## 📝 **7. Environment Note (Databricks CE)**
Production Databricks uses `/mnt/<container>/...` paths.  
This project runs on **Databricks Community Edition**, so paths use:

```
/Workspace/Users/<email>/insurance_claims/<layer>/<domain>
```

Architecture remains identical.

---

## 🎯 **8. Purpose of This Project**
This project is designed to demonstrate:

- Real‑world data engineering skills  
- BFSI domain understanding  
- Medallion architecture expertise  
- Databricks proficiency  
- End‑to‑end pipeline design  
- Data quality and governance practices  
