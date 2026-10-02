# **Bronze Layer — Raw Ingestion Zone**
## **Purpose**

The Bronze layer stores raw ingested data exactly as received from source systems.
It acts as the immutable landing zone for all upstream feeds.

### **Key Principles**

- No business logic
- Minimal transformations (type casting, column renaming only if required)
- Schema-on-read
- Append-only
- One folder per domain

### **Folder Structure**

bronze/
>     claims/
>     payments/
>     policy/
>     reference/
>     enrichment/

### **Naming Convention**

<sequence>_bronze_ingest_<domain>_<entity>

Examples:

02_bronze_ingest_claims_header
07_bronze_ingest_enrichment_weather

### **Responsibilities**

- Ingest raw files (CSV, JSON, Parquet)
- Apply minimal cleaning (trim, null handling)
- Add metadata columns:
    - ingest_ts
    - source_file
    - source_system

### **Output**

Unity Catalog Delta tables `<catalog>.bronze.brz_*_raw` (catalog `workspace` on Free Edition, `insurance` on Azure,
where the data physically sits in the ADLS `lakehouse` container).

Input files are read from the volume `/Volumes/<catalog>/raw/landing/<source folder>/` with **Auto Loader**
(`ingest_to_bronze()` in `configs/common/common_config.py`): only new files are read, all columns land as STRING, and
each row carries `bronze_source_file`, `bronze_ingest_ts` and `bronze_ingest_batch_id` (the job run id).
