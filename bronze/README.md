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

Delta tables stored under:
/mnt/.../bronze/<domain>/<entity>    