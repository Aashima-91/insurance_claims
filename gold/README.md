# **Gold Layer — Analytics & Reporting Zone**
### **Purpose**
The Gold layer contains star-schema optimized data models for BI, reporting, and ML.

### **Subfolders**
gold/
>     dim/
>     fact/
>     validation/

### **Dimensions**
- Slowly changing or static attributes
- One row per business entity
- Surrogate keys recommended (*_sk)

### **Facts**
- Transactional or event-level data
- Defined grain per table
- Foreign keys reference DIM surrogate keys

### **Validation**
- Grain checks
- FK integrity
- Business rule checks
- DQ logging

### **Naming Convention**

dim_<entity>
fact_<process>

### **Output**

Unity Catalog Delta tables `<catalog>.gold.*`:

| Table | Grain | Keys |
|---|---|---|
| `dim_date` | 1 row per calendar day (+ `-1` unknown) | `date_key` (yyyyMMdd) |
| `dim_claim` | 1 row per claim (claimant name hashed) | `claim_sk` |
| `dim_policy` | 1 row per policy | `policy_sk` |
| `dim_weather` | 1 row per weather event | `weather_sk` |
| `fact_claims` | 1 row per claim | `claim_sk`, `policy_sk`, `incident_date_key`, `reported_date_key` |
| `fact_payments` | 1 row per payment | `claim_sk`, `policy_sk`, `payment_date_key` |
| `fact_weather_events` | 1 row per weather event | `weather_sk`, `event_date_key` |
| `fact_fraud_scores` | 1 row per scored claim | `claim_sk`, `policy_sk`, `incident_date_key` |

Surrogate keys are `xxhash64(natural key)`, so they are stable across rebuilds.
