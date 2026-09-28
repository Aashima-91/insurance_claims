# **Silver Layer — Cleansed & Conformed Zone**
### **Purpose**
The Silver layer standardizes, cleans, and enriches Bronze data.
It prepares data for business consumption and Gold modeling.

### **Subfolders**

silver/
>     technical/
>     business/
>     validation/

### **Technical Layer**
Handles:

- Type casting
- Deduplication
- Null standardization
- Schema enforcement
- Column renaming
- Surrogate key generation (optional)

### **Business Layer**
Handles:

- Business logic
- Joins across domains
- Enrichments (weather, fraud, postcode)
- Derived attributes
- Conformed dimensions
- Validation Layer
- Contains DQ checks for Silver outputs:
    - Null checks
    - Duplicate checks
    - Schema drift checks
    - Referential integrity checks

### **Naming Convention**

<sequence>_silver_<technical|business>_<domain>_<entity>

### **Output**

Unity Catalog Delta tables `<catalog>.silver.*`:

| Technical (typed, trimmed, deduplicated) | Business (joined + derived) |
|---|---|
| `slv_claims_header_technical` | `slv_claims_header_business` (incl. postcode region + weather match) |
| `slv_claims_line_technical` | `slv_claims_line_business` |
| `slv_policy_technical` | `slv_policy_business` |
| `slv_payments_technical` | `slv_payments_business` |
| `slv_product_mapping_technical` | — |
| `slv_postcode_region_technical` | — |
| `slv_weather_technical` | `slv_weather_business` (1 row per event) |
| `slv_fraud_scores_technical` | `slv_fraud_scores_business` |

`validation/silver_validation_all` fails the run on `error` checks (grain, row counts) and reports `warn` checks.
