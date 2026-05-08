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

Delta tables stored under:
/mnt/.../silver/<domain>/<entity>