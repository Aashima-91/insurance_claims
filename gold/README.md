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
Delta tables stored under:
/mnt/.../gold/dim/  
/mnt/.../gold/fact/