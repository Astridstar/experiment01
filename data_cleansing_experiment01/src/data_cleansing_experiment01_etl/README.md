# Data Cleansing Pipeline - Ingestion, Silver & Gold with Dynamic PII Masking

## Overview
This pipeline provides standardized frameworks for:
* **200+ Bronze ingestions** with consistent column naming
* **400+ Silver tables** with data quality checks and transformations
* **Dynamic PII masking** using Unity Catalog column masks (most efficient for scale)

---

## Architecture Overview

```
Bronze (Streaming Tables)
    ↓
Silver (Materialized Views) + Column Masks
    ↓
Users Query Silver Directly (Masking Applied at Query Time)
```

**Key Benefits:**
* ✅ **No Gold layer needed** - Users query silver tables directly
* ✅ **Dynamic per-user masking** - Each user sees different data
* ✅ **Query-time evaluation** - Masking based on current access level
* ✅ **Minimal code** - 4 ALTER statements per table
* ✅ **Centralized logic** - Masking UDFs reused across 400 tables
* ✅ **No storage overhead** - No duplicate tables/views
* ✅ **Optimal performance** - Native Unity Catalog feature

---

## Bronze Layer: Ingestion Templates

[Previous bronze layer content remains the same...]

---

## Silver Layer: Data Quality + Column Masks

### Overview
Silver tables are **materialized views** with:
1. Data quality checks and transformations
2. **Unity Catalog column masks** for dynamic PII protection

### Framework Components

[Previous silver layer components remain the same...]

---

## Dynamic PII Masking with Unity Catalog Column Masks

### Why Column Masks (Recommended for 400 Tables)

| Feature | Column Masks | SQL Views | Materialized Views |
|---------|-------------|-----------|-------------------|
| **Per-user masking** | ✅ Yes | ✅ Yes | ❌ No |
| **Query-time evaluation** | ✅ Yes | ✅ Yes | ❌ No |
| **Code per table** | 4 ALTER statements | ~50 lines SQL | ~100 lines Python |
| **Performance** | ⚡ Native | 🔄 Subquery overhead | ⚡ Pre-computed |
| **Storage overhead** | ✅ None | ✅ None | ❌ Doubles storage |
| **Maintenance** | ✅ Centralized UDFs | ❌ 400 definitions | ❌ 400 definitions |
| **BI tool compatible** | ✅ Yes | ✅ Yes | ✅ Yes |

### Implementation Steps

#### **Step 1: Create Access Control Table**

Run pipeline to create `dev.experiment01.pii_access_grants` table.

File: `transformations/governance/access_control.py`

#### **Step 2: Create Masking UDFs (One-Time)**

Run this SQL script **once** in SQL Editor:

File: `setup/create_masking_udfs.sql`

```sql
-- Creates these functions in dev.experiment01:
-- - mask_email(email STRING)
-- - mask_phone(phone STRING)
-- - mask_nric(nric STRING)
-- - mask_address(address STRING, postal_code STRING)
-- - mask_ssn(ssn STRING)
```

**How it works:**
* Each function checks `pii_access_grants` table for current user
* Returns unmasked, partially masked, or fully masked data
* Evaluated at **query time** for each user

#### **Step 3: Create Silver Tables**

Run pipeline to create silver tables with quality checks.

Example: `transformations/silver/customers_silver.py`

```python
@dp.materialized_view(name="customers_silver")
def customers_silver():
    config = create_standard_customer_config()
    return build_silver_table(bronze_df, config)
```

#### **Step 4: Apply Column Masks to Silver Tables**

Use the provided script to apply masks:

File: `setup/apply_column_masks.py`

**Option A: Generate SQL script**
```python
python setup/apply_column_masks.py > apply_masks.sql
# Review and run the generated SQL
```

**Option B: Apply directly in notebook**
```python
from setup.apply_column_masks import apply_all_masks
apply_all_masks(spark)
```

**Option C: Manual for single table**
```sql
ALTER TABLE dev.experiment01.customers_silver 
  ALTER COLUMN email SET MASK dev.experiment01.mask_email;

ALTER TABLE dev.experiment01.customers_silver 
  ALTER COLUMN phone SET MASK dev.experiment01.mask_phone;

ALTER TABLE dev.experiment01.customers_silver 
  ALTER COLUMN nric SET MASK dev.experiment01.mask_nric;

ALTER TABLE dev.experiment01.customers_silver 
  ALTER COLUMN address SET MASK dev.experiment01.mask_address USING COLUMNS (postal_code);
```

#### **Step 5: Users Query Silver Tables**

```sql
-- Each user sees different data based on their access level
SELECT * FROM dev.experiment01.customers_silver LIMIT 10;
```

### PII Masking Rules

**Access Levels:**
* **full_access**: All PII visible (governance_officers, approved requests)
* **partial_access**: Partial masking (scientists, engineers with approval)
* **masked_only**: Full masking (analysts, managers, default)

**Masking Patterns:**
* **Email**: `user@domain.com` → `u***@domain.com` (partial) or `***@***` (full)
* **Phone**: `+65 12345678` → `+65 ****5678` (partial) or `***` (full)
* **NRIC**: `S1234567D` → `S****567D` (partial) or `***` (full)
* **Address**: Full address → `*** Singapore 123456` (partial) or `***` (full)
* **SSN**: `123-45-6789` → `***-**-6789` (partial) or `***` (full)

### Temporal Access Control

**Managed via Databricks App:**
1. User requests access (partial/full)
2. Manager/governance officer approves
3. Access grant inserted into `pii_access_grants` table
4. User queries silver table → sees unmasked data
5. Access expires automatically
6. User queries again → sees masked data

**Access Grant Schema:**
```python
- user_email: User's email
- user_group: Role (analyst, scientist, engineer, manager, governance_officer)
- access_level: full_access, partial_access, masked_only
- granted_by: Approver
- granted_at: Start time
- expires_at: End time (NULL = permanent)
- is_active: Currently active?
- reason: Business justification
- approval_ticket_id: Workflow reference
```

### Scaling to 400 Tables

**For each silver table:**

1. **Create silver table** (materialized view with quality checks)
   ```python
   @dp.materialized_view(name="table_name_silver")
   def table_name_silver():
       config = create_standard_customer_config()
       return build_silver_table(bronze_df, config)
   ```

2. **Apply column masks** (4 ALTER statements)
   ```sql
   ALTER TABLE dev.experiment01.table_name_silver 
     ALTER COLUMN email SET MASK dev.experiment01.mask_email;
   
   ALTER TABLE dev.experiment01.table_name_silver 
     ALTER COLUMN phone SET MASK dev.experiment01.mask_phone;
   
   ALTER TABLE dev.experiment01.table_name_silver 
     ALTER COLUMN nric SET MASK dev.experiment01.mask_nric;
   
   ALTER TABLE dev.experiment01.table_name_silver 
     ALTER COLUMN address SET MASK dev.experiment01.mask_address USING COLUMNS (postal_code);
   ```

3. **Done!** Users query the silver table directly.

**Total effort per table:**
* ~15 lines Python (silver table)
* 4 ALTER statements (column masks)
* **No gold layer needed**

### Performance Considerations

**Column Mask Performance:**
* Masking functions are evaluated **once per query** (not per row)
* Access grants table lookup is cached
* Minimal overhead compared to views with subqueries
* Native Unity Catalog feature optimized by Databricks

**Best Practices:**
* Keep `pii_access_grants` table small (only active grants)
* Add index on `user_email` column
* Regularly purge expired grants
* Monitor query performance

### Removing Column Masks

If needed, remove masks from a table:

```sql
-- Remove mask from a column
ALTER TABLE dev.experiment01.customers_silver 
  ALTER COLUMN email DROP MASK;

-- Remove all masks from a table
ALTER TABLE dev.experiment01.customers_silver 
  ALTER COLUMN email DROP MASK;
ALTER TABLE dev.experiment01.customers_silver 
  ALTER COLUMN phone DROP MASK;
ALTER TABLE dev.experiment01.customers_silver 
  ALTER COLUMN nric DROP MASK;
ALTER TABLE dev.experiment01.customers_silver 
  ALTER COLUMN address DROP MASK;
```

---

## File Organization

```
pipeline_root/
├── utils/
│   ├── column_utils.py          # Column name normalization
│   ├── validators.py            # Validation functions
│   ├── transformations.py       # Transformation functions
│   ├── data_quality.py          # Quality tracking
│   └── silver_builder.py        # Silver table framework
├── setup/
│   ├── create_masking_udfs.sql  # Masking UDFs (run once)
│   └── apply_column_masks.py    # Script to apply masks to 400 tables
├── transformations/
│   ├── bronze/
│   │   └── ... (200+ files)
│   ├── silver/
│   │   └── ... (400+ files - materialized views with column masks)
│   └── governance/
│       └── access_control.py    # Access grants table
└── tests/
    └── test_column_normalization.py
```

---

## Testing Workflow

1. **Create pipeline layers:**
   - Bronze: Ingest raw data
   - Silver: Apply quality checks
   - Governance: Create access grants table

2. **Run pipeline update:**
   - Materializes bronze and silver tables

3. **Create masking UDFs:**
   - Run `setup/create_masking_udfs.sql` in SQL Editor

4. **Apply column masks:**
   - Run `setup/apply_column_masks.py` or manual ALTER statements

5. **Test access control:**
   - Query as different users
   - Verify masking levels
   - Test temporal expiration

6. **Test approval workflow:**
   - Submit access request via App
   - Approve request
   - Verify unmasked access
   - Wait for expiration
   - Verify masking restored

---

## Summary: Why Column Masks Win

**For 400 tables with 500 users:**

✅ **Minimal code** - 4 ALTER statements per table  
✅ **Centralized logic** - 5 UDFs reused everywhere  
✅ **Query-time masking** - Dynamic per user  
✅ **No storage overhead** - No duplicate tables  
✅ **Native performance** - Built into Databricks  
✅ **Easy maintenance** - Update UDF once, applies to all tables  
✅ **BI tool compatible** - Works with Tableau, Power BI, etc.  
✅ **Temporal access** - Automatic expiration enforcement  

**Total implementation:**
* 5 masking UDFs (one-time)
* 400 silver tables (~15 lines each)
* 1,600 ALTER statements (4 per table, scripted)
* 1 access grants table
* 1 Databricks App for access management

**vs. SQL Views approach:**
* 400 silver tables
* 400 gold views (~50 lines each = 20,000 lines)
* Subquery overhead on every query
* Harder to maintain

**Column masks are the clear winner for scale!** 🏆
