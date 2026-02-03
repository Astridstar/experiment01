# Data Cleansing Pipeline - Complete Framework for 600+ Tables

## Overview
This pipeline provides standardized frameworks for:
* **200+ Bronze ingestions** with consistent column naming
* **400+ Silver tables** with SCD Type 2, data quality checks, and transformations
* **Dynamic PII masking** using Unity Catalog column masks

---

## Architecture Overview

```
Bronze (Streaming Tables)
    ↓
Silver Source Views (Quality Checks + Transformations)
    ↓
Silver Tables (SCD Type 2 with Auto CDC) + Column Masks
    ↓
Users Query Silver Directly (Masking Applied at Query Time)
```

---

## Bronze Layer: Ingestion Templates

### Column Naming Convention
All ingested data follows these rules:
* **Lowercase**: All column names converted to lowercase
* **Trimmed**: Leading/trailing whitespace removed
* **Underscores**: Spaces replaced with underscores
* **Example**: `"  Customer Name  "` → `"customer_name"`

### Quick Start for Bronze Ingestions

```python
from pyspark import pipelines as dp
from pyspark.sql import functions as F
from utils.column_utils import normalize_dataframe_columns

@dp.table(name="table_name_raw")
def table_name_raw():
    df = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", True)
        .load("/Volumes/catalog/schema/volume/path")
        .withColumn("ingested_file", F.col("_metadata.file_path"))
        .withColumn("ingestion_ts", F.current_timestamp())
    )
    return normalize_dataframe_columns(df)
```

---

## Silver Layer: SCD Type 2 + Data Quality Framework

### Overview
Silver tables use **SCD Type 2** for historical tracking with:
1. Data quality checks and transformations
2. Automatic versioning with `__START_AT` and `__END_AT` columns
3. **Unity Catalog column masks** for dynamic PII protection

### Framework Components

#### 1. Validators (`utils/validators.py`)
* `validate_singapore_postal_code()` - 6-digit format
* `validate_singapore_nric()` - Singapore NRIC format with checksum
* `validate_currency_code()` - USD, RMB, YEN, SGD, CNY, JPY
* `validate_nationality_code()` - US, UK, SG, CN, TW, FR, DK
* `validate_gender()` - M, F, X
* `validate_email()` - Email format

#### 2. Transformations (`utils/transformations.py`)
* `standardize_singapore_postal_code()` - Pad to 6 digits
* `standardize_nric()` - Uppercase, trimmed
* `normalize_currency_code()` - ISO codes
* `normalize_nationality_code()` - 2-letter ISO
* `normalize_gender()` - Uppercase M/F/X
* `normalize_name()` - Uppercase, trimmed
* `standardize_phone_number()` - +65 format

#### 3. Data Quality (`utils/data_quality.py`)
* `flag_invalid_values()` - Comma-separated list of failures
* `add_quality_score()` - Percentage of passed validations
* `fill_nulls_with_none()` - Fill nulls with 'None'

#### 4. Silver Builder (`utils/silver_builder.py`)
* `SilverTableConfig` - Configuration class
* `build_silver_table()` - Apply transformations and validations
* `create_standard_customer_config()` - Pre-built patterns

#### 5. SCD Type 2 Builder (`utils/scd_builder.py`)
* `SCDType2Config` - SCD configuration class
* **Config Factories:**
  * `create_customer_scd_config()` - For customer tables
  * `create_transaction_scd_config()` - For transaction tables
  * `create_product_scd_config()` - For product tables
  * `create_generic_scd_config()` - For any table type
* **Query Helpers:**
  * `query_scd_type2_current()` - Get current records only
  * `query_scd_type2_as_of()` - Time travel queries
  * `query_scd_type2_history()` - Full history for specific records

---

## Creating Silver Tables with SCD Type 2

### Pattern for 400 Tables (50 lines per table)

```python
from pyspark import pipelines as dp
from pyspark.sql import functions as F
from utils.silver_builder import build_silver_table, create_standard_customer_config, extract_postal_code_and_validate
from utils.scd_builder import create_customer_scd_config

# Step 1: Create source view with quality checks
@dp.view(name="table_name_silver_source")
def table_name_silver_source():
    bronze_df = spark.readStream.table("dev.experiment01.table_name_raw")
    config = create_standard_customer_config()
    silver_df = build_silver_table(bronze_df, config, add_quality_flags=True, add_quality_score=True)
    silver_df = extract_postal_code_and_validate(silver_df, "address")
    return silver_df.withColumn("silver_processed_ts", F.current_timestamp())

# Step 2 & 3: Create SCD Type 2 using config factory
scd_config = create_customer_scd_config(table_name="table_name")

dp.create_streaming_table(name=scd_config.target_name, comment="SCD Type 2 table")

dp.create_auto_cdc_flow(
    target=scd_config.target_name,
    source=scd_config.source_name,
    keys=scd_config.keys,
    sequence_by=scd_config.sequence_by,
    stored_as_scd_type=2,
    track_history_except_column_list=scd_config.track_history_except_columns,
    ignore_null_updates=scd_config.ignore_null_updates
)
```

### Config Factory Examples

**Customer Tables:**
```python
# Standard customer table
config = create_customer_scd_config("customers")

# Custom keys
config = create_customer_scd_config("vip_customers", keys=["vip_id"])

# Track all columns including metadata
config = create_customer_scd_config("customers", track_all_columns=True)
```

**Transaction Tables:**
```python
# Standard transaction table
config = create_transaction_scd_config("orders")

# Track status changes only
config = create_transaction_scd_config("payments", track_status_only=True)

# Custom keys
config = create_transaction_scd_config("invoices", keys=["invoice_id"])
```

**Product Tables:**
```python
# Standard product table
config = create_product_scd_config("products")

# Don't track price changes
config = create_product_scd_config("inventory", track_price_changes=False)
```

**Generic Tables:**
```python
# Full customization
config = create_generic_scd_config(
    table_name="employees",
    keys=["employee_id"],
    sequence_by="modified_ts",
    track_history_columns=["salary", "department", "title"],
    apply_as_deletes="is_deleted = true"
)
```

---

## SCD Type 2 Features

### Automatic Columns

Every SCD Type 2 table includes:
* `__START_AT` - When this version became active (TIMESTAMP)
* `__END_AT` - When this version became inactive (NULL = current)

### History Tracking Options

**Option 1: Track all columns (default)**
```python
stored_as_scd_type=2
# Every column change creates a new version
```

**Option 2: Exclude metadata columns (recommended)**
```python
track_history_except_column_list=[
    "ingested_file",
    "ingestion_ts",
    "silver_processed_ts",
    "data_quality_flags",
    "quality_score"
]
# Changes to these columns don't create new versions
```

**Option 3: Track specific columns only**
```python
track_history_column_list=["email", "phone", "address", "status"]
# Only changes to these columns create new versions
```

### Querying SCD Type 2 Tables

**Get current records only:**
```python
from utils.scd_builder import query_scd_type2_current

current = query_scd_type2_current("dev.experiment01.customers_silver")
display(current)
```

Or in SQL:
```sql
SELECT * FROM dev.experiment01.customers_silver
WHERE __END_AT IS NULL;
```

**Time travel (as of specific date):**
```python
from utils.scd_builder import query_scd_type2_as_of

historical = query_scd_type2_as_of("dev.experiment01.customers_silver", "2024-01-01")
display(historical)
```

Or in SQL:
```sql
SELECT * FROM dev.experiment01.customers_silver
WHERE __START_AT <= '2024-01-01'
  AND (__END_AT IS NULL OR __END_AT > '2024-01-01');
```

**Full history for a specific customer:**
```python
from utils.scd_builder import query_scd_type2_history

history = query_scd_type2_history("dev.experiment01.customers_silver", {"customer_id": 123})
display(history)
```

Or in SQL:
```sql
SELECT * FROM dev.experiment01.customers_silver
WHERE customer_id = 123
ORDER BY __START_AT;
```

---

## Dynamic PII Masking with Column Masks

### Implementation Steps

#### **Step 1: Create Masking UDFs (One-Time)**

Run `setup/create_masking_udfs.sql` in **SQL Editor with SQL Warehouse**:
```sql
CREATE FUNCTION dev.experiment01.mask_email(email STRING) ...
CREATE FUNCTION dev.experiment01.mask_phone(phone STRING) ...
CREATE FUNCTION dev.experiment01.mask_nric(nric STRING) ...
CREATE FUNCTION dev.experiment01.mask_address(address STRING, postal_code STRING) ...
```

#### **Step 2: Apply Column Masks to Silver Tables**

Run in **SQL Editor with SQL Warehouse**:
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

**Important:** Column masks must be applied via **SQL Warehouse**, not interactive/assigned clusters.

### PII Masking Rules

**Access Levels:**
* **full_access**: All PII visible (governance_officers, approved requests)
* **partial_access**: Partial masking (scientists, engineers with approval)
* **masked_only**: Full masking (analysts, managers, default)

**Masking Patterns:**
* **Email**: `user@domain.com` → `u***@domain.com` (partial) or `***@***` (full)
* **Phone**: `+65 12345678` → `+65 ****5678` (partial) or `***` (full)
* **NRIC**: `S1234567D` → `S****567D` (partial) or `***` (full)
* **Address**: Full → `*** Singapore 123456` (partial) or `***` (full)

---

## Scaling to 400 Tables

### Minimal Code Per Table

**For Customer-like tables (50 lines):**
```python
from utils.scd_builder import create_customer_scd_config

@dp.view(name="table_name_silver_source")
def table_name_silver_source():
    bronze_df = spark.readStream.table("dev.experiment01.table_name_raw")
    config = create_standard_customer_config()
    return build_silver_table(bronze_df, config).withColumn("silver_processed_ts", F.current_timestamp())

scd_config = create_customer_scd_config("table_name")
dp.create_streaming_table(name=scd_config.target_name)
dp.create_auto_cdc_flow(
    target=scd_config.target_name,
    source=scd_config.source_name,
    keys=scd_config.keys,
    sequence_by=scd_config.sequence_by,
    stored_as_scd_type=2,
    track_history_except_column_list=scd_config.track_history_except_columns,
    ignore_null_updates=scd_config.ignore_null_updates
)
```

**For Transaction tables:**
```python
from utils.scd_builder import create_transaction_scd_config

scd_config = create_transaction_scd_config("orders")
# ... same pattern as above
```

**For Product tables:**
```python
from utils.scd_builder import create_product_scd_config

scd_config = create_product_scd_config("products")
# ... same pattern as above
```

**For Custom tables:**
```python
from utils.scd_builder import create_generic_scd_config

scd_config = create_generic_scd_config(
    table_name="employees",
    keys=["employee_id"],
    sequence_by="modified_ts",
    track_history_columns=["salary", "department"]
)
# ... same pattern as above
```

### Apply Column Masks (4 lines per table)

```sql
ALTER TABLE dev.experiment01.table_name_silver ALTER COLUMN email SET MASK dev.experiment01.mask_email;
ALTER TABLE dev.experiment01.table_name_silver ALTER COLUMN phone SET MASK dev.experiment01.mask_phone;
ALTER TABLE dev.experiment01.table_name_silver ALTER COLUMN nric SET MASK dev.experiment01.mask_nric;
ALTER TABLE dev.experiment01.table_name_silver ALTER COLUMN address SET MASK dev.experiment01.mask_address USING COLUMNS (postal_code);
```

---

## Complete Implementation Summary

### Total Effort for 400 Tables

**One-time setup:**
* 5 masking UDFs (30 minutes)
* 4 config factory functions (already done)
* 1 access grants table (already done)

**Per table:**
* ~50 lines Python (silver table with SCD Type 2)
* 4 ALTER statements (column masks)

**Total: ~54 lines per table** vs 200+ without framework

### Benefits

✅ **SCD Type 2** - Automatic historical tracking  
✅ **Time travel** - Query data at any point in time  
✅ **Data quality** - Integrated validation and flagging  
✅ **PII masking** - Dynamic per-user at query time  
✅ **Temporal access** - Auto-expiring permissions  
✅ **Minimal code** - Config factories reduce boilerplate  
✅ **Centralized logic** - Update once, applies everywhere  
✅ **Audit trail** - Full history + access logs  

---

## File Organization

```
pipeline_root/
├── utils/
│   ├── column_utils.py          # Column name normalization
│   ├── validators.py            # Validation functions
│   ├── transformations.py       # Transformation functions
│   ├── data_quality.py          # Quality tracking
│   ├── silver_builder.py        # Silver table framework
│   ├── scd_builder.py           # SCD Type 2 framework + config factories
│   └── pii_masking.py           # PII masking utilities
├── setup/
│   ├── create_masking_udfs.sql  # Masking UDFs (run once in SQL Editor)
│   └── apply_column_masks.py    # Generate ALTER statements
├── transformations/
│   ├── bronze/
│   │   └── ... (200+ files)
│   ├── silver/
│   │   ├── customers_silver.py  # Example with SCD Type 2
│   │   └── ... (400+ files)
│   └── governance/
│       └── access_control.py    # Access grants table
└── tests/
    └── test_column_normalization.py
```

---

## Complete Workflow

### 1. Create Bronze Tables
```python
@dp.table(name="customers_raw")
def customers_raw():
    return spark.readStream.format("cloudFiles")...
```

### 2. Create Silver Tables with SCD Type 2
```python
# Source view with quality checks
@dp.view(name="customers_silver_source")
def customers_silver_source():
    return build_silver_table(bronze_df, config)

# SCD Type 2 flow
scd_config = create_customer_scd_config("customers")
dp.create_streaming_table(name=scd_config.target_name)
dp.create_auto_cdc_flow(...scd_config parameters...)
```

### 3. Run Pipeline
```bash
# Creates bronze and silver tables with SCD Type 2
databricks pipelines update --pipeline-id <pipeline-id>
```

### 4. Create Masking UDFs (One-Time)
```sql
-- Run in SQL Editor with SQL Warehouse
CREATE FUNCTION dev.experiment01.mask_email(email STRING) ...
```

### 5. Apply Column Masks
```sql
-- Run in SQL Editor with SQL Warehouse
ALTER TABLE dev.experiment01.customers_silver ALTER COLUMN email SET MASK dev.experiment01.mask_email;
```

### 6. Query with Dynamic Masking
```sql
-- Each user sees different data based on access level
SELECT * FROM dev.experiment01.customers_silver WHERE __END_AT IS NULL;
```

---

## Example: Complete Customer Pipeline

**Files:**
* Bronze: `transformations/bronze/customers_bronze.py`
* Silver: `transformations/silver/customers_silver.py` (with SCD Type 2)
* Access Control: `transformations/governance/access_control.py`
* Masking UDFs: `setup/create_masking_udfs.sql`

**Query Examples:**
```sql
-- Current customers only
SELECT * FROM dev.experiment01.customers_silver WHERE __END_AT IS NULL;

-- Customer history
SELECT * FROM dev.experiment01.customers_silver WHERE customer_id = 123 ORDER BY __START_AT;

-- Customers as of Jan 1, 2024
SELECT * FROM dev.experiment01.customers_silver 
WHERE __START_AT <= '2024-01-01' AND (__END_AT IS NULL OR __END_AT > '2024-01-01');
```

---

## Key Advantages

| Feature | Your Framework | Without Framework |
|---------|---------------|-------------------|
| **Code per table** | 50 lines | 200+ lines |
| **SCD Type 2** | ✅ Automatic | ❌ Manual versioning |
| **Data quality** | ✅ Integrated | ❌ Separate code |
| **PII masking** | ✅ Dynamic per-user | ❌ Static or complex |
| **Time travel** | ✅ Built-in | ❌ Manual implementation |
| **Maintenance** | ✅ Update utilities once | ❌ Update 400 files |
| **Total code for 400 tables** | 20,000 lines | 80,000+ lines |

**You've reduced code by 75% while adding more features!** 🎉
