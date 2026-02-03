# Data Cleansing Pipeline - Ingestion, Silver & Gold Layers with PII Masking

## Overview
This pipeline provides standardized frameworks for:
* **200+ Bronze ingestions** with consistent column naming
* **400+ Silver tables** with data quality checks and transformations
* **400+ Gold views** with dynamic PII masking and temporal access control

---

## Bronze Layer: Ingestion Templates

### Column Naming Convention
All ingested data follows these rules:
* **Lowercase**: All column names converted to lowercase
* **Trimmed**: Leading/trailing whitespace removed
* **Underscores**: Spaces replaced with underscores
* **Example**: `"  Customer Name  "` → `"customer_name"`

### Quick Start for Bronze Ingestions

**For Python:**
```python
from pyspark import pipelines as dp
from pyspark.sql import functions as F
from utils.column_utils import normalize_dataframe_columns

@dp.table(name="dev.experiment01.my_table")
def my_table():
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

## Silver Layer: Data Quality Framework

### Overview
The silver layer framework provides **configuration-driven** data quality and transformations, making it easy to scale to 400+ tables with minimal code per table.

### Framework Components

#### 1. Validators (`utils/validators.py`)
Validation functions that return boolean columns:
* `validate_singapore_postal_code()` - 6-digit format
* `validate_singapore_nric()` - Singapore NRIC format with checksum
* `validate_currency_code()` - USD, RMB, YEN, SGD, CNY, JPY
* `validate_nationality_code()` - US, UK, SG, CN, TW, FR, DK
* `validate_gender()` - M, F, X
* `validate_email()` - Email format
* `validate_nric_9char()` - 9 alphanumeric uppercase

#### 2. Transformations (`utils/transformations.py`)
Standardization functions:
* `standardize_singapore_postal_code()` - Pad to 6 digits with leading zeros
* `standardize_nric()` - Uppercase, trimmed
* `normalize_currency_code()` - Convert to ISO codes (CNY, JPY, USD, SGD)
* `normalize_nationality_code()` - Convert to 2-letter ISO (US, GB, SG, CN, TW, FR, DK)
* `normalize_gender()` - Uppercase M/F/X
* `normalize_name()` - Uppercase, trimmed
* `standardize_phone_number()` - Singapore +65 format
* `extract_postal_code_from_address()` - Extract 6-digit postal code
* `fill_null_with_none_string()` - Replace nulls with 'None'

#### 3. Data Quality (`utils/data_quality.py`)
Quality tracking functions:
* `flag_invalid_values()` - Creates comma-separated list of failed validations
* `add_quality_score()` - Calculates percentage of passed validations (0-100)
* `fill_nulls_with_none()` - Fill nulls with 'None' for specified columns
* `add_validation_columns()` - Add individual boolean validation columns

#### 4. Silver Builder (`utils/silver_builder.py`)
Configuration-driven framework:
* `SilverTableConfig` - Configuration class for transformations and validations
* `build_silver_table()` - Apply all configs to create silver table
* `create_standard_customer_config()` - Pre-built config for customer tables
* `extract_postal_code_and_validate()` - Helper for postal code processing

### Quick Start for Silver Tables

**Use Materialized Views for Silver Layer:**
```python
from pyspark import pipelines as dp
from pyspark.sql import functions as F
from utils.silver_builder import build_silver_table, create_standard_customer_config

@dp.materialized_view(name="dev.experiment01.my_table_silver")
def my_table_silver():
    # Read bronze
    bronze_df = spark.read.table("dev.experiment01.my_table_raw")
    
    # Use standard config or create custom
    config = create_standard_customer_config()
    
    # Apply transformations and validations
    silver_df = build_silver_table(bronze_df, config)
    
    # Add metadata
    return silver_df.withColumn("silver_processed_ts", F.current_timestamp())
```

---

## Gold Layer: Dynamic PII Masking Framework

### Overview
The gold layer provides **temporal, user-based PII masking** for 500 users across 400 tables using dynamic views and an access control table.

### Architecture

**3-Tier Access Control:**
1. **Access Grants Table** - Tracks temporal access permissions
2. **PII Masking Utilities** - Reusable masking functions
3. **Gold Views** - Dynamic views that apply masking based on current user

### PII Masking Rules

**Supported PII Fields:**
* **Email**: `user@domain.com` → `u***@domain.com` (partial) or `***@***` (full)
* **Phone**: `+65 12345678` → `+65 ****5678` (partial) or `***` (full)
* **NRIC**: `S1234567D` → `S****567D` (partial) or `***` (full)
* **Address**: Full address → `*** Singapore 123456` (partial) or `***` (full)
* **SSN**: `123-45-6789` → `***-**-6789` (partial) or `***` (full)

**Access Levels:**
* **full_access**: All PII visible (governance_officers, approved requests)
* **partial_access**: Partial masking (scientists, engineers with approval)
* **masked_only**: Full masking (analysts, managers, default)

**User Groups:**
* `analyst` - Default: masked_only
* `scientist` - Default: partial_access
* `engineer` - Default: masked_only
* `manager` - Default: masked_only
* `governance_officer` - Default: full_access

### Components

#### 1. Access Control Table (`transformations/governance/access_control.py`)

Tracks temporal access grants:
```python
Schema:
- user_email: User's email address
- user_group: User's role (analyst, scientist, engineer, manager, governance_officer)
- access_level: full_access, partial_access, masked_only
- granted_by: Who approved the access
- granted_at: When access was granted
- expires_at: When access expires (NULL for permanent)
- is_active: Whether access is currently active
- reason: Business justification for access
- approval_ticket_id: Reference to approval workflow
```

#### 2. PII Masking Utilities (`utils/pii_masking.py`)

Reusable masking functions:
* `mask_email(col, access_level)` - Mask email prefix
* `mask_phone(col, access_level)` - Mask phone middle digits
* `mask_nric(col, access_level)` - Mask NRIC middle digits
* `mask_address(col, access_level)` - Keep postal code only
* `mask_ssn(col, access_level)` - Mask all but last 4
* `apply_pii_masking(df, access_level_col)` - Apply all masks to DataFrame

#### 3. Gold Views (`transformations/gold/customers_gold.py`)

Dynamic views with masking:
```python
@dp.view(name="dev.experiment01.customers_gold")
def customers_gold():
    # Read silver layer
    silver_df = spark.read.table("dev.experiment01.customers_silver")
    
    # Get current user's access level
    access_grants = spark.read.table("dev.experiment01.pii_access_grants")
    current_user = F.current_user()
    
    # Filter active grants for current user
    user_access = access_grants.filter(
        (F.col("user_email") == current_user) &
        (F.col("is_active") == True) &
        ((F.col("expires_at").isNull()) | (F.col("expires_at") > F.current_timestamp()))
    )
    
    # Apply masking based on access level
    result_df = silver_df.crossJoin(user_access)
    result_df = apply_pii_masking(result_df, "access_level")
    
    return result_df
```

### Databricks App for Access Management

**App Purpose:** Manage temporal PII access grants with approval workflow

**Key Features:**
* Request access to unmasked PII data
* Approval workflow (manager/governance officer approval)
* Temporal access grants (auto-expire)
* Audit trail of all access requests
* Revoke access before expiration

**App Components:**

1. **Request Form:**
   - User email
   - Requested access level (partial/full)
   - Business justification
   - Duration (hours/days)
   - Tables needed

2. **Approval Workflow:**
   - Notify approvers (managers, governance officers)
   - Review request details
   - Approve/Reject with comments
   - Auto-update access_grants table

3. **Access Management:**
   - View active grants
   - Revoke access
   - Extend expiration
   - Audit log

**Implementation Steps:**

1. Create Databricks App with Streamlit/Dash
2. Use Databricks SQL connector to read/write access_grants table
3. Integrate with Databricks Workflows for approval notifications
4. Use OAuth for user authentication
5. Log all actions to audit table

**Example App Code Structure:**
```python
# app.py (Databricks App)
import streamlit as st
from databricks import sql

# Request Access Form
st.title("PII Access Request")
user_email = st.text_input("Your Email")
access_level = st.selectbox("Access Level", ["partial_access", "full_access"])
reason = st.text_area("Business Justification")
duration_hours = st.number_input("Duration (hours)", min_value=1, max_value=168)

if st.button("Submit Request"):
    # Insert into access_grants table with is_active=False
    # Trigger approval workflow
    # Send notification to approvers
    pass

# Approval Interface (for managers/governance officers)
st.title("Pending Approvals")
pending_requests = get_pending_requests()
for request in pending_requests:
    if st.button(f"Approve {request['user_email']}"):
        # Update access_grants: set is_active=True, set expires_at
        # Send notification to requester
        pass
```

### Scaling to 400 Tables

**Pattern for Each Table:**
```python
# Silver layer (materialized view)
@dp.materialized_view(name="dev.experiment01.table_name_silver")
def table_name_silver():
    config = create_standard_customer_config()
    return build_silver_table(bronze_df, config)

# Gold layer (dynamic view with masking)
@dp.view(name="dev.experiment01.table_name_gold")
def table_name_gold():
    silver_df = spark.read.table("dev.experiment01.table_name_silver")
    access_grants = spark.read.table("dev.experiment01.pii_access_grants")
    # Apply masking logic (same pattern for all 400 tables)
    return masked_df
```

**Benefits:**
* ✅ **Scalable for 500 users** - Single access control table
* ✅ **Temporal access** - Auto-expiring grants
* ✅ **Audit trail** - All access logged
* ✅ **Workflow integration** - Databricks App + Workflows
* ✅ **Minimal code per table** - Reusable masking utilities
* ✅ **No Unity Catalog UDF dependency** - Works immediately

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
│   └── pii_masking.py           # PII masking utilities
├── transformations/
│   ├── bronze/
│   │   └── ... (200+ files)
│   ├── silver/
│   │   └── ... (400+ files - materialized views)
│   ├── gold/
│   │   └── ... (400+ files - dynamic views with masking)
│   └── governance/
│       └── access_control.py    # Access grants table
└── tests/
    └── test_column_normalization.py
```

---

## Testing Workflow
1. Create bronze, silver, and gold layers
2. Run dry run: Validate configuration
3. Test masking with different users
4. Verify temporal access expiration
5. Test approval workflow via Databricks App

---

## Example: Complete Customer Pipeline

See implementation files:
* Bronze: `transformations/bronze/customers.py`
* Silver: `transformations/silver/customers_silver.py` (materialized view)
* Gold: `transformations/gold/customers_gold.py` (dynamic view with masking)
* Access Control: `transformations/governance/access_control.py`
