# Data Cleansing Pipeline - Ingestion & Silver Layer Framework

## Overview
This pipeline provides standardized frameworks for:
* **200+ Bronze ingestions** with consistent column naming
* **400+ Silver tables** with data quality checks and transformations

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

**Minimal Code Example (56 lines including comments):**
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

**Custom Configuration Example:**
```python
from utils.silver_builder import SilverTableConfig
from utils import validators, transformations

config = SilverTableConfig()

# Add transformations
config.add_transformation("nric", transformations.standardize_nric)
config.add_transformation("country", transformations.normalize_nationality_code)

# Add validations
config.add_validation("nric", validators.validate_singapore_nric)
config.add_validation("email", validators.validate_email)

# Uppercase fields
config.to_uppercase(["full_name", "nric", "gender"])

# Fill nulls
config.fill_nulls(["email", "phone"])

# Build silver table
silver_df = build_silver_table(bronze_df, config)
```

### Quality Checks Applied

**Standard Customer Config includes:**
1. **NRIC**: Singapore format validation, uppercase standardization
2. **Email**: Valid email format check
3. **Gender**: Valid values (M, F, X), uppercase
4. **Country**: Valid nationality codes, converted to 2-letter ISO uppercase
5. **Phone**: Standardized to +65 format
6. **Names**: Uppercase, trimmed
7. **Nulls**: Filled with 'None' string
8. **Postal Code**: Extracted from address, validated, standardized to 6 digits

**Output Columns:**
* `data_quality_flags` - Comma-separated list of failed validations (null if all pass)
* `quality_score` - Percentage of passed validations (0-100)
* `is_valid_postal_code` - Boolean for postal code validation

### Scaling to 400 Tables

**Benefits:**
* ✅ **10-20 lines of code** per table (vs 100+ without framework)
* ✅ **Consistent quality checks** across all tables
* ✅ **Easy to maintain** - update utilities once, applies everywhere
* ✅ **Configuration-driven** - no repetitive code
* ✅ **Invalid values kept and flagged** - no data loss
* ✅ **Quality metrics** - track data quality over time

**For Future Ingestions with Field Prefixes:**
```python
config = SilverTableConfig(source_prefix="customer_")
# All source columns will be prefixed: customer_id, customer_name, etc.
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
├── transformations/
│   ├── bronze/
│   │   ├── customers.py         # Bronze ingestion examples
│   │   └── ... (200+ files)
│   └── silver/
│       ├── customers_silver.py  # Silver transformation examples
│       └── ... (400+ files)
└── tests/
    └── test_column_normalization.py
```

---

## Testing Workflow
1. Create new table file
2. Run dry run: Validate configuration
3. Fix any issues
4. Run pipeline update: Process data
5. Verify quality metrics and transformations

---

## Example: Customers Silver Table

See `transformations/silver/customers_silver.py` for a complete working example that demonstrates:
* Using the standard customer config
* Postal code extraction and validation
* Quality flags and scores
* Metadata tracking
