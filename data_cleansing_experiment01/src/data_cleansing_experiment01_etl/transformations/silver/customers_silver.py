"""
Silver layer: Customers table with data quality checks and transformations.
Demonstrates the silver table framework for scalable data quality management.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from utils.silver_builder import build_silver_table, create_standard_customer_config, extract_postal_code_and_validate


@dp.table(
    name="customers_silver",
    comment="Silver layer: Cleaned and validated customer data with quality flags"
)
def customers_silver():
    """
    Transform bronze customers into silver layer with:
    - Data quality validations (NRIC, email, gender, country, postal code)
    - Standardized transformations (uppercase names/NRIC/gender/country)
    - Null handling (fill with 'None')
    - Quality flags and scores
    - Postal code extraction and validation
    
    Quality Checks Applied:
    1. NRIC: Singapore format validation (9 alphanumeric, uppercase)
    2. Email: Valid email format
    3. Gender: Valid values (M, F, X) uppercase
    4. Country: Valid nationality codes (US, GB, SG, CN, TW, FR, DK) uppercase
    5. Postal Code: Singapore 6-digit format
    6. Nulls: Filled with 'None' string
    
    Invalid values are KEPT and FLAGGED in data_quality_flags column.
    
    NOTE: Using @dp.table() with spark.readStream to create an actual streaming table
    (Delta table) that supports Unity Catalog column masks for PII protection.
    """
    # Read from bronze layer using streaming
    bronze_df = spark.readStream.table("dev.experiment01.customers_raw")
    
    # Create standard customer configuration
    config = create_standard_customer_config()
    
    # Apply silver transformations and validations
    silver_df = build_silver_table(
        bronze_df,
        config,
        add_quality_flags=True,
        add_quality_score=True
    )
    
    # Extract and validate postal code from address
    silver_df = extract_postal_code_and_validate(silver_df, "address")
    
    # Add silver layer metadata
    silver_df = silver_df.withColumn("silver_processed_ts", F.current_timestamp())
    
    # Select final columns in logical order
    return silver_df.select(
        "customer_id",
        "full_name",
        "email",
        "phone",
        "nric",
        "dob",
        "address",
        "postal_code",
        "country",
        "gender",
        "signup_ts",
        "last_login_ts",
        "status",
        "segment",
        "credit_score",
        "annual_income",
        "data_quality_flags",
        "quality_score",
        "is_valid_postal_code",
        "ingested_file",
        "ingestion_ts",
        "silver_processed_ts"
    )
