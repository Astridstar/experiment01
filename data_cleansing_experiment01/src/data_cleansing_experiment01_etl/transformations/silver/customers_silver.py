"""
Silver layer: Customers table with SCD Type 2 and data quality checks.
Demonstrates the SCD Type 2 framework for scalable historical tracking across 400 tables.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from utils.silver_builder import build_silver_table, create_standard_customer_config, extract_postal_code_and_validate
from utils.scd_builder import create_customer_scd_config


# Step 1: Create intermediate view with quality checks and transformations
@dp.view(name="customers_silver_source")
def customers_silver_source():
    """
    Intermediate view that applies quality checks and transformations to bronze data.
    This view feeds into the SCD Type 2 flow.
    """
    # Read from bronze layer (streaming)
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
    
    return silver_df


# Step 2 & 3: Create SCD Type 2 target and flow using config factory
scd_config = create_customer_scd_config(table_name="customers")

# Create target streaming table
dp.create_streaming_table(
    name=scd_config.target_name,
    comment="Silver layer: Customer data with SCD Type 2 historical tracking and quality flags"
)

# Create Auto CDC flow with SCD Type 2
dp.create_auto_cdc_flow(
    target=scd_config.target_name,
    source=scd_config.source_name,
    keys=scd_config.keys,
    sequence_by=scd_config.sequence_by,
    stored_as_scd_type=2,
    track_history_except_column_list=scd_config.track_history_except_columns,
    ignore_null_updates=scd_config.ignore_null_updates
)
