"""
SCD Type 2 builder framework for scalable historical tracking.
Provides reusable patterns to create SCD Type 2 flows for 400+ tables efficiently.
"""

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from typing import List, Optional, Dict, Callable
from utils.silver_builder import SilverTableConfig, build_silver_table


class SCDType2Config:
    """
    Configuration class for SCD Type 2 flows.
    """
    
    def __init__(
        self,
        target_name: str,
        source_name: str,
        keys: List[str],
        sequence_by: str,
        track_history_columns: Optional[List[str]] = None,
        track_history_except_columns: Optional[List[str]] = None,
        ignore_null_updates: bool = True,
        apply_as_deletes: Optional[str] = None
    ):
        """
        Initialize SCD Type 2 configuration.
        
        Args:
            target_name: Name of the target streaming table
            source_name: Name of the source table/view with changes
            keys: Primary key columns
            sequence_by: Column for ordering changes (timestamp, version)
            track_history_columns: Columns to track history for (None = all columns)
            track_history_except_columns: Columns to exclude from history tracking
            ignore_null_updates: If True, NULL values won't overwrite existing values
            apply_as_deletes: SQL expression for delete operations (e.g., "is_deleted = true")
        """
        self.target_name = target_name
        self.source_name = source_name
        self.keys = keys
        self.sequence_by = sequence_by
        self.track_history_columns = track_history_columns
        self.track_history_except_columns = track_history_except_columns
        self.ignore_null_updates = ignore_null_updates
        self.apply_as_deletes = apply_as_deletes


# =============================================================================
# Config Factory Functions for Common Table Patterns
# =============================================================================

def create_customer_scd_config(
    table_name: str = "customers",
    keys: List[str] = None,
    sequence_by: str = "silver_processed_ts",
    track_all_columns: bool = False
) -> SCDType2Config:
    """
    Create SCD Type 2 config for customer tables.
    
    Args:
        table_name: Base table name (without _silver suffix)
        keys: Primary key columns (default: ["customer_id"])
        sequence_by: Sequence column (default: "silver_processed_ts")
        track_all_columns: If False, excludes metadata columns from history
        
    Returns:
        SCDType2Config for customer tables
        
    Example:
        config = create_customer_scd_config("customers")
        config = create_customer_scd_config("vip_customers", keys=["vip_id"])
    """
    if keys is None:
        keys = ["customer_id"]
    
    # Standard metadata columns to exclude from history tracking
    metadata_columns = [
        "ingested_file",
        "ingestion_ts",
        "silver_processed_ts",
        "data_quality_flags",
        "quality_score",
        "is_valid_postal_code"
    ]
    
    return SCDType2Config(
        target_name=f"{table_name}_silver",
        source_name=f"{table_name}_silver_source",
        keys=keys,
        sequence_by=sequence_by,
        track_history_except_columns=None if track_all_columns else metadata_columns,
        ignore_null_updates=True
    )


def create_transaction_scd_config(
    table_name: str = "transactions",
    keys: List[str] = None,
    sequence_by: str = "transaction_ts",
    track_status_only: bool = False
) -> SCDType2Config:
    """
    Create SCD Type 2 config for transaction tables.
    
    Args:
        table_name: Base table name (without _silver suffix)
        keys: Primary key columns (default: ["transaction_id"])
        sequence_by: Sequence column (default: "transaction_ts")
        track_status_only: If True, only track history for status changes
        
    Returns:
        SCDType2Config for transaction tables
        
    Example:
        config = create_transaction_scd_config("orders")
        config = create_transaction_scd_config("payments", track_status_only=True)
    """
    if keys is None:
        keys = ["transaction_id"]
    
    # For transactions, typically track status changes only
    track_columns = ["status", "amount", "updated_at"] if track_status_only else None
    
    return SCDType2Config(
        target_name=f"{table_name}_silver",
        source_name=f"{table_name}_silver_source",
        keys=keys,
        sequence_by=sequence_by,
        track_history_columns=track_columns,
        ignore_null_updates=True
    )


def create_product_scd_config(
    table_name: str = "products",
    keys: List[str] = None,
    sequence_by: str = "updated_at",
    track_price_changes: bool = True
) -> SCDType2Config:
    """
    Create SCD Type 2 config for product tables.
    
    Args:
        table_name: Base table name (without _silver suffix)
        keys: Primary key columns (default: ["product_id"])
        sequence_by: Sequence column (default: "updated_at")
        track_price_changes: If True, track history for price-related columns
        
    Returns:
        SCDType2Config for product tables
        
    Example:
        config = create_product_scd_config("products")
        config = create_product_scd_config("inventory", track_price_changes=False)
    """
    if keys is None:
        keys = ["product_id"]
    
    # For products, track price and availability changes
    track_columns = ["price", "cost", "availability", "status"] if track_price_changes else None
    
    return SCDType2Config(
        target_name=f"{table_name}_silver",
        source_name=f"{table_name}_silver_source",
        keys=keys,
        sequence_by=sequence_by,
        track_history_columns=track_columns,
        ignore_null_updates=True
    )


def create_generic_scd_config(
    table_name: str,
    keys: List[str],
    sequence_by: str = "updated_at",
    track_history_columns: Optional[List[str]] = None,
    track_history_except_columns: Optional[List[str]] = None,
    ignore_null_updates: bool = True,
    apply_as_deletes: Optional[str] = None
) -> SCDType2Config:
    """
    Create a generic SCD Type 2 config for any table type.
    
    Args:
        table_name: Base table name (without _silver suffix)
        keys: Primary key columns
        sequence_by: Sequence column for ordering changes
        track_history_columns: Specific columns to track (None = all)
        track_history_except_columns: Columns to exclude from tracking
        ignore_null_updates: If True, NULL values won't overwrite existing values
        apply_as_deletes: SQL expression for delete operations
        
    Returns:
        SCDType2Config for the table
        
    Example:
        config = create_generic_scd_config(
            table_name="employees",
            keys=["employee_id"],
            sequence_by="modified_ts",
            track_history_columns=["salary", "department", "title"]
        )
    """
    return SCDType2Config(
        target_name=f"{table_name}_silver",
        source_name=f"{table_name}_silver_source",
        keys=keys,
        sequence_by=sequence_by,
        track_history_columns=track_history_columns,
        track_history_except_columns=track_history_except_columns,
        ignore_null_updates=ignore_null_updates,
        apply_as_deletes=apply_as_deletes
    )


# =============================================================================
# Helper Functions
# =============================================================================

def create_scd_type2_target(
    name: str,
    comment: str,
    keys: List[str],
    sequence_by_type: str = "TIMESTAMP",
    additional_schema: Optional[str] = None
):
    """
    Create a streaming table target for SCD Type 2 with required columns.
    
    Args:
        name: Target table name
        comment: Table description
        keys: Primary key columns
        sequence_by_type: Data type of sequence_by column (TIMESTAMP, BIGINT, etc.)
        additional_schema: Optional additional schema definition
        
    Example:
        create_scd_type2_target(
            name="customers_silver",
            comment="Customer data with SCD Type 2 history",
            keys=["customer_id"],
            sequence_by_type="TIMESTAMP"
        )
    """
    # SCD Type 2 requires __START_AT and __END_AT columns
    # Schema will be inferred from source, but we need to ensure these columns exist
    dp.create_streaming_table(
        name=name,
        comment=comment
    )


def create_scd_type2_flow_with_quality_checks(
    scd_config: SCDType2Config,
    silver_config: Optional[SilverTableConfig] = None,
    source_view_name: Optional[str] = None
):
    """
    Create an SCD Type 2 flow with integrated data quality checks.
    
    This function:
    1. Creates a view with quality checks and transformations (if silver_config provided)
    2. Creates the target streaming table
    3. Creates the Auto CDC flow with SCD Type 2
    
    Args:
        scd_config: SCD Type 2 configuration
        silver_config: Optional silver table config for quality checks
        source_view_name: Optional name for the intermediate view (defaults to target_name + "_source")
        
    Example:
        scd_config = SCDType2Config(
            target_name="customers_silver",
            source_name="customers_bronze",
            keys=["customer_id"],
            sequence_by="ingestion_ts"
        )
        
        silver_config = create_standard_customer_config()
        
        create_scd_type2_flow_with_quality_checks(scd_config, silver_config)
    """
    # Create target streaming table
    create_scd_type2_target(
        name=scd_config.target_name,
        comment=f"SCD Type 2: {scd_config.target_name} with historical tracking",
        keys=scd_config.keys,
        sequence_by_type="TIMESTAMP"
    )
    
    # If silver config provided, create intermediate view with quality checks
    if silver_config:
        view_name = source_view_name or f"{scd_config.target_name}_with_quality"
        
        @dp.view(name=view_name)
        def quality_checked_source():
            # Read source
            source_df = spark.readStream.table(scd_config.source_name)
            
            # Apply quality checks and transformations
            return build_silver_table(
                source_df,
                silver_config,
                add_quality_flags=True,
                add_quality_score=True
            )
        
        # Use the quality-checked view as source
        actual_source = view_name
    else:
        actual_source = scd_config.source_name
    
    # Create Auto CDC flow with SCD Type 2
    dp.create_auto_cdc_flow(
        target=scd_config.target_name,
        source=actual_source,
        keys=scd_config.keys,
        sequence_by=scd_config.sequence_by,
        stored_as_scd_type=2,
        track_history_column_list=scd_config.track_history_columns,
        track_history_except_column_list=scd_config.track_history_except_columns,
        ignore_null_updates=scd_config.ignore_null_updates,
        apply_as_deletes=scd_config.apply_as_deletes
    )


def query_scd_type2_current(table_name: str) -> DataFrame:
    """
    Query current (active) records from an SCD Type 2 table.
    
    Args:
        table_name: Fully qualified SCD Type 2 table name
        
    Returns:
        DataFrame with only current records
        
    Example:
        current_customers = query_scd_type2_current("dev.experiment01.customers_silver")
    """
    return (
        spark.read.table(table_name)
        .filter(F.col("__END_AT").isNull())
    )


def query_scd_type2_as_of(table_name: str, as_of_timestamp) -> DataFrame:
    """
    Query SCD Type 2 table as of a specific point in time.
    
    Args:
        table_name: Fully qualified SCD Type 2 table name
        as_of_timestamp: Timestamp or date to query as of
        
    Returns:
        DataFrame with records valid at the specified time
        
    Example:
        historical = query_scd_type2_as_of(
            "dev.experiment01.customers_silver",
            "2024-01-01"
        )
    """
    return (
        spark.read.table(table_name)
        .filter(
            (F.col("__START_AT") <= as_of_timestamp) &
            ((F.col("__END_AT").isNull()) | (F.col("__END_AT") > as_of_timestamp))
        )
    )


def query_scd_type2_history(table_name: str, key_values: Dict[str, any]) -> DataFrame:
    """
    Query full history for specific record(s) in an SCD Type 2 table.
    
    Args:
        table_name: Fully qualified SCD Type 2 table name
        key_values: Dict of key column names and values
        
    Returns:
        DataFrame with full history ordered by __START_AT
        
    Example:
        history = query_scd_type2_history(
            "dev.experiment01.customers_silver",
            {"customer_id": 123}
        )
    """
    df = spark.read.table(table_name)
    
    # Apply filters for each key
    for key_col, key_val in key_values.items():
        df = df.filter(F.col(key_col) == key_val)
    
    return df.orderBy("__START_AT")
