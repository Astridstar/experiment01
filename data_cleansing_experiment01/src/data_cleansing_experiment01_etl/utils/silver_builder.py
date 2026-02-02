"""
Silver table builder framework for scalable data quality and transformation.
Provides configuration-driven approach to building 400+ silver tables consistently.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from typing import Dict, List, Callable, Optional
from utils import validators, transformations, data_quality


class SilverTableConfig:
    """
    Configuration class for silver table transformations and validations.
    """
    
    def __init__(self, source_prefix: Optional[str] = None):
        """
        Initialize silver table configuration.
        
        Args:
            source_prefix: Optional prefix to add to all source column names
                          (e.g., "customer_" for future ingestions)
        """
        self.source_prefix = source_prefix
        self.transformations = {}
        self.validations = {}
        self.null_fill_columns = []
        self.uppercase_columns = []
        
    def add_transformation(self, column: str, transform_func: Callable) -> 'SilverTableConfig':
        """
        Add a transformation for a specific column.
        
        Args:
            column: Column name to transform
            transform_func: Function that takes a Column and returns transformed Column
            
        Returns:
            Self for method chaining
        """
        self.transformations[column] = transform_func
        return self
    
    def add_validation(self, name: str, validation_func: Callable) -> 'SilverTableConfig':
        """
        Add a validation rule.
        
        Args:
            name: Name of the validation (e.g., "email", "nric")
            validation_func: Function that takes a Column and returns boolean Column
            
        Returns:
            Self for method chaining
        """
        self.validations[name] = validation_func
        return self
    
    def fill_nulls(self, columns: List[str]) -> 'SilverTableConfig':
        """
        Mark columns to fill nulls with 'None' string.
        
        Args:
            columns: List of column names
            
        Returns:
            Self for method chaining
        """
        self.null_fill_columns.extend(columns)
        return self
    
    def to_uppercase(self, columns: List[str]) -> 'SilverTableConfig':
        """
        Mark columns to convert to uppercase.
        
        Args:
            columns: List of column names
            
        Returns:
            Self for method chaining
        """
        self.uppercase_columns.extend(columns)
        return self
    

def build_silver_table(
    df: DataFrame,
    config: SilverTableConfig,
    add_quality_flags: bool = True,
    add_quality_score: bool = True
) -> DataFrame:
    """
    Build a silver table by applying transformations and validations from config.
    
    Args:
        df: Input bronze DataFrame
        config: SilverTableConfig with transformations and validations
        add_quality_flags: Whether to add data_quality_flags column
        add_quality_score: Whether to add quality_score column
        
    Returns:
        Transformed silver DataFrame
        
    Example:
        config = SilverTableConfig()
        config.add_transformation("nric", transformations.standardize_nric)
        config.add_validation("nric", validators.validate_singapore_nric)
        silver_df = build_silver_table(bronze_df, config)
    """
    result_df = df
    
    # Apply prefix to source columns if specified
    if config.source_prefix:
        for col_name in result_df.columns:
            if not col_name.startswith(config.source_prefix):
                result_df = result_df.withColumnRenamed(
                    col_name,
                    f"{config.source_prefix}{col_name}"
                )
    
    # Apply transformations
    for col_name, transform_func in config.transformations.items():
        if col_name in result_df.columns:
            result_df = result_df.withColumn(
                col_name,
                transform_func(F.col(col_name))
            )
    
    # Apply uppercase transformations
    for col_name in config.uppercase_columns:
        if col_name in result_df.columns:
            result_df = result_df.withColumn(
                col_name,
                F.upper(F.trim(F.col(col_name)))
            )
    
    # Fill nulls with 'None'
    if config.null_fill_columns:
        result_df = data_quality.fill_nulls_with_none(result_df, config.null_fill_columns)
    
    # Build validation rules dictionary
    validation_rules = {}
    for validation_name, validation_func in config.validations.items():
        col_name = validation_name
        if col_name in result_df.columns:
            validation_rules[validation_name] = validation_func(F.col(col_name))
    
    # Add quality flags
    if add_quality_flags and validation_rules:
        result_df = data_quality.flag_invalid_values(result_df, validation_rules)
    
    # Add quality score
    if add_quality_score and validation_rules:
        result_df = data_quality.add_quality_score(result_df, validation_rules)
    
    return result_df


def create_standard_customer_config() -> SilverTableConfig:
    """
    Create standard configuration for customer tables.
    Includes common transformations and validations for customer data.
    
    Returns:
        Pre-configured SilverTableConfig for customer tables
        
    Example:
        config = create_standard_customer_config()
        silver_df = build_silver_table(bronze_df, config)
    """
    config = SilverTableConfig()
    
    # Transformations
    config.add_transformation("nric", transformations.standardize_nric)
    config.add_transformation("gender", transformations.normalize_gender)
    config.add_transformation("country", transformations.normalize_nationality_code)
    config.add_transformation("phone", transformations.standardize_phone_number)
    
    # Validations
    config.add_validation("nric", validators.validate_singapore_nric)
    config.add_validation("email", validators.validate_email)
    config.add_validation("gender", validators.validate_gender)
    config.add_validation("country", validators.validate_nationality_code)
    
    # Uppercase fields
    config.to_uppercase(["full_name", "nric", "gender", "country"])
    
    # Fill nulls
    config.fill_nulls(["email", "phone", "address"])
    
    return config


def extract_postal_code_and_validate(df: DataFrame, address_col: str = "address") -> DataFrame:
    """
    Extract postal code from address and validate it.
    Adds postal_code and is_valid_postal_code columns.
    
    Args:
        df: Input DataFrame
        address_col: Name of the address column
        
    Returns:
        DataFrame with postal_code extracted and validated
        
    Example:
        df = extract_postal_code_and_validate(df, "address")
    """
    # Extract postal code
    df = df.withColumn(
        "postal_code",
        transformations.extract_postal_code_from_address(F.col(address_col))
    )
    
    # Standardize postal code
    df = df.withColumn(
        "postal_code",
        transformations.standardize_singapore_postal_code(F.col("postal_code"))
    )
    
    # Validate postal code
    df = df.withColumn(
        "is_valid_postal_code",
        validators.validate_singapore_postal_code(F.col("postal_code"))
    )
    
    return df
