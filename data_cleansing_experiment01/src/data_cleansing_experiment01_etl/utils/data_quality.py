"""
Data quality utilities for flagging and tracking invalid values.
Provides functions to identify, flag, and track data quality issues.
"""

from pyspark.sql import DataFrame, Column
from pyspark.sql import functions as F
from typing import Dict, List


def flag_invalid_values(
    df: DataFrame,
    validation_rules: Dict[str, Column],
    flag_column: str = "data_quality_flags"
) -> DataFrame:
    """
    Flag records with invalid values based on validation rules.
    Creates a column with comma-separated list of failed validations.
    
    Args:
        df: Input DataFrame
        validation_rules: Dict mapping validation names to boolean Column expressions
                         Example: {"valid_email": validate_email(F.col("email"))}
        flag_column: Name of the column to store quality flags
        
    Returns:
        DataFrame with quality flag column added
        
    Example:
        rules = {
            "valid_email": validate_email(F.col("email")),
            "valid_nric": validate_singapore_nric(F.col("nric"))
        }
        df = flag_invalid_values(df, rules)
    """
    # Build array of failed validation names
    failed_validations = []
    
    for validation_name, validation_expr in validation_rules.items():
        # Add validation name to array if validation fails
        failed_validations.append(
            F.when(~validation_expr, F.lit(validation_name))
        )
    
    # Combine all failed validations into comma-separated string
    flags_array = F.array(*failed_validations)
    flags_string = F.array_join(F.array_compact(flags_array), ", ")
    
    # Add flag column (null if no failures, otherwise comma-separated list)
    return df.withColumn(
        flag_column,
        F.when(flags_string != "", flags_string).otherwise(None)
    )


def add_quality_score(
    df: DataFrame,
    validation_rules: Dict[str, Column],
    score_column: str = "quality_score"
) -> DataFrame:
    """
    Calculate quality score as percentage of passed validations.
    
    Args:
        df: Input DataFrame
        validation_rules: Dict mapping validation names to boolean Column expressions
        score_column: Name of the column to store quality score
        
    Returns:
        DataFrame with quality score column (0-100)
        
    Example:
        rules = {
            "valid_email": validate_email(F.col("email")),
            "valid_nric": validate_singapore_nric(F.col("nric"))
        }
        df = add_quality_score(df, rules)
    """
    total_checks = len(validation_rules)
    
    # Count passed validations
    passed_count = F.lit(0)
    for validation_expr in validation_rules.values():
        passed_count = passed_count + F.when(validation_expr, 1).otherwise(0)
    
    # Calculate percentage
    score = (passed_count / total_checks) * 100
    
    return df.withColumn(score_column, F.round(score, 2))


def fill_nulls_with_none(df: DataFrame, columns: List[str]) -> DataFrame:
    """
    Replace null values with string 'None' for specified columns.
    
    Args:
        df: Input DataFrame
        columns: List of column names to process
        
    Returns:
        DataFrame with nulls replaced by 'None'
        
    Example:
        df = fill_nulls_with_none(df, ["email", "phone", "address"])
    """
    for col_name in columns:
        if col_name in df.columns:
            df = df.withColumn(
                col_name,
                F.when(F.col(col_name).isNull(), F.lit('None')).otherwise(F.col(col_name))
            )
    return df


def add_validation_columns(
    df: DataFrame,
    validation_rules: Dict[str, Column],
    prefix: str = "is_valid_"
) -> DataFrame:
    """
    Add individual validation result columns for each rule.
    
    Args:
        df: Input DataFrame
        validation_rules: Dict mapping validation names to boolean Column expressions
        prefix: Prefix for validation column names
        
    Returns:
        DataFrame with validation columns added
        
    Example:
        rules = {
            "email": validate_email(F.col("email")),
            "nric": validate_singapore_nric(F.col("nric"))
        }
        df = add_validation_columns(df, rules)
        # Creates columns: is_valid_email, is_valid_nric
    """
    for validation_name, validation_expr in validation_rules.items():
        col_name = f"{prefix}{validation_name}"
        df = df.withColumn(col_name, validation_expr)
    
    return df


def create_quality_summary(
    df: DataFrame,
    validation_rules: Dict[str, Column]
) -> DataFrame:
    """
    Create a summary DataFrame with quality metrics for each validation rule.
    Useful for monitoring data quality across the pipeline.
    
    Args:
        df: Input DataFrame
        validation_rules: Dict mapping validation names to boolean Column expressions
        
    Returns:
        Summary DataFrame with validation statistics
        
    Example:
        rules = {
            "valid_email": validate_email(F.col("email")),
            "valid_nric": validate_singapore_nric(F.col("nric"))
        }
        summary = create_quality_summary(df, rules)
    """
    # This would typically be used in a separate monitoring table
    # For now, return the input df with validation columns
    return add_validation_columns(df, validation_rules)
