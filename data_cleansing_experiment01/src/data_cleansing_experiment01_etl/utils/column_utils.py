"""
Utility functions for standardizing column names across all ingestions.
This module provides reusable functions to normalize column names to lowercase,
trim whitespace, and replace spaces with underscores.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def normalize_column_name(col_name: str) -> str:
    """
    Normalize a single column name to standard format.
    
    Rules:
    - Strip leading/trailing whitespace
    - Convert to lowercase
    - Replace spaces with underscores
    - Replace multiple underscores with single underscore
    
    Args:
        col_name: Original column name
        
    Returns:
        Normalized column name
        
    Example:
        >>> normalize_column_name("  Customer Name  ")
        'customer_name'
        >>> normalize_column_name("Email Address")
        'email_address'
    """
    return col_name.strip().lower().replace(" ", "_").replace("__", "_")


def normalize_dataframe_columns(df: DataFrame) -> DataFrame:
    """
    Normalize all column names in a DataFrame.
    
    Args:
        df: Input DataFrame with original column names
        
    Returns:
        DataFrame with normalized column names
        
    Example:
        >>> df = normalize_dataframe_columns(raw_df)
    """
    for col_name in df.columns:
        normalized_name = normalize_column_name(col_name)
        df = df.withColumnRenamed(col_name, normalized_name)
    return df
