"""
PII masking utilities for dynamic data protection.
Provides masking functions for different PII fields based on access levels.
"""

from pyspark.sql import Column
from pyspark.sql import functions as F


def mask_email(col: Column, access_level: Column) -> Column:
    """
    Mask email based on access level.
    
    Access levels:
    - full_access: user@domain.com
    - partial_access: u***@domain.com (mask prefix, keep domain)
    - masked_only: ***@***
    
    Args:
        col: Email column
        access_level: Access level column
        
    Returns:
        Masked email column
    """
    # Extract parts
    email_parts = F.split(col, "@")
    username = email_parts.getItem(0)
    domain = email_parts.getItem(1)
    
    # Partial mask: first char + *** + @ + domain
    partial_mask = F.concat(
        F.substring(username, 1, 1),
        F.lit("***@"),
        domain
    )
    
    return (
        F.when(access_level == "full_access", col)
        .when(access_level == "partial_access", partial_mask)
        .otherwise(F.lit("***@***"))
    )


def mask_phone(col: Column, access_level: Column) -> Column:
    """
    Mask phone number based on access level.
    
    Access levels:
    - full_access: +65 12345678
    - partial_access: +65 ****5678 (mask middle, keep last 4)
    - masked_only: ***
    
    Args:
        col: Phone column
        access_level: Access level column
        
    Returns:
        Masked phone column
    """
    # Keep last 4 digits for partial access
    partial_mask = F.concat(
        F.substring(col, 1, 4),  # Keep country code +65
        F.lit(" ****"),
        F.substring(col, -4, 4)  # Keep last 4 digits
    )
    
    return (
        F.when(access_level == "full_access", col)
        .when(access_level == "partial_access", partial_mask)
        .otherwise(F.lit("***"))
    )


def mask_nric(col: Column, access_level: Column) -> Column:
    """
    Mask NRIC based on access level.
    
    Access levels:
    - full_access: S1234567D
    - partial_access: S****567D (mask middle, keep first and last 3)
    - masked_only: ***
    
    Args:
        col: NRIC column
        access_level: Access level column
        
    Returns:
        Masked NRIC column
    """
    # Partial mask: first char + **** + last 3 chars
    partial_mask = F.concat(
        F.substring(col, 1, 1),
        F.lit("****"),
        F.substring(col, -3, 3)
    )
    
    return (
        F.when(access_level == "full_access", col)
        .when(access_level == "partial_access", partial_mask)
        .otherwise(F.lit("***"))
    )


def mask_address(col: Column, access_level: Column) -> Column:
    """
    Mask address based on access level.
    
    Access levels:
    - full_access: Full address
    - partial_access: Keep postal code only
    - masked_only: ***
    
    Args:
        col: Address column
        access_level: Access level column
        
    Returns:
        Masked address column
    """
    # Extract postal code (6 digits)
    postal_code = F.regexp_extract(col, r'\b(\d{6})\b', 1)
    partial_mask = F.concat(F.lit("*** Singapore "), postal_code)
    
    return (
        F.when(access_level == "full_access", col)
        .when(access_level == "partial_access", partial_mask)
        .otherwise(F.lit("***"))
    )


def mask_ssn(col: Column, access_level: Column) -> Column:
    """
    Mask SSN based on access level.
    
    Access levels:
    - full_access: 123-45-6789
    - partial_access: ***-**-6789 (mask all but last 4)
    - masked_only: ***
    
    Args:
        col: SSN column
        access_level: Access level column
        
    Returns:
        Masked SSN column
    """
    # Keep last 4 digits for partial access
    partial_mask = F.concat(
        F.lit("***-**-"),
        F.substring(col, -4, 4)
    )
    
    return (
        F.when(access_level == "full_access", col)
        .when(access_level == "partial_access", partial_mask)
        .otherwise(F.lit("***"))
    )


def get_user_access_level(access_grants_table: str = "dev.experiment01.pii_access_grants") -> Column:
    """
    Get the current user's access level by looking up in access grants table.
    
    Args:
        access_grants_table: Fully qualified name of access grants table
        
    Returns:
        Column expression for access level
        
    Usage in view:
        access_level = get_user_access_level()
        masked_email = mask_email(F.col("email"), access_level)
    """
    # This will be used in SQL views with a subquery
    # For now, return a placeholder that will be replaced in SQL
    return F.lit("masked_only")  # Default to most restrictive


def apply_pii_masking(df, access_level_col: str = "access_level"):
    """
    Apply PII masking to all sensitive columns in a DataFrame.
    
    Args:
        df: Input DataFrame
        access_level_col: Name of column containing access level
        
    Returns:
        DataFrame with PII columns masked
    """
    access_level = F.col(access_level_col)
    
    # Apply masking to each PII column if it exists
    if "email" in df.columns:
        df = df.withColumn("email", mask_email(F.col("email"), access_level))
    
    if "phone" in df.columns:
        df = df.withColumn("phone", mask_phone(F.col("phone"), access_level))
    
    if "nric" in df.columns:
        df = df.withColumn("nric", mask_nric(F.col("nric"), access_level))
    
    if "address" in df.columns:
        df = df.withColumn("address", mask_address(F.col("address"), access_level))
    
    if "ssn" in df.columns:
        df = df.withColumn("ssn", mask_ssn(F.col("ssn"), access_level))
    
    return df
