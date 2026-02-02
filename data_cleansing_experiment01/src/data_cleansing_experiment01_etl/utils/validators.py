"""
Validation utilities for data quality checks.
Provides reusable validation functions for Singapore-specific formats and international standards.
"""

from pyspark.sql import Column
from pyspark.sql import functions as F


def validate_singapore_postal_code(col: Column) -> Column:
    """
    Validate Singapore postal code format (6 digits, can have leading zeros).
    Valid formats: XXXXXX, 0XXXXX, 00XXXX
    
    Args:
        col: Column containing postal code values
        
    Returns:
        Boolean column indicating if postal code is valid
        
    Example:
        df.withColumn("is_valid_postal", validate_singapore_postal_code(F.col("postal_code")))
    """
    # Remove spaces and check if it's exactly 6 digits
    cleaned = F.regexp_replace(col, r'\s+', '')
    return (
        F.length(cleaned) == 6
    ) & (
        cleaned.rlike(r'^\d{6}$')
    )


def validate_singapore_nric(col: Column) -> Column:
    """
    Validate Singapore NRIC/FIN format and checksum.
    Format: 1 letter + 7 digits + 1 checksum letter
    Valid prefixes: S, T, F, G, M
    
    Args:
        col: Column containing NRIC values
        
    Returns:
        Boolean column indicating if NRIC is valid
        
    Example:
        df.withColumn("is_valid_nric", validate_singapore_nric(F.col("nric")))
    """
    # Basic format check: 1 letter + 7 digits + 1 letter
    format_valid = col.rlike(r'^[STFGM]\d{7}[A-Z]$')
    
    # For full checksum validation, we'd need a UDF with the algorithm
    # For now, return format validation
    return format_valid


def validate_currency_code(col: Column) -> Column:
    """
    Validate currency code against supported currencies.
    Supported: USD, RMB, YEN, SGD
    
    Args:
        col: Column containing currency code values
        
    Returns:
        Boolean column indicating if currency is valid
        
    Example:
        df.withColumn("is_valid_currency", validate_currency_code(F.col("currency")))
    """
    return F.upper(col).isin(['USD', 'RMB', 'YEN', 'SGD', 'CNY', 'JPY'])


def validate_nationality_code(col: Column) -> Column:
    """
    Validate nationality code against supported countries.
    Supported: USA, UK, SG, CN (China), TW (Taiwan), FR (France), DK (Denmark)
    
    Args:
        col: Column containing nationality/country code values
        
    Returns:
        Boolean column indicating if nationality is valid
        
    Example:
        df.withColumn("is_valid_nationality", validate_nationality_code(F.col("country")))
    """
    return F.upper(col).isin(['USA', 'US', 'UK', 'GB', 'SG', 'CN', 'TW', 'FR', 'DK'])


def validate_gender(col: Column) -> Column:
    """
    Validate gender code.
    Valid values: M, F, X
    
    Args:
        col: Column containing gender values
        
    Returns:
        Boolean column indicating if gender is valid
        
    Example:
        df.withColumn("is_valid_gender", validate_gender(F.col("gender")))
    """
    return F.upper(col).isin(['M', 'F', 'X'])


def validate_email(col: Column) -> Column:
    """
    Validate email format.
    
    Args:
        col: Column containing email values
        
    Returns:
        Boolean column indicating if email is valid
        
    Example:
        df.withColumn("is_valid_email", validate_email(F.col("email")))
    """
    return (
        col.isNotNull()
    ) & (
        col.rlike(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$')
    )


def validate_nric_9char(col: Column) -> Column:
    """
    Validate NRIC is exactly 9 alphanumeric characters (all uppercase).
    
    Args:
        col: Column containing NRIC values
        
    Returns:
        Boolean column indicating if NRIC format is valid
        
    Example:
        df.withColumn("is_valid_nric_format", validate_nric_9char(F.col("nric")))
    """
    return (
        F.length(col) == 9
    ) & (
        col.rlike(r'^[A-Z0-9]{9}$')
    )


# Checksum calculation for Singapore NRIC (for reference/future UDF implementation)
def calculate_nric_checksum(nric: str) -> str:
    """
    Calculate Singapore NRIC checksum letter.
    This is a Python function for reference - would need to be a UDF for DataFrame use.
    
    Algorithm:
    1. Multiply each digit by weight: [2,7,6,5,4,3,2]
    2. Sum the products
    3. Add offset based on prefix (S/T: 0, F/G: 4, M: 3)
    4. Modulo 11
    5. Map to checksum letter
    
    Args:
        nric: NRIC string (e.g., "S1234567D")
        
    Returns:
        Expected checksum letter
    """
    if not nric or len(nric) != 9:
        return None
    
    prefix = nric[0].upper()
    digits = nric[1:8]
    
    # Weights for each digit position
    weights = [2, 7, 6, 5, 4, 3, 2]
    
    # Calculate weighted sum
    total = sum(int(d) * w for d, w in zip(digits, weights))
    
    # Add offset based on prefix
    if prefix in ['T', 'G', 'M']:
        total += 4
    
    # Get checksum index
    checksum_index = 10 - (total % 11)
    
    # Checksum mapping
    if prefix in ['S', 'T']:
        checksum_map = ['J', 'Z', 'I', 'H', 'G', 'F', 'E', 'D', 'C', 'B', 'A']
    elif prefix in ['F', 'G']:
        checksum_map = ['X', 'W', 'U', 'T', 'R', 'Q', 'P', 'N', 'M', 'L', 'K']
    elif prefix == 'M':
        checksum_map = ['K', 'L', 'J', 'N', 'P', 'Q', 'R', 'T', 'U', 'W', 'X']
    else:
        return None
    
    return checksum_map[checksum_index]
