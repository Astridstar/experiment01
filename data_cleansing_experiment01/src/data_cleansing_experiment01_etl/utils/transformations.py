"""
Transformation utilities for data standardization and normalization.
Provides reusable transformation functions for cleaning and standardizing data.
"""

from pyspark.sql import Column
from pyspark.sql import functions as F


def standardize_singapore_postal_code(col: Column) -> Column:
    """
    Standardize Singapore postal code to 6-digit format with leading zeros.
    Handles formats: XXXXXX, 0XXXXX, 00XXXX
    
    Args:
        col: Column containing postal code values
        
    Returns:
        Column with standardized 6-digit postal code
        
    Example:
        df.withColumn("postal_code_clean", standardize_singapore_postal_code(F.col("postal_code")))
    """
    # Remove spaces and pad with leading zeros to 6 digits
    cleaned = F.regexp_replace(col, r'\s+', '')
    return F.when(
        cleaned.rlike(r'^\d+$') & (F.length(cleaned) <= 6),
        F.lpad(cleaned, 6, '0')
    ).otherwise(cleaned)


def standardize_nric(col: Column) -> Column:
    """
    Standardize NRIC to 9 alphanumeric characters, all uppercase.
    Format: 1 letter + 7 digits + 1 letter
    
    Args:
        col: Column containing NRIC values
        
    Returns:
        Column with standardized NRIC (uppercase, trimmed)
        
    Example:
        df.withColumn("nric_clean", standardize_nric(F.col("nric")))
    """
    return F.upper(F.trim(col))


def normalize_currency_code(col: Column) -> Column:
    """
    Normalize currency codes to standard 3-letter ISO codes.
    Mappings:
    - USD → USD
    - RMB/CNY → CNY
    - YEN/JPY → JPY
    - SGD → SGD
    
    Args:
        col: Column containing currency values
        
    Returns:
        Column with normalized currency code
        
    Example:
        df.withColumn("currency_clean", normalize_currency_code(F.col("currency")))
    """
    upper_col = F.upper(F.trim(col))
    return (
        F.when(upper_col.isin(['USD']), 'USD')
        .when(upper_col.isin(['RMB', 'CNY']), 'CNY')
        .when(upper_col.isin(['YEN', 'JPY']), 'JPY')
        .when(upper_col.isin(['SGD']), 'SGD')
        .otherwise(upper_col)
    )


def normalize_nationality_code(col: Column) -> Column:
    """
    Normalize nationality/country codes to ISO 2-letter codes (uppercase).
    Mappings:
    - USA/US → US
    - UK/GB → GB
    - SG → SG
    - CN/China → CN
    - TW/Taiwan → TW
    - FR/France → FR
    - DK/Denmark → DK
    
    Args:
        col: Column containing nationality/country values
        
    Returns:
        Column with normalized 2-letter country code (uppercase)
        
    Example:
        df.withColumn("country_clean", normalize_nationality_code(F.col("country")))
    """
    upper_col = F.upper(F.trim(col))
    return (
        F.when(upper_col.isin(['USA', 'US']), 'US')
        .when(upper_col.isin(['UK', 'GB']), 'GB')
        .when(upper_col.isin(['SG', 'SINGAPORE']), 'SG')
        .when(upper_col.isin(['CN', 'CHINA']), 'CN')
        .when(upper_col.isin(['TW', 'TAIWAN']), 'TW')
        .when(upper_col.isin(['FR', 'FRANCE']), 'FR')
        .when(upper_col.isin(['DK', 'DENMARK']), 'DK')
        .otherwise(upper_col)
    )


def normalize_gender(col: Column) -> Column:
    """
    Normalize gender to uppercase single letter.
    Valid values: M, F, X
    
    Args:
        col: Column containing gender values
        
    Returns:
        Column with normalized gender (uppercase)
        
    Example:
        df.withColumn("gender_clean", normalize_gender(F.col("gender")))
    """
    upper_col = F.upper(F.trim(col))
    return F.when(
        upper_col.isin(['M', 'F', 'X']),
        upper_col
    ).otherwise(None)


def normalize_name(col: Column) -> Column:
    """
    Normalize name to uppercase and trim whitespace.
    
    Args:
        col: Column containing name values
        
    Returns:
        Column with normalized name (uppercase, trimmed)
        
    Example:
        df.withColumn("name_clean", normalize_name(F.col("full_name")))
    """
    return F.upper(F.trim(col))


def extract_postal_code_from_address(col: Column) -> Column:
    """
    Extract Singapore postal code from address string.
    Looks for 6-digit patterns in the address.
    
    Args:
        col: Column containing address values
        
    Returns:
        Column with extracted postal code
        
    Example:
        df.withColumn("postal_code", extract_postal_code_from_address(F.col("address")))
    """
    # Extract 6-digit postal code pattern
    return F.regexp_extract(col, r'\b(\d{6})\b', 1)


def standardize_phone_number(col: Column) -> Column:
    """
    Standardize phone number to Singapore format (+65 XXXXXXXX).
    Handles various input formats.
    
    Args:
        col: Column containing phone number values
        
    Returns:
        Column with standardized phone number
        
    Example:
        df.withColumn("phone_clean", standardize_phone_number(F.col("phone")))
    """
    # Remove all non-digit characters except +
    cleaned = F.regexp_replace(col, r'[^\d+]', '')
    
    # Standardize to +65 format
    return (
        F.when(cleaned.startswith('+65'), cleaned)
        .when(cleaned.startswith('65'), F.concat(F.lit('+'), cleaned))
        .when(cleaned.startswith('0'), F.concat(F.lit('+65'), F.substring(cleaned, 2, 100)))
        .when(F.length(cleaned) == 8, F.concat(F.lit('+65'), cleaned))
        .otherwise(cleaned)
    )


def fill_null_with_none_string(col: Column) -> Column:
    """
    Replace null values with string 'None'.
    
    Args:
        col: Column to process
        
    Returns:
        Column with nulls replaced by 'None'
        
    Example:
        df.withColumn("field_clean", fill_null_with_none_string(F.col("field")))
    """
    return F.when(col.isNull(), F.lit('None')).otherwise(col)
