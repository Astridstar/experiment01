"""
Test file to validate column normalization utility functions.
This creates a test table with various column name issues to verify normalization works correctly.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from utils.column_utils import normalize_dataframe_columns, normalize_column_name


@dp.table(
    name="test_column_normalization",
    comment="Test table to validate column normalization utility"
)
def test_column_normalization():
    """
    Creates a test DataFrame with problematic column names and normalizes them.
    
    Test cases:
    - Leading/trailing spaces: "  Customer ID  " -> "customer_id"
    - Mixed case: "Full Name" -> "full_name"
    - Multiple spaces: "Email  Address" -> "email_address"
    - Already normalized: "phone" -> "phone"
    - Special characters with spaces: "Date Of Birth" -> "date_of_birth"
    """
    
    # Create test data with problematic column names
    test_data = [
        (1, "John Doe", "john@example.com", "+65 12345678", "1990-01-01"),
        (2, "Jane Smith", "jane@example.com", "+65 87654321", "1985-05-15"),
        (3, "Bob Lee", "bob@example.com", "+65 11223344", "1992-12-30")
    ]
    
    # Define schema with problematic column names
    schema = StructType([
        StructField("  Customer ID  ", IntegerType(), True),  # Leading/trailing spaces
        StructField("Full Name", StringType(), True),         # Space in name
        StructField("Email  Address", StringType(), True),    # Multiple spaces
        StructField("phone", StringType(), True),             # Already normalized
        StructField("Date Of Birth", StringType(), True)      # Multiple words
    ])
    
    # Create DataFrame with problematic column names
    df = spark.createDataFrame(test_data, schema)
    
    # Apply normalization
    normalized_df = normalize_dataframe_columns(df)
    
    # Add metadata for verification
    result_df = normalized_df.withColumn("test_timestamp", F.current_timestamp())
    
    return result_df


# Test individual function
print("Testing normalize_column_name function:")
print(f"'  Customer ID  ' -> '{normalize_column_name('  Customer ID  ')}'")
print(f"'Full Name' -> '{normalize_column_name('Full Name')}'")
print(f"'Email  Address' -> '{normalize_column_name('Email  Address')}'")
print(f"'phone' -> '{normalize_column_name('phone')}'")
print(f"'Date Of Birth' -> '{normalize_column_name('Date Of Birth')}'")
