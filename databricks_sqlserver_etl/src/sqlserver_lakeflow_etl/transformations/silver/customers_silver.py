"""
Silver Layer: Cleansed and validated customers from SQL Server bronze tables.

Reads from bronze_customers (Lakeflow Connect replica) and applies:
  - Column name standardisation (snake_case)
  - Data type casting
  - Quality validation rules
  - Deduplication
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from utils.quality_checks import add_quality_flags, add_quality_score


@dp.table(
    name="silver_customers",
    comment="Silver: Cleansed customers with quality checks from SQL Server source",
)
@dp.expect_all_or_drop(
    {
        "valid_customer_id": "customer_id IS NOT NULL",
        "valid_email_format": "email RLIKE '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\\\.[A-Za-z]{2,}$'",
    }
)
def silver_customers():
    bronze_df = spark.read.table("bronze_customers")

    # Standardise column names to snake_case
    df = (
        bronze_df
        .withColumnRenamed("CustomerID", "customer_id")
        .withColumnRenamed("FirstName", "first_name")
        .withColumnRenamed("LastName", "last_name")
        .withColumnRenamed("Email", "email")
        .withColumnRenamed("Phone", "phone")
        .withColumnRenamed("Address", "address")
        .withColumnRenamed("City", "city")
        .withColumnRenamed("State", "state")
        .withColumnRenamed("ZipCode", "zip_code")
        .withColumnRenamed("Country", "country")
        .withColumnRenamed("CreatedDate", "created_date")
        .withColumnRenamed("ModifiedDate", "modified_date")
    )

    # Derive full_name
    df = df.withColumn(
        "full_name",
        F.concat_ws(" ", F.initcap("first_name"), F.initcap("last_name")),
    )

    # Trim whitespace from string columns
    for col_name in ["email", "phone", "address", "city", "state", "zip_code"]:
        df = df.withColumn(col_name, F.trim(F.col(col_name)))

    # Normalise email to lowercase
    df = df.withColumn("email", F.lower("email"))

    # Quality rules
    quality_rules = {
        "missing_email": "email IS NOT NULL AND email != ''",
        "missing_phone": "phone IS NOT NULL AND phone != ''",
        "missing_address": "address IS NOT NULL AND address != ''",
        "invalid_zip": "zip_code RLIKE '^[0-9]{5}(-[0-9]{4})?$'",
        "missing_name": "first_name IS NOT NULL AND last_name IS NOT NULL",
    }
    df = add_quality_flags(df, quality_rules)
    df = add_quality_score(df)

    # Add silver metadata
    df = df.withColumn("silver_processed_ts", F.current_timestamp())

    return df
