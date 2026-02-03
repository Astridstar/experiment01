"""
Gold layer: Customers materialized view with dynamic PII masking based on user access.
Demonstrates temporal access control for 500 users across 400 tables.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from utils.pii_masking import mask_email, mask_phone, mask_nric, mask_address


@dp.materialized_view(
    name="customers_gold",
    comment="Gold layer: Customer data with dynamic PII masking based on temporal access grants"
)
def customers_gold():
    """
    Materialized view that masks PII based on current user's access level at refresh time.
    
    Access Levels:
    - full_access: All PII visible (governance_officers, approved requests)
    - partial_access: Partial masking (scientists, engineers with approval)
    - masked_only: Full masking (analysts, managers, default)
    
    PII Masking Rules:
    - Email: Mask prefix (u***@domain.com) or full (***@***)
    - Phone: Mask middle (+65 ****5678) or full (***)
    - NRIC: Mask middle (S****567D) or full (***)
    - Address: Keep postal code only or full (***)
    - SSN: Mask all but last 4 (***-**-6789) or full (***)
    
    Access is temporal and managed via Databricks App approval workflow.
    
    NOTE: Masking is applied at PIPELINE REFRESH TIME based on the pipeline owner's access level.
    All users will see the same masked data. For true per-user dynamic masking, query the
    SQL view 'customers_gold_uc' instead.
    """
    
    # Read silver layer
    silver_df = spark.read.table("dev.experiment01.customers_silver")
    
    # Read access grants table
    access_grants = spark.read.table("dev.experiment01.pii_access_grants")
    
    # Get current user (pipeline owner at refresh time)
    current_user = F.current_user()
    
    # Filter active grants for current user
    user_access = (
        access_grants
        .filter(F.col("user_email") == current_user)
        .filter(F.col("is_active") == True)
        .filter(
            (F.col("expires_at").isNull()) | 
            (F.col("expires_at") > F.current_timestamp())
        )
        .select("access_level")
        .limit(1)
    )
    
    # Join with silver data to get access level for each row
    # Use cross join since we're getting a single access level value
    result_df = silver_df.crossJoin(
        user_access.withColumnRenamed("access_level", "user_access_level")
    )
    
    # Default to masked_only if no access grant found
    result_df = result_df.withColumn(
        "access_level",
        F.coalesce(F.col("user_access_level"), F.lit("masked_only"))
    )
    
    # Apply PII masking based on access level
    result_df = result_df.withColumn(
        "email",
        mask_email(F.col("email"), F.col("access_level"))
    ).withColumn(
        "phone",
        mask_phone(F.col("phone"), F.col("access_level"))
    ).withColumn(
        "nric",
        mask_nric(F.col("nric"), F.col("access_level"))
    ).withColumn(
        "address",
        mask_address(F.col("address"), F.col("access_level"))
    )
    
    # Add masking metadata
    result_df = result_df.withColumn("masked_at", F.current_timestamp())
    result_df = result_df.withColumn("masked_for_user", current_user)
    
    # Select final columns (exclude internal access_level column)
    return result_df.select(
        "customer_id",
        "full_name",
        "email",
        "phone",
        "nric",
        "dob",
        "address",
        "postal_code",
        "country",
        "gender",
        "signup_ts",
        "last_login_ts",
        "status",
        "segment",
        "credit_score",
        "annual_income",
        "data_quality_flags",
        "quality_score",
        "masked_at",
        "masked_for_user"
    )
