"""
Access control table for managing temporal PII access grants.
This table is managed by Databricks Apps/Workflows for approval-based access.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, BooleanType
from datetime import datetime


@dp.materialized_view(
    name="pii_access_grants",
    comment="Temporal access grants for PII data - managed by approval workflow"
)
def pii_access_grants():
    """
    Access control table tracking who has access to unmasked PII data.
    
    Schema:
    - user_email: User's email address
    - user_group: User's role (analyst, scientist, engineer, manager, governance_officer)
    - access_level: full_access, partial_access, masked_only
    - granted_by: Who approved the access
    - granted_at: When access was granted
    - expires_at: When access expires (NULL for permanent)
    - is_active: Whether access is currently active
    - reason: Business justification for access
    - approval_ticket_id: Reference to approval workflow
    
    Managed by: Databricks App for PII Access Management
    """
    
    # Define schema for access control
    schema = StructType([
        StructField("user_email", StringType(), False),
        StructField("user_group", StringType(), False),
        StructField("access_level", StringType(), False),
        StructField("granted_by", StringType(), False),
        StructField("granted_at", TimestampType(), False),
        StructField("expires_at", TimestampType(), True),
        StructField("is_active", BooleanType(), False),
        StructField("reason", StringType(), True),
        StructField("approval_ticket_id", StringType(), True)
    ])
    
    # Get current timestamp as Python datetime
    now = datetime.now()
    
    # Initialize with default access levels for user groups
    # This will be managed by Databricks App - this is just the initial state
    default_access = [
        ("analyst@company.com", "analyst", "masked_only", "system", now, None, True, "Default group access", None),
        ("scientist@company.com", "scientist", "partial_access", "system", now, None, True, "Default group access", None),
        ("governance@company.com", "governance_officer", "full_access", "system", now, None, True, "Default group access", None),
    ]
    
    return spark.createDataFrame(default_access, schema)
