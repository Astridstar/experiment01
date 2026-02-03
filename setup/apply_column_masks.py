"""
Script to apply column masks to all silver tables.
Run this after creating masking UDFs and after silver tables are created.

This script generates ALTER TABLE statements to apply column masks to PII fields
across all 400 silver tables efficiently.
"""

# Configuration: Define which columns need masking for each table pattern
MASKING_CONFIG = {
    "customers": {
        "email": "dev.experiment01.mask_email",
        "phone": "dev.experiment01.mask_phone",
        "nric": "dev.experiment01.mask_nric",
        "address": "dev.experiment01.mask_address USING COLUMNS (postal_code)"
    }
    # Add more table patterns as needed
}

# List of all silver tables (example - replace with your actual table list)
SILVER_TABLES = [
    "dev.experiment01.customers_silver"
]


def generate_alter_statements(table_name: str, masking_config: dict) -> list:
    """
    Generate ALTER TABLE statements to apply column masks.
    
    Args:
        table_name: Fully qualified table name (catalog.schema.table)
        masking_config: Dict mapping column names to masking functions
        
    Returns:
        List of ALTER TABLE SQL statements
    """
    statements = []
    
    for column, mask_function in masking_config.items():
        statement = f"""
ALTER TABLE {table_name} 
  ALTER COLUMN {column} SET MASK {mask_function};
"""
        statements.append(statement)
    
    return statements


def get_table_pattern(table_name: str) -> str:
    """
    Extract table pattern from full table name.
    Example: dev.experiment01.customers_silver -> customers
    """
    base_name = table_name.split(".")[-1]  # Get table name without catalog/schema
    if "_silver" in base_name:
        return base_name.replace("_silver", "")
    return base_name


def generate_all_alter_statements() -> str:
    """
    Generate ALTER TABLE statements for all silver tables.
    
    Returns:
        SQL script with all ALTER statements
    """
    all_statements = []
    
    all_statements.append("-- =============================================================================")
    all_statements.append("-- Apply Column Masks to All Silver Tables")
    all_statements.append("-- Generated automatically - review before executing")
    all_statements.append("-- =============================================================================\n")
    
    for table_name in SILVER_TABLES:
        # Determine which masking config to use based on table pattern
        table_pattern = get_table_pattern(table_name)
        
        if table_pattern in MASKING_CONFIG:
            config = MASKING_CONFIG[table_pattern]
            
            all_statements.append(f"\n-- Apply masks to {table_name}")
            statements = generate_alter_statements(table_name, config)
            all_statements.extend(statements)
        else:
            all_statements.append(f"\n-- WARNING: No masking config found for {table_name}")
    
    return "\n".join(all_statements)


def apply_masks_to_table(spark, table_name: str, masking_config: dict):
    """
    Apply column masks to a single table using Spark SQL.
    
    Args:
        spark: SparkSession
        table_name: Fully qualified table name
        masking_config: Dict mapping column names to masking functions
    """
    for column, mask_function in masking_config.items():
        try:
            sql = f"ALTER TABLE {table_name} ALTER COLUMN {column} SET MASK {mask_function}"
            spark.sql(sql)
            print(f"✓ Applied mask to {table_name}.{column}")
        except Exception as e:
            print(f"✗ Failed to apply mask to {table_name}.{column}: {str(e)}")


def apply_all_masks(spark):
    """
    Apply column masks to all silver tables.
    
    Args:
        spark: SparkSession
    """
    print("Starting to apply column masks to all silver tables...")
    print(f"Total tables to process: {len(SILVER_TABLES)}\n")

    from databricks import sql
    import os

    server_hostname = dbutils.secrets.get("data_cleansing_scope", "DBC_SQL_HOST")
    http_path = dbutils.secrets.get("data_cleansing_scope", "DBC_SQL_HTTP_PATH")
    access_token = dbutils.secrets.get("data_cleansing_scope", "DBC_SQL_TOKEN")

    # SQL Warehouse connection
    connection = sql.connect(
        server_hostname = f"${server_hostname}",
        http_path = f"${http_path}",  # Replace with your warehouse ID
        access_token = f"${access_token}"
    )

    cursor = connection.cursor()

    # Apply masks
    for table_name in SILVER_TABLES:
        table_pattern = get_table_pattern(table_name)
        
        if table_pattern in MASKING_CONFIG:
            config = MASKING_CONFIG[table_pattern]
            print(f"\nProcessing {table_name}...")
            
            for column, mask_function in config.items():
                try:
                    sql_stmt = f"ALTER TABLE {table_name} ALTER COLUMN {column} SET MASK {mask_function}"
                    cursor.execute(sql_stmt)
                    print(f"✓ Applied mask to {table_name}.{column}")
                except Exception as e:
                    print(f"✗ Failed to apply mask to {table_name}.{column}: {str(e)}")

    cursor.close()
    connection.close()
    print("\n✓ Finished applying column masks!")

# =============================================================================
# Usage Examples
# =============================================================================

# Option 1: Generate SQL script to review and run manually
if __name__ == "__main__":
    sql_script = generate_all_alter_statements()
    
    # Print to console
    print(sql_script)
    
    # Or save to file
    with open("/tmp/apply_column_masks.sql", "w") as f:
        f.write(sql_script)
    print("\n✓ SQL script saved to /tmp/apply_column_masks.sql")

# Option 2: Apply masks directly using Spark (run in notebook)
# from setup.apply_column_masks import apply_all_masks
# apply_all_masks(spark)

# Option 3: Apply masks to a single table
# from setup.apply_column_masks import apply_masks_to_table, MASKING_CONFIG
# apply_masks_to_table(spark, "dev.experiment01.customers_silver", MASKING_CONFIG["customers"])
