"""
Bronze Layer: Lakeflow Connect ingestion from MS SQL Server.

Uses Databricks Lakeflow Connect to ingest tables from MS SQL Server
via a Unity Catalog connection configured through Databricks SDP.

Prerequisites:
  1. A Unity Catalog connection of type 'sqlserver' must be created:
       CREATE CONNECTION sqlserver_conn
       TYPE sqlserver
       OPTIONS (
         host '<your-sqlserver-host>.database.windows.net',
         port '1433',
         user secret('sqlserver-scope', 'username'),
         password secret('sqlserver-scope', 'password'),
         encrypt 'true',
         trustServerCertificate 'false'
       );

  2. Grant access to the connection:
       GRANT USE CONNECTION sqlserver_conn TO `data-engineering-group`;

  3. A foreign catalog must be created for browsing source schemas:
       CREATE FOREIGN CATALOG IF NOT EXISTS sqlserver_source
       USING CONNECTION sqlserver_conn
       OPTIONS (database 'AdventureWorks');
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F


# ---------------------------------------------------------------------------
# Lakeflow Connect: Gateway / Ingestion pipeline configuration
# ---------------------------------------------------------------------------
# Lakeflow Connect uses `create_streaming_table` + `create_auto_cdc_flow`
# to continuously replicate SQL Server tables into Delta Lake via Change
# Data Capture (CDC).  The connection is managed through Unity Catalog and
# Databricks Secure Data Platform (SDP).
# ---------------------------------------------------------------------------

# -- Table 1: Customers -------------------------------------------------------

dp.create_streaming_table(
    name="bronze_customers",
    comment="Bronze: Raw customers table replicated from SQL Server via Lakeflow Connect",
)

dp.create_auto_cdc_flow(
    target="bronze_customers",
    source="sqlserver_source.dbo.Customers",
    keys=["CustomerID"],
    sequence_by="ModifiedDate",
    stored_as_scd_type=1,
    ignore_null_updates=True,
)


# -- Table 2: Orders ----------------------------------------------------------

dp.create_streaming_table(
    name="bronze_orders",
    comment="Bronze: Raw orders table replicated from SQL Server via Lakeflow Connect",
)

dp.create_auto_cdc_flow(
    target="bronze_orders",
    source="sqlserver_source.dbo.Orders",
    keys=["OrderID"],
    sequence_by="ModifiedDate",
    stored_as_scd_type=1,
    ignore_null_updates=True,
)


# -- Table 3: Order Line Items ------------------------------------------------

dp.create_streaming_table(
    name="bronze_order_items",
    comment="Bronze: Raw order line items replicated from SQL Server via Lakeflow Connect",
)

dp.create_auto_cdc_flow(
    target="bronze_order_items",
    source="sqlserver_source.dbo.OrderItems",
    keys=["OrderItemID"],
    sequence_by="ModifiedDate",
    stored_as_scd_type=1,
    ignore_null_updates=True,
)


# -- Table 4: Products --------------------------------------------------------

dp.create_streaming_table(
    name="bronze_products",
    comment="Bronze: Raw products table replicated from SQL Server via Lakeflow Connect",
)

dp.create_auto_cdc_flow(
    target="bronze_products",
    source="sqlserver_source.dbo.Products",
    keys=["ProductID"],
    sequence_by="ModifiedDate",
    stored_as_scd_type=1,
    ignore_null_updates=True,
)


# -- Table 5: Customers with SCD Type 2 (full history tracking) ---------------

dp.create_streaming_table(
    name="bronze_customers_history",
    comment="Bronze: Customers with SCD Type 2 history from SQL Server via Lakeflow Connect",
)

dp.create_auto_cdc_flow(
    target="bronze_customers_history",
    source="sqlserver_source.dbo.Customers",
    keys=["CustomerID"],
    sequence_by="ModifiedDate",
    stored_as_scd_type=2,
    track_history_column_list=["Email", "Phone", "Address", "City", "State", "ZipCode"],
    ignore_null_updates=True,
)
