-- =============================================================================
-- SQL Server Connection Setup for Databricks Lakeflow Connect (SDP)
-- =============================================================================
-- Run these statements in a Databricks SQL warehouse or notebook to configure
-- the Unity Catalog connection and foreign catalog for MS SQL Server ingestion.
-- =============================================================================

-- Step 1: Store credentials in Databricks Secret Scope
-- (Run via Databricks CLI - not SQL)
--   databricks secrets create-scope sqlserver-scope
--   databricks secrets put-secret sqlserver-scope username --string-value "<sql-user>"
--   databricks secrets put-secret sqlserver-scope password --string-value "<sql-password>"

-- Step 2: Create Unity Catalog connection to MS SQL Server
CREATE CONNECTION IF NOT EXISTS sqlserver_conn
TYPE sqlserver
OPTIONS (
  host '<your-sqlserver-host>.database.windows.net',
  port '1433',
  user secret('sqlserver-scope', 'username'),
  password secret('sqlserver-scope', 'password'),
  encrypt 'true',
  trustServerCertificate 'false'
);

-- Step 3: Verify the connection
DESCRIBE CONNECTION sqlserver_conn;

-- Step 4: Create a foreign catalog to browse SQL Server schemas/tables
CREATE FOREIGN CATALOG IF NOT EXISTS sqlserver_source
USING CONNECTION sqlserver_conn
OPTIONS (database 'AdventureWorks');

-- Step 5: Verify you can see SQL Server tables
SHOW TABLES IN sqlserver_source.dbo;

-- Step 6: Grant access to the data engineering group
GRANT USE CONNECTION sqlserver_conn TO `data-engineering-group`;
GRANT USE CATALOG sqlserver_source TO `data-engineering-group`;

-- Step 7: (Optional) Preview source data before pipeline run
-- SELECT * FROM sqlserver_source.dbo.Customers LIMIT 10;
-- SELECT * FROM sqlserver_source.dbo.Orders LIMIT 10;
-- SELECT * FROM sqlserver_source.dbo.OrderItems LIMIT 10;
-- SELECT * FROM sqlserver_source.dbo.Products LIMIT 10;
