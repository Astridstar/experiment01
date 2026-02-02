-- ==========================================================================
-- == Incrementally load RAW CUSTOMER INFO from CSV                        ==
-- ==========================================================================
CREATE OR REFRESH STREAMING TABLE customers_raw
COMMENT "Raw customer information streamed in from CSV files with normalized column names (lowercase, trimmed, spaces replaced with underscores)."
AS SELECT 
  customer_id,
  TRIM(full_name) AS full_name,
  TRIM(email) AS email,
  TRIM(phone) AS phone,
  TRIM(nric) AS nric,
  dob,
  TRIM(address) AS address,
  TRIM(country) AS country,
  TRIM(gender) AS gender,
  signup_ts,
  last_login_ts,
  TRIM(status) AS status,
  TRIM(segment) AS segment,
  credit_score,
  annual_income,
  is_dirty,
  TRIM(dirty_reason) AS dirty_reason,
  attributes_json,
  _rescued_data,
  _metadata.file_path AS ingested_file,
  current_timestamp() AS ingestion_ts
FROM 
  STREAM READ_FILES(
    "/Volumes/dev/experiment01/raw_data/*.csv", 
    FORMAT => "csv", 
    MULTILINE => TRUE
  );
