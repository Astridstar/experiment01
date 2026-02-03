-- =============================================================================
-- Gold Layer: Customers View with Dynamic PII Masking
-- Published to Unity Catalog - Accessible to all users with SELECT permission
-- Masking applied at QUERY TIME based on current user's access level
-- =============================================================================

CREATE VIEW customers_gold_uc
COMMENT 'Gold layer: Customer data with dynamic PII masking based on temporal access grants (query-time evaluation)'
AS
WITH user_access AS (
  -- Get current user's access level from access grants table
  SELECT 
    COALESCE(access_level, 'masked_only') AS access_level
  FROM dev.experiment01.pii_access_grants
  WHERE user_email = CURRENT_USER()
    AND is_active = TRUE
    AND (expires_at IS NULL OR expires_at > CURRENT_TIMESTAMP())
  LIMIT 1
),
silver_with_access AS (
  -- Cross join silver data with user's access level
  SELECT 
    s.*,
    COALESCE(u.access_level, 'masked_only') AS user_access_level
  FROM dev.experiment01.customers_silver s
  CROSS JOIN (
    SELECT access_level FROM user_access
    UNION ALL
    SELECT 'masked_only' AS access_level
    LIMIT 1
  ) u
)
SELECT
  customer_id,
  full_name,
  
  -- Email masking: prefix masked for partial, full mask for masked_only
  CASE 
    WHEN user_access_level = 'full_access' THEN email
    WHEN user_access_level = 'partial_access' THEN 
      CONCAT(SUBSTRING(SPLIT(email, '@')[0], 1, 1), '***@', SPLIT(email, '@')[1])
    ELSE '***@***'
  END AS email,
  
  -- Phone masking: middle digits masked for partial, full mask for masked_only
  CASE 
    WHEN user_access_level = 'full_access' THEN phone
    WHEN user_access_level = 'partial_access' THEN 
      CONCAT(SUBSTRING(phone, 1, 4), ' ****', SUBSTRING(phone, -4, 4))
    ELSE '***'
  END AS phone,
  
  -- NRIC masking: middle digits masked for partial, full mask for masked_only
  CASE 
    WHEN user_access_level = 'full_access' THEN nric
    WHEN user_access_level = 'partial_access' THEN 
      CONCAT(SUBSTRING(nric, 1, 1), '****', SUBSTRING(nric, -3, 3))
    ELSE '***'
  END AS nric,
  
  dob,
  
  -- Address masking: keep postal code for partial, full mask for masked_only
  CASE 
    WHEN user_access_level = 'full_access' THEN address
    WHEN user_access_level = 'partial_access' THEN 
      CONCAT('*** Singapore ', postal_code)
    ELSE '***'
  END AS address,
  
  postal_code,
  country,
  gender,
  signup_ts,
  last_login_ts,
  status,
  segment,
  credit_score,
  annual_income,
  data_quality_flags,
  quality_score,
  
  -- Metadata
  CURRENT_TIMESTAMP() AS masked_at,
  CURRENT_USER() AS masked_for_user,
  user_access_level AS applied_access_level
  
FROM silver_with_access;
