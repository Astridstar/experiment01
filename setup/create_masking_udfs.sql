-- =============================================================================
-- Unity Catalog Masking UDFs for Dynamic PII Protection
-- Run this ONCE in SQL Editor to create masking functions
-- These functions will be reused across all 400 silver tables
-- =============================================================================

-- NOTE: This file should be run manually in SQL Editor, NOT as part of the pipeline
-- Spark Declarative Pipelines cannot create UDFs - they must be pre-created in Unity Catalog

USE CATALOG dev;
USE SCHEMA experiment01;

-- =============================================================================
-- Email Masking Function
-- =============================================================================
CREATE OR REPLACE FUNCTION mask_email(email STRING)
RETURNS STRING
COMMENT 'Mask email based on user access level from pii_access_grants table'
RETURN CASE
  WHEN EXISTS (
    SELECT 1 FROM dev.experiment01.pii_access_grants
    WHERE user_email = CURRENT_USER()
      AND access_level = 'full_access'
      AND is_active = TRUE
      AND (expires_at IS NULL OR expires_at > CURRENT_TIMESTAMP())
  ) THEN email
  WHEN EXISTS (
    SELECT 1 FROM dev.experiment01.pii_access_grants
    WHERE user_email = CURRENT_USER()
      AND access_level = 'partial_access'
      AND is_active = TRUE
      AND (expires_at IS NULL OR expires_at > CURRENT_TIMESTAMP())
  ) THEN CONCAT(SUBSTRING(SPLIT(email, '@')[0], 1, 1), '***@', SPLIT(email, '@')[1])
  ELSE '***@***'
END;

-- =============================================================================
-- Phone Masking Function
-- =============================================================================
CREATE OR REPLACE FUNCTION mask_phone(phone STRING)
RETURNS STRING
COMMENT 'Mask phone number based on user access level from pii_access_grants table'
RETURN CASE
  WHEN EXISTS (
    SELECT 1 FROM dev.experiment01.pii_access_grants
    WHERE user_email = CURRENT_USER()
      AND access_level = 'full_access'
      AND is_active = TRUE
      AND (expires_at IS NULL OR expires_at > CURRENT_TIMESTAMP())
  ) THEN phone
  WHEN EXISTS (
    SELECT 1 FROM dev.experiment01.pii_access_grants
    WHERE user_email = CURRENT_USER()
      AND access_level = 'partial_access'
      AND is_active = TRUE
      AND (expires_at IS NULL OR expires_at > CURRENT_TIMESTAMP())
  ) THEN CONCAT(SUBSTRING(phone, 1, 4), ' ****', SUBSTRING(phone, -4, 4))
  ELSE '***'
END;

-- =============================================================================
-- NRIC Masking Function
-- =============================================================================
CREATE OR REPLACE FUNCTION mask_nric(nric STRING)
RETURNS STRING
COMMENT 'Mask NRIC based on user access level from pii_access_grants table'
RETURN CASE
  WHEN EXISTS (
    SELECT 1 FROM dev.experiment01.pii_access_grants
    WHERE user_email = CURRENT_USER()
      AND access_level = 'full_access'
      AND is_active = TRUE
      AND (expires_at IS NULL OR expires_at > CURRENT_TIMESTAMP())
  ) THEN nric
  WHEN EXISTS (
    SELECT 1 FROM dev.experiment01.pii_access_grants
    WHERE user_email = CURRENT_USER()
      AND access_level = 'partial_access'
      AND is_active = TRUE
      AND (expires_at IS NULL OR expires_at > CURRENT_TIMESTAMP())
  ) THEN CONCAT(SUBSTRING(nric, 1, 1), '****', SUBSTRING(nric, -3, 3))
  ELSE '***'
END;

-- =============================================================================
-- Address Masking Function
-- =============================================================================
CREATE OR REPLACE FUNCTION mask_address(address STRING, postal_code STRING)
RETURNS STRING
COMMENT 'Mask address based on user access level from pii_access_grants table'
RETURN CASE
  WHEN EXISTS (
    SELECT 1 FROM dev.experiment01.pii_access_grants
    WHERE user_email = CURRENT_USER()
      AND access_level = 'full_access'
      AND is_active = TRUE
      AND (expires_at IS NULL OR expires_at > CURRENT_TIMESTAMP())
  ) THEN address
  WHEN EXISTS (
    SELECT 1 FROM dev.experiment01.pii_access_grants
    WHERE user_email = CURRENT_USER()
      AND access_level = 'partial_access'
      AND is_active = TRUE
      AND (expires_at IS NULL OR expires_at > CURRENT_TIMESTAMP())
  ) THEN CONCAT('*** Singapore ', postal_code)
  ELSE '***'
END;

-- =============================================================================
-- SSN Masking Function (if needed)
-- =============================================================================
CREATE OR REPLACE FUNCTION mask_ssn(ssn STRING)
RETURNS STRING
COMMENT 'Mask SSN based on user access level from pii_access_grants table'
RETURN CASE
  WHEN EXISTS (
    SELECT 1 FROM dev.experiment01.pii_access_grants
    WHERE user_email = CURRENT_USER()
      AND access_level = 'full_access'
      AND is_active = TRUE
      AND (expires_at IS NULL OR expires_at > CURRENT_TIMESTAMP())
  ) THEN ssn
  WHEN EXISTS (
    SELECT 1 FROM dev.experiment01.pii_access_grants
    WHERE user_email = CURRENT_USER()
      AND access_level = 'partial_access'
      AND is_active = TRUE
      AND (expires_at IS NULL OR expires_at > CURRENT_TIMESTAMP())
  ) THEN CONCAT('***-**-', SUBSTRING(ssn, -4, 4))
  ELSE '***'
END;

-- =============================================================================
-- Verify Functions Created
-- =============================================================================
SHOW FUNCTIONS IN dev.experiment01 LIKE 'mask_*';
