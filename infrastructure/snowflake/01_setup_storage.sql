-- ============================================================================
-- GFN Pipeline - Snowflake Storage Integration Setup
-- ============================================================================
-- This script sets up the S3 external stage and storage integration
-- for Snowpipe to automatically ingest data from S3.
--
-- Prerequisites:
--   1. AWS S3 bucket: gfn-data-lake
--   2. IAM role with S3 access (created separately)
--   3. Snowflake ACCOUNTADMIN or equivalent privileges
-- ============================================================================

-- Use appropriate role and warehouse
USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;

-- ============================================================================
-- 1. Create Database and Schemas
-- ============================================================================

CREATE DATABASE IF NOT EXISTS GFN;

USE DATABASE GFN;

-- Raw layer: Landing zone for Snowpipe
CREATE SCHEMA IF NOT EXISTS RAW;

-- Staging layer: Cleaned and validated data
CREATE SCHEMA IF NOT EXISTS STAGING;

-- Mart layer: Business-ready aggregations
CREATE SCHEMA IF NOT EXISTS MART;

-- ============================================================================
-- 2. Create Storage Integration (S3 → Snowflake)
-- ============================================================================
-- NOTE: After running this, you need to:
--   1. Run: DESC STORAGE INTEGRATION gfn_s3_integration;
--   2. Copy STORAGE_AWS_IAM_USER_ARN and STORAGE_AWS_EXTERNAL_ID
--   3. Update your AWS IAM role trust policy with these values

CREATE OR REPLACE STORAGE INTEGRATION gfn_s3_integration
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = 'S3'
    ENABLED = TRUE
    STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::<YOUR_AWS_ACCOUNT_ID>:role/gfn-snowflake-role'
    STORAGE_ALLOWED_LOCATIONS = ('s3://gfn-data-lake/processed/');

-- Verify the integration (copy these values for IAM role setup)
DESC STORAGE INTEGRATION gfn_s3_integration;

-- ============================================================================
-- 3. Create External Stage
-- ============================================================================

USE SCHEMA RAW;

CREATE OR REPLACE STAGE gfn_processed_stage
    STORAGE_INTEGRATION = gfn_s3_integration
    URL = 's3://gfn-data-lake/processed/carbon_footprint/'
    FILE_FORMAT = (
        TYPE = 'JSON'
        STRIP_OUTER_ARRAY = TRUE
        DATE_FORMAT = 'AUTO'
        TIMESTAMP_FORMAT = 'AUTO'
    );

-- Verify stage
LIST @gfn_processed_stage;

-- ============================================================================
-- 4. Create Raw Landing Table
-- ============================================================================

CREATE OR REPLACE TABLE RAW.CARBON_FOOTPRINT_RAW (
    -- Metadata columns
    _loaded_at          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _source_file        VARCHAR(500),
    _row_number         NUMBER,
    
    -- Raw JSON payload
    raw_data            VARIANT
);

-- ============================================================================
-- 5. Create Processed Table (Structured)
-- ============================================================================

CREATE OR REPLACE TABLE STAGING.CARBON_FOOTPRINT (
    -- Primary key
    id                  NUMBER AUTOINCREMENT PRIMARY KEY,
    
    -- Business columns
    country_code        NUMBER,
    country_name        VARCHAR(100),
    year                NUMBER,
    record              VARCHAR(50),
    crop_land           FLOAT,
    grazing_land        FLOAT,
    forest_land         FLOAT,
    fishing_ground      FLOAT,
    built_up_land       FLOAT,
    carbon              FLOAT,
    value               FLOAT,
    score               VARCHAR(10),
    
    -- Computed columns
    total_ecological    FLOAT,
    carbon_percentage   FLOAT,
    
    -- Audit columns
    _loaded_at          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _source_file        VARCHAR(500),
    _updated_at         TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ============================================================================
-- 6. Create Mart Table (Aggregated)
-- ============================================================================

CREATE OR REPLACE TABLE MART.CARBON_FOOTPRINT_SUMMARY (
    country_code        NUMBER,
    country_name        VARCHAR(100),
    year                NUMBER,
    
    -- Aggregated metrics
    total_footprint     FLOAT,
    carbon_footprint    FLOAT,
    biocapacity         FLOAT,
    ecological_deficit  FLOAT,
    carbon_intensity    FLOAT,
    
    -- Metadata
    _updated_at         TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Add clustering for performance
ALTER TABLE MART.CARBON_FOOTPRINT_SUMMARY 
    CLUSTER BY (year, country_code);
