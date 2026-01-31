-- ============================================================================
-- GFN Pipeline - Snowflake Storage Integration Setup
-- ============================================================================
-- This script sets up the S3 external stage and storage integration
-- for Snowpipe to automatically ingest data from S3.
--
-- Data Model: Comprehensive extraction of ALL GFN data types:
--   - 7 Ecological Footprint types (total + 6 components)
--   - 7 Biocapacity types (total + 6 components)  
--   - 14 Per Capita versions of the above
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
    URL = 's3://gfn-data-lake/processed/gfn_footprint_data/'
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

CREATE OR REPLACE TABLE RAW.GFN_FOOTPRINT_DATA_RAW (
    -- Metadata columns
    _loaded_at          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _source_file        VARCHAR(500),
    _row_number         NUMBER,
    
    -- Raw JSON payload
    raw_data            VARIANT
);

-- ============================================================================
-- 5. Create Staging Table (Structured)
-- ============================================================================
-- Normalized structure for all GFN data types

CREATE OR REPLACE TABLE STAGING.GFN_FOOTPRINT_DATA (
    -- Primary key
    id                  NUMBER AUTOINCREMENT PRIMARY KEY,
    
    -- Country identification
    country_code        NUMBER,
    country_name        VARCHAR(100),
    iso_alpha2          VARCHAR(2),
    
    -- Time dimension
    year                NUMBER,
    
    -- Record classification
    record_type         VARCHAR(50),      -- e.g., 'EFCtot', 'BioCaptot', 'EFCcarbPerCap'
    record_category     VARCHAR(50),      -- 'ecological_footprint' or 'biocapacity'
    
    -- Measurement
    value               FLOAT,            -- Value in global hectares (or gha per capita)
    per_capita          BOOLEAN,          -- True if per-capita metric
    
    -- Quality indicator
    score               VARCHAR(10),
    
    -- Audit columns
    _loaded_at          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _source_file        VARCHAR(500),
    _updated_at         TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Add index for common query patterns
CREATE OR REPLACE INDEX idx_gfn_footprint_country_year 
    ON STAGING.GFN_FOOTPRINT_DATA (country_code, year);

-- ============================================================================
-- 6. Create Mart Table (Aggregated Summary)
-- ============================================================================
-- Pivoted view with key metrics per country-year

CREATE OR REPLACE TABLE MART.GFN_FOOTPRINT_SUMMARY (
    -- Dimensions
    country_code            NUMBER,
    country_name            VARCHAR(100),
    iso_alpha2              VARCHAR(2),
    year                    NUMBER,
    
    -- Total metrics (in global hectares)
    total_ecological_footprint  FLOAT,
    carbon_footprint            FLOAT,
    total_biocapacity           FLOAT,
    ecological_deficit          FLOAT,    -- Positive = deficit, Negative = reserve
    
    -- Per capita metrics (in global hectares per person)
    footprint_per_capita        FLOAT,
    carbon_per_capita           FLOAT,
    biocapacity_per_capita      FLOAT,
    
    -- Derived metrics
    carbon_percentage           FLOAT,    -- Carbon as % of total footprint
    
    -- Component footprints (in global hectares)
    cropland_footprint          FLOAT,
    grazing_footprint           FLOAT,
    forest_footprint            FLOAT,
    fishing_footprint           FLOAT,
    builtup_footprint           FLOAT,
    
    -- Metadata
    _updated_at             TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Add clustering for performance
ALTER TABLE MART.GFN_FOOTPRINT_SUMMARY 
    CLUSTER BY (year, country_code);

-- ============================================================================
-- 7. Create Reference Table for Record Types
-- ============================================================================

CREATE OR REPLACE TABLE RAW.GFN_RECORD_TYPES (
    record_type         VARCHAR(50) PRIMARY KEY,
    description         VARCHAR(200),
    category            VARCHAR(50),
    is_per_capita       BOOLEAN,
    unit                VARCHAR(50)
);

-- Populate reference data
INSERT INTO RAW.GFN_RECORD_TYPES VALUES
    -- Ecological Footprint (Total)
    ('EFCtot', 'Total Ecological Footprint', 'ecological_footprint', FALSE, 'global hectares'),
    ('EFCcrop', 'Cropland Footprint', 'ecological_footprint', FALSE, 'global hectares'),
    ('EFCgraz', 'Grazing Land Footprint', 'ecological_footprint', FALSE, 'global hectares'),
    ('EFCfrst', 'Forest Product Footprint', 'ecological_footprint', FALSE, 'global hectares'),
    ('EFCfish', 'Fishing Ground Footprint', 'ecological_footprint', FALSE, 'global hectares'),
    ('EFCbult', 'Built-up Land Footprint', 'ecological_footprint', FALSE, 'global hectares'),
    ('EFCcarb', 'Carbon Footprint', 'ecological_footprint', FALSE, 'global hectares'),
    -- Biocapacity (Total)
    ('BioCaptot', 'Total Biocapacity', 'biocapacity', FALSE, 'global hectares'),
    ('BioCapcrop', 'Cropland Biocapacity', 'biocapacity', FALSE, 'global hectares'),
    ('BioCapgraz', 'Grazing Land Biocapacity', 'biocapacity', FALSE, 'global hectares'),
    ('BioCapfrst', 'Forest Biocapacity', 'biocapacity', FALSE, 'global hectares'),
    ('BioCapfish', 'Fishing Ground Biocapacity', 'biocapacity', FALSE, 'global hectares'),
    ('BioCapbult', 'Built-up Land Biocapacity', 'biocapacity', FALSE, 'global hectares'),
    -- Ecological Footprint (Per Capita)
    ('EFCtotPerCap', 'Total Ecological Footprint per Capita', 'ecological_footprint', TRUE, 'gha per person'),
    ('EFCcropPerCap', 'Cropland Footprint per Capita', 'ecological_footprint', TRUE, 'gha per person'),
    ('EFCgrazPerCap', 'Grazing Land Footprint per Capita', 'ecological_footprint', TRUE, 'gha per person'),
    ('EFCfrstPerCap', 'Forest Product Footprint per Capita', 'ecological_footprint', TRUE, 'gha per person'),
    ('EFCfishPerCap', 'Fishing Ground Footprint per Capita', 'ecological_footprint', TRUE, 'gha per person'),
    ('EFCbultPerCap', 'Built-up Land Footprint per Capita', 'ecological_footprint', TRUE, 'gha per person'),
    ('EFCcarbPerCap', 'Carbon Footprint per Capita', 'ecological_footprint', TRUE, 'gha per person'),
    -- Biocapacity (Per Capita)
    ('BioCaptotPerCap', 'Total Biocapacity per Capita', 'biocapacity', TRUE, 'gha per person'),
    ('BioCapcropPerCap', 'Cropland Biocapacity per Capita', 'biocapacity', TRUE, 'gha per person'),
    ('BioCapgrazPerCap', 'Grazing Land Biocapacity per Capita', 'biocapacity', TRUE, 'gha per person'),
    ('BioCapfrstPerCap', 'Forest Biocapacity per Capita', 'biocapacity', TRUE, 'gha per person'),
    ('BioCapfishPerCap', 'Fishing Ground Biocapacity per Capita', 'biocapacity', TRUE, 'gha per person'),
    ('BioCapbultPerCap', 'Built-up Land Biocapacity per Capita', 'biocapacity', TRUE, 'gha per person');
