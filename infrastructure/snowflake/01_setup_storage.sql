-- ============================================================================
-- GFN Pipeline - Snowflake Storage Integration Setup
-- ============================================================================
-- This script sets up the S3 external stage and storage integration
-- for Snowpipe to automatically ingest data from S3.
--
-- Data Model: Comprehensive extraction of ALL GFN data types using the
-- efficient bulk API endpoint /data/all/{year}.
--
-- S3 Folder Structure (simplified):
--   s3://gfn-data-lake/
--   ├── raw/                    # Raw extracted data
--   │   └── gfn_footprint_{timestamp}.json
--   └── transformed/            # Processed data ready for loading
--       └── gfn_footprint_{timestamp}_transformed.json
--
-- Prerequisites:
--   1. AWS S3 bucket: gfn-data-lake
--   2. IAM role with S3 access (see snowpipe_iam_role.json)
--   3. Snowflake ACCOUNTADMIN or equivalent privileges
--
-- Idempotency: All CREATE statements use CREATE OR REPLACE or IF NOT EXISTS
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

-- Staging layer: Cleaned and validated data (deduplicated)
CREATE SCHEMA IF NOT EXISTS STAGING;

-- Mart layer: Business-ready aggregations
CREATE SCHEMA IF NOT EXISTS MART;

-- Monitoring layer: Pipeline health and data quality
CREATE SCHEMA IF NOT EXISTS MONITORING;

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
    STORAGE_ALLOWED_LOCATIONS = ('s3://gfn-data-lake/transformed/');

-- Verify the integration (copy these values for IAM role setup)
DESC STORAGE INTEGRATION gfn_s3_integration;

-- ============================================================================
-- 3. Create External Stage for Transformed Data
-- ============================================================================

USE SCHEMA RAW;

CREATE OR REPLACE STAGE gfn_transformed_stage
    STORAGE_INTEGRATION = gfn_s3_integration
    URL = 's3://gfn-data-lake/transformed/'
    FILE_FORMAT = (
        TYPE = 'JSON'
        STRIP_OUTER_ARRAY = TRUE
        DATE_FORMAT = 'AUTO'
        TIMESTAMP_FORMAT = 'AUTO'
    );

-- Verify stage
LIST @gfn_transformed_stage;

-- ============================================================================
-- 4. Create Raw Landing Table (Snowpipe Target)
-- ============================================================================
-- This table receives raw JSON from Snowpipe with minimal transformation.
-- Schema matches the pipeline output from lambda_handlers.py

CREATE OR REPLACE TABLE RAW.GFN_FOOTPRINT_RAW (
    -- Metadata columns (added by Snowpipe)
    _loaded_at          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _source_file        VARCHAR(500),
    _row_number         NUMBER,
    
    -- Data columns (from JSON)
    country_code        NUMBER,
    country_name        VARCHAR(200),
    short_name          VARCHAR(100),
    iso_alpha2          VARCHAR(2),
    year                NUMBER,
    record_type         VARCHAR(50),
    
    -- Land use breakdown (from bulk API)
    crop_land           FLOAT,
    grazing_land        FLOAT,
    forest_land         FLOAT,
    fishing_ground      FLOAT,
    builtup_land        FLOAT,
    carbon              FLOAT,
    
    -- Aggregate values
    value               FLOAT,
    score               VARCHAR(10),
    
    -- Derived fields (from transform)
    carbon_pct_of_total FLOAT,
    
    -- Timestamps
    extracted_at        TIMESTAMP_TZ,
    transformed_at      TIMESTAMP_TZ
);

-- ============================================================================
-- 5. Create Staging Table (Deduplicated)
-- ============================================================================
-- Staging table with primary key for idempotent MERGE operations.
-- Supports backfill by updating existing records.

CREATE OR REPLACE TABLE STAGING.GFN_FOOTPRINT (
    -- Primary key
    country_code        NUMBER NOT NULL,
    year                NUMBER NOT NULL,
    record_type         VARCHAR(50) NOT NULL,
    
    -- Country info
    country_name        VARCHAR(200),
    short_name          VARCHAR(100),
    iso_alpha2          VARCHAR(2),
    
    -- Land use breakdown
    crop_land           FLOAT,
    grazing_land        FLOAT,
    forest_land         FLOAT,
    fishing_ground      FLOAT,
    builtup_land        FLOAT,
    carbon              FLOAT,
    
    -- Aggregate values
    value               FLOAT,
    score               VARCHAR(10),
    
    -- Derived fields
    carbon_pct_of_total FLOAT,
    
    -- Audit columns
    extracted_at        TIMESTAMP_TZ,
    transformed_at      TIMESTAMP_TZ,
    _loaded_at          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _updated_at         TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _source_file        VARCHAR(500),
    
    -- Primary key constraint for idempotent MERGE
    PRIMARY KEY (country_code, year, record_type)
);

-- Clustering for query performance
ALTER TABLE STAGING.GFN_FOOTPRINT 
    CLUSTER BY (year, country_code);

-- ============================================================================
-- 6. Create Mart Summary Table
-- ============================================================================
-- Aggregated view with key metrics per country-year.
-- Pivots record types into columns for easy analysis.

CREATE OR REPLACE TABLE MART.GFN_FOOTPRINT_SUMMARY (
    -- Dimensions
    country_code            NUMBER NOT NULL,
    country_name            VARCHAR(200),
    iso_alpha2              VARCHAR(2),
    year                    NUMBER NOT NULL,
    
    -- Total metrics (in global hectares)
    total_ecological_footprint  FLOAT,
    carbon_footprint            FLOAT,
    total_biocapacity           FLOAT,
    ecological_deficit          FLOAT,    -- Positive = deficit, Negative = reserve
    
    -- Per capita metrics
    footprint_per_capita        FLOAT,
    carbon_per_capita           FLOAT,
    biocapacity_per_capita      FLOAT,
    
    -- Derived metrics
    carbon_percentage           FLOAT,    -- Carbon as % of total footprint
    
    -- Component footprints
    cropland_footprint          FLOAT,
    grazing_footprint           FLOAT,
    forest_footprint            FLOAT,
    fishing_footprint           FLOAT,
    builtup_footprint           FLOAT,
    
    -- Metadata
    _updated_at             TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    
    PRIMARY KEY (country_code, year)
);

-- Clustering for performance
ALTER TABLE MART.GFN_FOOTPRINT_SUMMARY 
    CLUSTER BY (year, country_code);

-- ============================================================================
-- 7. Create Countries Reference Table
-- ============================================================================

CREATE OR REPLACE TABLE RAW.GFN_COUNTRIES (
    country_code        NUMBER PRIMARY KEY,
    country_name        VARCHAR(200) NOT NULL,
    short_name          VARCHAR(100),
    iso_alpha2          VARCHAR(2),
    version             VARCHAR(20),
    score               VARCHAR(10),
    extracted_at        TIMESTAMP_TZ,
    _loaded_at          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ============================================================================
-- 8. Create Record Types Reference Table
-- ============================================================================

CREATE OR REPLACE TABLE RAW.GFN_RECORD_TYPES (
    record_type         VARCHAR(50) PRIMARY KEY,
    description         VARCHAR(200),
    category            VARCHAR(50),
    is_per_capita       BOOLEAN,
    unit                VARCHAR(50),
    _loaded_at          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Populate reference data (dynamically discovered types)
-- These are the common types from the GFN API
MERGE INTO RAW.GFN_RECORD_TYPES AS target
USING (
    SELECT * FROM VALUES
        -- Ecological Footprint (Total)
        ('EFConsTotGHA', 'Total Ecological Footprint of Consumption', 'ecological_footprint', FALSE, 'global hectares'),
        ('EFConsCropLandGHA', 'Cropland Footprint', 'ecological_footprint', FALSE, 'global hectares'),
        ('EFConsGrazLandGHA', 'Grazing Land Footprint', 'ecological_footprint', FALSE, 'global hectares'),
        ('EFConsForestLandGHA', 'Forest Product Footprint', 'ecological_footprint', FALSE, 'global hectares'),
        ('EFConsFishSeaGHA', 'Fishing Ground Footprint', 'ecological_footprint', FALSE, 'global hectares'),
        ('EFConsBuiltLandGHA', 'Built-up Land Footprint', 'ecological_footprint', FALSE, 'global hectares'),
        ('EFConsCarbonGHA', 'Carbon Footprint', 'ecological_footprint', FALSE, 'global hectares'),
        -- Biocapacity (Total)
        ('BiocapTotGHA', 'Total Biocapacity', 'biocapacity', FALSE, 'global hectares'),
        ('BiocapCropLandGHA', 'Cropland Biocapacity', 'biocapacity', FALSE, 'global hectares'),
        ('BiocapGrazLandGHA', 'Grazing Land Biocapacity', 'biocapacity', FALSE, 'global hectares'),
        ('BiocapForestLandGHA', 'Forest Biocapacity', 'biocapacity', FALSE, 'global hectares'),
        ('BiocapFishSeaGHA', 'Fishing Ground Biocapacity', 'biocapacity', FALSE, 'global hectares'),
        ('BiocapBuiltLandGHA', 'Built-up Land Biocapacity', 'biocapacity', FALSE, 'global hectares'),
        -- Per Capita versions
        ('EFConsPerCap', 'Ecological Footprint per Capita', 'ecological_footprint', TRUE, 'gha per person'),
        ('BiocapPerCap', 'Biocapacity per Capita', 'biocapacity', TRUE, 'gha per person')
    AS t(record_type, description, category, is_per_capita, unit)
) AS source
ON target.record_type = source.record_type
WHEN MATCHED THEN UPDATE SET
    description = source.description,
    category = source.category,
    is_per_capita = source.is_per_capita,
    unit = source.unit
WHEN NOT MATCHED THEN INSERT (record_type, description, category, is_per_capita, unit)
VALUES (source.record_type, source.description, source.category, source.is_per_capita, source.unit);

-- ============================================================================
-- 9. Create File Format for JSON
-- ============================================================================

CREATE OR REPLACE FILE FORMAT RAW.GFN_JSON_FORMAT
    TYPE = 'JSON'
    STRIP_OUTER_ARRAY = TRUE
    DATE_FORMAT = 'AUTO'
    TIMESTAMP_FORMAT = 'AUTO'
    IGNORE_UTF8_ERRORS = TRUE;

-- ============================================================================
-- 10. Grant Permissions
-- ============================================================================

-- Create a role for pipeline operations
CREATE ROLE IF NOT EXISTS GFN_PIPELINE_ROLE;

-- Grant necessary permissions
GRANT USAGE ON DATABASE GFN TO ROLE GFN_PIPELINE_ROLE;
GRANT USAGE ON ALL SCHEMAS IN DATABASE GFN TO ROLE GFN_PIPELINE_ROLE;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA GFN.RAW TO ROLE GFN_PIPELINE_ROLE;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA GFN.STAGING TO ROLE GFN_PIPELINE_ROLE;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA GFN.MART TO ROLE GFN_PIPELINE_ROLE;
GRANT USAGE ON STAGE RAW.GFN_TRANSFORMED_STAGE TO ROLE GFN_PIPELINE_ROLE;

-- Grant future permissions
GRANT SELECT, INSERT, UPDATE, DELETE ON FUTURE TABLES IN SCHEMA GFN.RAW TO ROLE GFN_PIPELINE_ROLE;
GRANT SELECT, INSERT, UPDATE, DELETE ON FUTURE TABLES IN SCHEMA GFN.STAGING TO ROLE GFN_PIPELINE_ROLE;
GRANT SELECT, INSERT, UPDATE, DELETE ON FUTURE TABLES IN SCHEMA GFN.MART TO ROLE GFN_PIPELINE_ROLE;
