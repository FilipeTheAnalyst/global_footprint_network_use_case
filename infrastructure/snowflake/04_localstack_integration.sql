-- =============================================================================
-- Snowflake Integration with LocalStack via ngrok
-- =============================================================================
-- This script sets up Snowflake to load data from LocalStack S3 exposed via ngrok.
-- 
-- IMPORTANT: The ngrok URL changes each time you restart ngrok!
-- Update NGROK_BASE_URL below with your current ngrok URL.
--
-- Usage:
--   1. Start LocalStack: make localstack-up
--   2. Start ngrok: ngrok http 4566
--   3. Copy the ngrok URL and update NGROK_BASE_URL below
--   4. Run this script in Snowflake
--
-- S3 Structure (simplified):
--   s3://gfn-data-lake/raw/{timestamp}.json
--   s3://gfn-data-lake/transformed/{timestamp}_transformed.json
-- =============================================================================

-- Set your ngrok URL here (without trailing slash)
SET NGROK_BASE_URL = 'https://your-ngrok-subdomain.ngrok-free.dev';

-- =============================================================================
-- 1. Database and Schema Setup
-- =============================================================================
USE ROLE ACCOUNTADMIN;

CREATE DATABASE IF NOT EXISTS GFN;
CREATE SCHEMA IF NOT EXISTS GFN.RAW;
CREATE SCHEMA IF NOT EXISTS GFN.STAGING;
CREATE SCHEMA IF NOT EXISTS GFN.MART;

USE DATABASE GFN;
USE SCHEMA RAW;

-- =============================================================================
-- 2. Create Network Rule for ngrok (required for external access)
-- =============================================================================
-- NOTE: Update the domain to match your ngrok URL

CREATE OR REPLACE NETWORK RULE ngrok_network_rule
    TYPE = HOST_PORT
    MODE = EGRESS
    VALUE_LIST = ('your-ngrok-subdomain.ngrok-free.dev:443');

-- Create external access integration
CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION ngrok_access_integration
    ALLOWED_NETWORK_RULES = (ngrok_network_rule)
    ENABLED = TRUE;

-- =============================================================================
-- 3. Create Target Tables for GFN Footprint Data
-- =============================================================================

-- Raw landing table (matches Snowpipe structure)
CREATE TABLE IF NOT EXISTS GFN.RAW.GFN_FOOTPRINT_RAW (
    _source_file        VARCHAR(500),
    _row_number         NUMBER,
    country_code        NUMBER,
    country_name        VARCHAR(200),
    short_name          VARCHAR(100),
    iso_alpha2          VARCHAR(2),
    year                NUMBER,
    record_type         VARCHAR(100),
    crop_land           FLOAT,
    grazing_land        FLOAT,
    forest_land         FLOAT,
    fishing_ground      FLOAT,
    builtup_land        FLOAT,
    carbon              FLOAT,
    value               FLOAT,
    score               VARCHAR(10),
    carbon_pct_of_total FLOAT,
    extracted_at        TIMESTAMP_TZ,
    transformed_at      TIMESTAMP_TZ,
    _loaded_at          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Staging table (deduplicated)
CREATE TABLE IF NOT EXISTS GFN.STAGING.GFN_FOOTPRINT (
    country_code        NUMBER,
    country_name        VARCHAR(200),
    short_name          VARCHAR(100),
    iso_alpha2          VARCHAR(2),
    year                NUMBER,
    record_type         VARCHAR(100),
    crop_land           FLOAT,
    grazing_land        FLOAT,
    forest_land         FLOAT,
    fishing_ground      FLOAT,
    builtup_land        FLOAT,
    carbon              FLOAT,
    value               FLOAT,
    score               VARCHAR(10),
    carbon_pct_of_total FLOAT,
    extracted_at        TIMESTAMP_TZ,
    transformed_at      TIMESTAMP_TZ,
    _loaded_at          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _updated_at         TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _source_file        VARCHAR(500),
    -- Composite primary key
    PRIMARY KEY (country_code, year, record_type)
);

-- =============================================================================
-- 4. Create Stored Procedure to Load Data from LocalStack
-- =============================================================================
-- This procedure fetches JSON data from LocalStack S3 via ngrok and loads it
-- into the raw table using idempotent MERGE logic.

CREATE OR REPLACE PROCEDURE GFN.RAW.LOAD_FROM_LOCALSTACK(
    file_path VARCHAR,
    ngrok_base_url VARCHAR DEFAULT 'https://your-ngrok-subdomain.ngrok-free.dev'
)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python', 'requests')
EXTERNAL_ACCESS_INTEGRATIONS = (ngrok_access_integration)
HANDLER = 'load_data'
AS
$$
import requests
import json
from snowflake.snowpark import Session
from datetime import datetime

def load_data(session: Session, file_path: str, ngrok_base_url: str) -> str:
    """Load JSON data from LocalStack S3 via ngrok into Snowflake."""
    
    # Construct the full URL
    url = f"{ngrok_base_url}/gfn-data-lake/{file_path}"
    
    try:
        # Fetch data from LocalStack via ngrok
        # Add ngrok-skip-browser-warning header to bypass ngrok warning page
        headers = {'ngrok-skip-browser-warning': 'true'}
        response = requests.get(url, timeout=30, headers=headers)
        response.raise_for_status()
        
        records = response.json()
        
        if not records:
            return "No records found in file"
        
        # Handle both single record and array of records
        if isinstance(records, dict):
            records = [records]
        
        # Insert records into the raw table
        inserted = 0
        for idx, record in enumerate(records):
            session.sql("""
                INSERT INTO GFN.RAW.GFN_FOOTPRINT_RAW 
                (_source_file, _row_number, country_code, country_name, short_name,
                 iso_alpha2, year, record_type, crop_land, grazing_land, forest_land,
                 fishing_ground, builtup_land, carbon, value, score, carbon_pct_of_total,
                 extracted_at, transformed_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, [
                file_path,
                idx + 1,
                record.get('country_code'),
                record.get('country_name'),
                record.get('short_name'),
                record.get('iso_alpha2'),
                record.get('year'),
                record.get('record_type', record.get('record')),  # Handle both field names
                record.get('crop_land', record.get('cropLand')),
                record.get('grazing_land', record.get('grazingLand')),
                record.get('forest_land', record.get('forestLand')),
                record.get('fishing_ground', record.get('fishingGround')),
                record.get('builtup_land', record.get('builtupLand')),
                record.get('carbon'),
                record.get('value'),
                record.get('score'),
                record.get('carbon_pct_of_total'),
                record.get('extracted_at'),
                record.get('transformed_at')
            ]).collect()
            inserted += 1
        
        return f"Successfully loaded {inserted} records from {file_path}"
        
    except requests.exceptions.RequestException as e:
        return f"HTTP Error: {str(e)}"
    except Exception as e:
        return f"Error: {str(e)}"
$$;

-- =============================================================================
-- 5. Create Procedure to List and Load All Files
-- =============================================================================
-- Since we can't list S3 directly via ngrok, provide known file paths

CREATE OR REPLACE PROCEDURE GFN.RAW.LOAD_ALL_FROM_LOCALSTACK(
    ngrok_base_url VARCHAR DEFAULT 'https://your-ngrok-subdomain.ngrok-free.dev'
)
RETURNS TABLE(file_path VARCHAR, result VARCHAR)
LANGUAGE SQL
AS
$$
DECLARE
    -- List of known files to load
    -- Update this array with your actual file paths from LocalStack
    files ARRAY := ARRAY_CONSTRUCT(
        'transformed/gfn_footprint_20260131_030733_transformed.json'
    );
    result_table RESULTSET;
BEGIN
    -- Create temp table for results
    CREATE OR REPLACE TEMPORARY TABLE load_results (file_path VARCHAR, result VARCHAR);
    
    -- Load each file
    FOR i IN 0 TO ARRAY_SIZE(:files) - 1 DO
        LET file_path VARCHAR := :files[i]::VARCHAR;
        LET load_result VARCHAR := (CALL GFN.RAW.LOAD_FROM_LOCALSTACK(:file_path, :ngrok_base_url));
        INSERT INTO load_results VALUES (:file_path, :load_result);
    END FOR;
    
    result_table := (SELECT * FROM load_results);
    RETURN TABLE(result_table);
END;
$$;

-- =============================================================================
-- 6. Create Procedure to MERGE Raw to Staging (Idempotent)
-- =============================================================================

CREATE OR REPLACE PROCEDURE GFN.RAW.MERGE_RAW_TO_STAGING()
RETURNS VARCHAR
LANGUAGE SQL
COMMENT = 'Idempotent MERGE from raw to staging table'
AS
$$
BEGIN
    MERGE INTO GFN.STAGING.GFN_FOOTPRINT AS target
    USING (
        SELECT
            country_code,
            country_name,
            short_name,
            iso_alpha2,
            year,
            record_type,
            crop_land,
            grazing_land,
            forest_land,
            fishing_ground,
            builtup_land,
            carbon,
            value,
            score,
            carbon_pct_of_total,
            extracted_at,
            transformed_at,
            _loaded_at,
            _source_file
        FROM GFN.RAW.GFN_FOOTPRINT_RAW
        -- Get latest record per key
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY country_code, year, record_type 
            ORDER BY transformed_at DESC NULLS LAST, _loaded_at DESC
        ) = 1
    ) AS source
    ON target.country_code = source.country_code 
       AND target.year = source.year
       AND target.record_type = source.record_type
    WHEN MATCHED AND (
        source.transformed_at > target.transformed_at 
        OR target.transformed_at IS NULL
    ) THEN UPDATE SET
        target.country_name = source.country_name,
        target.short_name = source.short_name,
        target.iso_alpha2 = source.iso_alpha2,
        target.crop_land = source.crop_land,
        target.grazing_land = source.grazing_land,
        target.forest_land = source.forest_land,
        target.fishing_ground = source.fishing_ground,
        target.builtup_land = source.builtup_land,
        target.carbon = source.carbon,
        target.value = source.value,
        target.score = source.score,
        target.carbon_pct_of_total = source.carbon_pct_of_total,
        target.extracted_at = source.extracted_at,
        target.transformed_at = source.transformed_at,
        target._updated_at = CURRENT_TIMESTAMP(),
        target._source_file = source._source_file
    WHEN NOT MATCHED THEN INSERT (
        country_code, country_name, short_name, iso_alpha2, year, record_type,
        crop_land, grazing_land, forest_land, fishing_ground, builtup_land, carbon,
        value, score, carbon_pct_of_total,
        extracted_at, transformed_at, _loaded_at, _source_file
    ) VALUES (
        source.country_code, source.country_name, source.short_name, source.iso_alpha2,
        source.year, source.record_type,
        source.crop_land, source.grazing_land, source.forest_land, 
        source.fishing_ground, source.builtup_land, source.carbon,
        source.value, source.score, source.carbon_pct_of_total,
        source.extracted_at, source.transformed_at, source._loaded_at, source._source_file
    );
    
    RETURN 'MERGE complete. Rows affected: ' || SQLROWCOUNT;
END;
$$;

-- =============================================================================
-- 7. Create Views for Analytics
-- =============================================================================

-- Summary view with pivoted metrics
CREATE OR REPLACE VIEW GFN.MART.V_FOOTPRINT_SUMMARY AS
SELECT
    country_code,
    country_name,
    iso_alpha2,
    year,
    -- Total Ecological Footprint
    MAX(CASE WHEN record_type LIKE '%EFConsTot%' OR record_type = 'EFCtot' THEN value END) AS total_ecological_footprint,
    -- Carbon Footprint
    MAX(CASE WHEN record_type LIKE '%Carbon%' OR record_type = 'EFCcarb' THEN carbon END) AS carbon_footprint,
    -- Total Biocapacity
    MAX(CASE WHEN record_type LIKE '%BiocapTot%' OR record_type = 'BioCaptot' THEN value END) AS total_biocapacity,
    -- Ecological Deficit/Reserve
    MAX(CASE WHEN record_type LIKE '%EFConsTot%' OR record_type = 'EFCtot' THEN value END) -
    MAX(CASE WHEN record_type LIKE '%BiocapTot%' OR record_type = 'BioCaptot' THEN value END) AS ecological_deficit,
    -- Carbon percentage
    MAX(carbon_pct_of_total) AS carbon_percentage,
    MAX(_updated_at) AS last_updated
FROM GFN.STAGING.GFN_FOOTPRINT
GROUP BY country_code, country_name, iso_alpha2, year;

-- Time series view for trend analysis
CREATE OR REPLACE VIEW GFN.MART.V_FOOTPRINT_TRENDS AS
SELECT
    country_name,
    iso_alpha2,
    year,
    SUM(CASE WHEN record_type LIKE '%EFConsTot%' THEN value END) AS total_footprint,
    SUM(carbon) AS carbon_footprint,
    MAX(carbon_pct_of_total) AS carbon_percentage
FROM GFN.STAGING.GFN_FOOTPRINT
GROUP BY country_name, iso_alpha2, year
ORDER BY country_name, year;

-- Latest data view (most recent year per country)
CREATE OR REPLACE VIEW GFN.MART.V_LATEST_FOOTPRINT AS
SELECT *
FROM GFN.MART.V_FOOTPRINT_SUMMARY
WHERE (country_code, year) IN (
    SELECT country_code, MAX(year)
    FROM GFN.STAGING.GFN_FOOTPRINT
    GROUP BY country_code
);

-- =============================================================================
-- 8. Helper Procedure: Update ngrok URL
-- =============================================================================
-- Call this when your ngrok URL changes

CREATE OR REPLACE PROCEDURE GFN.RAW.UPDATE_NGROK_URL(new_url VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
COMMENT = 'Update the ngrok network rule with a new URL'
AS
$$
DECLARE
    domain VARCHAR;
BEGIN
    -- Extract domain from URL (remove https:// prefix)
    domain := REGEXP_REPLACE(:new_url, '^https?://', '');
    domain := REGEXP_REPLACE(:domain, '/$', '');  -- Remove trailing slash
    
    -- Recreate network rule with new domain
    EXECUTE IMMEDIATE 'CREATE OR REPLACE NETWORK RULE ngrok_network_rule
        TYPE = HOST_PORT
        MODE = EGRESS
        VALUE_LIST = (''' || :domain || ':443'')';
    
    -- Recreate external access integration
    EXECUTE IMMEDIATE 'CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION ngrok_access_integration
        ALLOWED_NETWORK_RULES = (ngrok_network_rule)
        ENABLED = TRUE';
    
    RETURN 'Updated ngrok URL to: ' || :new_url;
END;
$$;

-- =============================================================================
-- 9. Test Commands
-- =============================================================================
-- Uncomment and run these to test:

-- Update ngrok URL first:
-- CALL GFN.RAW.UPDATE_NGROK_URL('https://your-new-ngrok-url.ngrok-free.dev');

-- Load a single file:
-- CALL GFN.RAW.LOAD_FROM_LOCALSTACK(
--     'transformed/gfn_footprint_20260131_030733_transformed.json',
--     'https://your-ngrok-url.ngrok-free.dev'
-- );

-- Merge to staging:
-- CALL GFN.RAW.MERGE_RAW_TO_STAGING();

-- Check results:
-- SELECT COUNT(*) FROM GFN.RAW.GFN_FOOTPRINT_RAW;
-- SELECT COUNT(*) FROM GFN.STAGING.GFN_FOOTPRINT;
-- SELECT * FROM GFN.MART.V_FOOTPRINT_SUMMARY LIMIT 10;
