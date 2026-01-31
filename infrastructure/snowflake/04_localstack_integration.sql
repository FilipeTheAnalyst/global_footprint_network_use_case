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
-- =============================================================================

-- Set your ngrok URL here (without trailing slash)
SET NGROK_BASE_URL = 'https://informative-arabinosic-adriana.ngrok-free.dev';

-- =============================================================================
-- 1. Database and Schema Setup
-- =============================================================================
USE ROLE ACCOUNTADMIN;

CREATE DATABASE IF NOT EXISTS GFN;
CREATE SCHEMA IF NOT EXISTS GFN.RAW;
CREATE SCHEMA IF NOT EXISTS GFN.ANALYTICS;

USE DATABASE GFN;
USE SCHEMA RAW;

-- =============================================================================
-- 2. Create Network Rule for ngrok (required for external access)
-- =============================================================================
CREATE OR REPLACE NETWORK RULE ngrok_network_rule
    TYPE = HOST_PORT
    MODE = EGRESS
    VALUE_LIST = ('informative-arabinosic-adriana.ngrok-free.dev:443');

-- Create external access integration
CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION ngrok_access_integration
    ALLOWED_NETWORK_RULES = (ngrok_network_rule)
    ENABLED = TRUE;

-- =============================================================================
-- 3. Create Target Table for Carbon Footprint Data
-- =============================================================================
CREATE TABLE IF NOT EXISTS GFN.RAW.CARBON_FOOTPRINT_RAW (
    country_code INTEGER,
    country_name VARCHAR(100),
    iso_alpha2 VARCHAR(2),
    year INTEGER,
    carbon_footprint_gha FLOAT,
    total_footprint_gha FLOAT,
    score VARCHAR(10),
    extracted_at TIMESTAMP_NTZ,
    -- Metadata
    _loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _source_file VARCHAR(500)
);

-- =============================================================================
-- 4. Create Stored Procedure to Load Data from LocalStack
-- =============================================================================
CREATE OR REPLACE PROCEDURE GFN.RAW.LOAD_FROM_LOCALSTACK(file_path VARCHAR)
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

def load_data(session: Session, file_path: str) -> str:
    """Load JSON data from LocalStack S3 via ngrok into Snowflake."""
    
    # Construct the full URL
    base_url = "https://informative-arabinosic-adriana.ngrok-free.dev"
    url = f"{base_url}/gfn-data-lake/{file_path}"
    
    try:
        # Fetch data from LocalStack via ngrok
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        
        records = response.json()
        
        if not records:
            return "No records found in file"
        
        # Insert records into the table
        inserted = 0
        for record in records:
            session.sql("""
                INSERT INTO GFN.RAW.CARBON_FOOTPRINT_RAW 
                (country_code, country_name, iso_alpha2, year, 
                 carbon_footprint_gha, total_footprint_gha, score, 
                 extracted_at, _source_file)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, [
                record.get('country_code'),
                record.get('country_name'),
                record.get('iso_alpha2'),
                record.get('year'),
                record.get('carbon_footprint_gha'),
                record.get('total_footprint_gha'),
                record.get('score'),
                record.get('extracted_at'),
                file_path
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
CREATE OR REPLACE PROCEDURE GFN.RAW.LOAD_ALL_FROM_LOCALSTACK()
RETURNS TABLE(file_path VARCHAR, result VARCHAR)
LANGUAGE SQL
AS
$$
DECLARE
    -- Hardcoded list of known files (since we can't list S3 directly)
    -- Update this list with your actual file paths
    files ARRAY := ARRAY_CONSTRUCT(
        'processed/carbon_footprint/2026/01/31/data_015435_processed.json'
    );
    result_table RESULTSET;
BEGIN
    -- Create temp table for results
    CREATE OR REPLACE TEMPORARY TABLE load_results (file_path VARCHAR, result VARCHAR);
    
    -- Load each file
    FOR i IN 0 TO ARRAY_SIZE(:files) - 1 DO
        LET file_path VARCHAR := :files[i]::VARCHAR;
        LET load_result VARCHAR := (CALL GFN.RAW.LOAD_FROM_LOCALSTACK(:file_path));
        INSERT INTO load_results VALUES (:file_path, :load_result);
    END FOR;
    
    result_table := (SELECT * FROM load_results);
    RETURN TABLE(result_table);
END;
$$;

-- =============================================================================
-- 6. Create View for Analytics
-- =============================================================================
CREATE OR REPLACE VIEW GFN.ANALYTICS.V_CARBON_FOOTPRINT AS
SELECT 
    country_name,
    iso_alpha2,
    year,
    carbon_footprint_gha,
    total_footprint_gha,
    ROUND(carbon_footprint_gha / NULLIF(total_footprint_gha, 0) * 100, 2) AS carbon_pct_of_total,
    score,
    _loaded_at
FROM GFN.RAW.CARBON_FOOTPRINT_RAW
WHERE _loaded_at = (
    SELECT MAX(_loaded_at) 
    FROM GFN.RAW.CARBON_FOOTPRINT_RAW r2 
    WHERE r2.country_name = CARBON_FOOTPRINT_RAW.country_name 
    AND r2.year = CARBON_FOOTPRINT_RAW.year
);

-- =============================================================================
-- 7. Test: Load a single file
-- =============================================================================
-- Uncomment to test:
-- CALL GFN.RAW.LOAD_FROM_LOCALSTACK('processed/carbon_footprint/2026/01/31/data_015435_processed.json');

-- Check results:
-- SELECT * FROM GFN.RAW.CARBON_FOOTPRINT_RAW LIMIT 10;
-- SELECT COUNT(*) FROM GFN.RAW.CARBON_FOOTPRINT_RAW;
