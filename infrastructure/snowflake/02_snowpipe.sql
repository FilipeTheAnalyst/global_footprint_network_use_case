-- ============================================================================
-- GFN Pipeline - Snowpipe Configuration (Comprehensive Data Model)
-- ============================================================================
-- Snowpipe provides continuous, serverless data ingestion from S3.
-- When new files land in S3, Snowpipe automatically loads them.
--
-- Flow: S3 → SNS → Snowpipe → RAW table → Stream → Task → STAGING table
--
-- Data Model: Comprehensive extraction of ALL GFN data types:
--   - 7 Ecological Footprint types (total + 6 components)
--   - 7 Biocapacity types (total + 6 components)
--   - 14 Per Capita versions of the above
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE GFN;
USE SCHEMA RAW;
USE WAREHOUSE COMPUTE_WH;

-- ============================================================================
-- 1. Create Snowpipe for Auto-Ingestion
-- ============================================================================

CREATE OR REPLACE PIPE GFN.RAW.GFN_FOOTPRINT_DATA_PIPE
    AUTO_INGEST = TRUE
    AWS_SNS_TOPIC = 'arn:aws:sns:us-east-1:<YOUR_AWS_ACCOUNT_ID>:gfn-snowpipe-notifications'
    AS
    COPY INTO GFN.RAW.GFN_FOOTPRINT_DATA_RAW (
        _source_file,
        _row_number,
        raw_data
    )
    FROM (
        SELECT
            METADATA$FILENAME,
            METADATA$FILE_ROW_NUMBER,
            $1
        FROM @gfn_processed_stage
    )
    FILE_FORMAT = (TYPE = 'JSON' STRIP_OUTER_ARRAY = TRUE)
    ON_ERROR = 'CONTINUE';

-- Get the SQS ARN for S3 event notifications
-- Copy this value and configure S3 bucket notifications
SHOW PIPES LIKE 'GFN_FOOTPRINT_DATA_PIPE';

-- The notification_channel column contains the SQS ARN
-- Configure S3 to send events to this SQS queue

-- ============================================================================
-- 2. Create Stream for Change Data Capture
-- ============================================================================
-- Stream tracks inserts/updates/deletes on the raw table

CREATE OR REPLACE STREAM GFN.RAW.GFN_FOOTPRINT_DATA_STREAM
    ON TABLE GFN.RAW.GFN_FOOTPRINT_DATA_RAW
    APPEND_ONLY = TRUE;  -- Only track inserts (Snowpipe only inserts)

-- ============================================================================
-- 3. Create Task for Continuous Processing
-- ============================================================================
-- Task runs every 5 minutes to process new data from stream

CREATE OR REPLACE TASK GFN.RAW.PROCESS_GFN_FOOTPRINT_DATA_TASK
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = '5 MINUTE'
    WHEN SYSTEM$STREAM_HAS_DATA('GFN.RAW.GFN_FOOTPRINT_DATA_STREAM')
    AS
    INSERT INTO GFN.STAGING.GFN_FOOTPRINT_DATA (
        country_code,
        country_name,
        iso_alpha2,
        year,
        record_type,
        record_category,
        value,
        per_capita,
        score,
        _loaded_at,
        _source_file
    )
    SELECT
        raw_data:countryCode::NUMBER,
        raw_data:countryName::VARCHAR,
        raw_data:isoAlpha2::VARCHAR,
        raw_data:year::NUMBER,
        raw_data:recordType::VARCHAR,
        raw_data:recordCategory::VARCHAR,
        raw_data:value::FLOAT,
        raw_data:perCapita::BOOLEAN,
        raw_data:score::VARCHAR,
        _loaded_at,
        _source_file
    FROM GFN.RAW.GFN_FOOTPRINT_DATA_STREAM;

-- ============================================================================
-- 4. Create Task for Mart Aggregation
-- ============================================================================
-- Runs after processing task to update mart layer with summary metrics

CREATE OR REPLACE TASK GFN.RAW.UPDATE_MART_TASK
    WAREHOUSE = COMPUTE_WH
    AFTER GFN.RAW.PROCESS_GFN_FOOTPRINT_DATA_TASK
    AS
    MERGE INTO GFN.MART.GFN_FOOTPRINT_SUMMARY AS target
    USING (
        SELECT
            country_code,
            country_name,
            iso_alpha2,
            year,
            -- Total Ecological Footprint
            MAX(CASE WHEN record_type = 'EFCtot' THEN value END) AS total_ecological_footprint,
            -- Carbon Footprint
            MAX(CASE WHEN record_type = 'EFCcarb' THEN value END) AS carbon_footprint,
            -- Total Biocapacity
            MAX(CASE WHEN record_type = 'BioCaptot' THEN value END) AS total_biocapacity,
            -- Ecological Deficit/Reserve (Footprint - Biocapacity)
            MAX(CASE WHEN record_type = 'EFCtot' THEN value END) -
            MAX(CASE WHEN record_type = 'BioCaptot' THEN value END) AS ecological_deficit,
            -- Per Capita versions
            MAX(CASE WHEN record_type = 'EFCtotPerCap' THEN value END) AS footprint_per_capita,
            MAX(CASE WHEN record_type = 'EFCcarbPerCap' THEN value END) AS carbon_per_capita,
            MAX(CASE WHEN record_type = 'BioCaptotPerCap' THEN value END) AS biocapacity_per_capita,
            -- Carbon as percentage of total footprint
            CASE 
                WHEN MAX(CASE WHEN record_type = 'EFCtot' THEN value END) > 0
                THEN MAX(CASE WHEN record_type = 'EFCcarb' THEN value END) /
                     MAX(CASE WHEN record_type = 'EFCtot' THEN value END) * 100
                ELSE 0
            END AS carbon_percentage,
            -- Component footprints
            MAX(CASE WHEN record_type = 'EFCcrop' THEN value END) AS cropland_footprint,
            MAX(CASE WHEN record_type = 'EFCgraz' THEN value END) AS grazing_footprint,
            MAX(CASE WHEN record_type = 'EFCfrst' THEN value END) AS forest_footprint,
            MAX(CASE WHEN record_type = 'EFCfish' THEN value END) AS fishing_footprint,
            MAX(CASE WHEN record_type = 'EFCbult' THEN value END) AS builtup_footprint
        FROM GFN.STAGING.GFN_FOOTPRINT_DATA
        WHERE _loaded_at >= DATEADD(hour, -1, CURRENT_TIMESTAMP())
        GROUP BY country_code, country_name, iso_alpha2, year
    ) AS source
    ON target.country_code = source.country_code 
       AND target.year = source.year
    WHEN MATCHED THEN UPDATE SET
        target.total_ecological_footprint = source.total_ecological_footprint,
        target.carbon_footprint = source.carbon_footprint,
        target.total_biocapacity = source.total_biocapacity,
        target.ecological_deficit = source.ecological_deficit,
        target.footprint_per_capita = source.footprint_per_capita,
        target.carbon_per_capita = source.carbon_per_capita,
        target.biocapacity_per_capita = source.biocapacity_per_capita,
        target.carbon_percentage = source.carbon_percentage,
        target.cropland_footprint = source.cropland_footprint,
        target.grazing_footprint = source.grazing_footprint,
        target.forest_footprint = source.forest_footprint,
        target.fishing_footprint = source.fishing_footprint,
        target.builtup_footprint = source.builtup_footprint,
        target._updated_at = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN INSERT (
        country_code, country_name, iso_alpha2, year,
        total_ecological_footprint, carbon_footprint, total_biocapacity,
        ecological_deficit, footprint_per_capita, carbon_per_capita,
        biocapacity_per_capita, carbon_percentage,
        cropland_footprint, grazing_footprint, forest_footprint,
        fishing_footprint, builtup_footprint, _updated_at
    ) VALUES (
        source.country_code, source.country_name, source.iso_alpha2, source.year,
        source.total_ecological_footprint, source.carbon_footprint, source.total_biocapacity,
        source.ecological_deficit, source.footprint_per_capita, source.carbon_per_capita,
        source.biocapacity_per_capita, source.carbon_percentage,
        source.cropland_footprint, source.grazing_footprint, source.forest_footprint,
        source.fishing_footprint, source.builtup_footprint, CURRENT_TIMESTAMP()
    );

-- ============================================================================
-- 5. Enable Tasks
-- ============================================================================

ALTER TASK GFN.RAW.UPDATE_MART_TASK RESUME;
ALTER TASK GFN.RAW.PROCESS_GFN_FOOTPRINT_DATA_TASK RESUME;

-- ============================================================================
-- 6. Verify Setup
-- ============================================================================

-- Check pipe status
SELECT SYSTEM$PIPE_STATUS('GFN.RAW.GFN_FOOTPRINT_DATA_PIPE');

-- Check task status
SHOW TASKS IN SCHEMA GFN.RAW;

-- Check stream status
SHOW STREAMS IN SCHEMA GFN.RAW;
