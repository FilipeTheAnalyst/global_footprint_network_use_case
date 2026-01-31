-- ============================================================================
-- GFN Pipeline - Snowpipe Configuration
-- ============================================================================
-- Snowpipe provides continuous, serverless data ingestion from S3.
-- When new files land in S3, Snowpipe automatically loads them.
--
-- Flow: S3 → SNS → Snowpipe → RAW table → Stream → Task → STAGING table
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE GFN;
USE SCHEMA RAW;
USE WAREHOUSE COMPUTE_WH;

-- ============================================================================
-- 1. Create Snowpipe for Auto-Ingestion
-- ============================================================================

CREATE OR REPLACE PIPE GFN.RAW.CARBON_FOOTPRINT_PIPE
    AUTO_INGEST = TRUE
    AWS_SNS_TOPIC = 'arn:aws:sns:us-east-1:<YOUR_AWS_ACCOUNT_ID>:gfn-snowpipe-notifications'
    AS
    COPY INTO GFN.RAW.CARBON_FOOTPRINT_RAW (
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
SHOW PIPES LIKE 'CARBON_FOOTPRINT_PIPE';

-- The notification_channel column contains the SQS ARN
-- Configure S3 to send events to this SQS queue

-- ============================================================================
-- 2. Create Stream for Change Data Capture
-- ============================================================================
-- Stream tracks inserts/updates/deletes on the raw table

CREATE OR REPLACE STREAM GFN.RAW.CARBON_FOOTPRINT_STREAM
    ON TABLE GFN.RAW.CARBON_FOOTPRINT_RAW
    APPEND_ONLY = TRUE;  -- Only track inserts (Snowpipe only inserts)

-- ============================================================================
-- 3. Create Task for Continuous Processing
-- ============================================================================
-- Task runs every 5 minutes to process new data from stream

CREATE OR REPLACE TASK GFN.RAW.PROCESS_CARBON_FOOTPRINT_TASK
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = '5 MINUTE'
    WHEN SYSTEM$STREAM_HAS_DATA('GFN.RAW.CARBON_FOOTPRINT_STREAM')
    AS
    INSERT INTO GFN.STAGING.CARBON_FOOTPRINT (
        country_code,
        country_name,
        year,
        record,
        crop_land,
        grazing_land,
        forest_land,
        fishing_ground,
        built_up_land,
        carbon,
        value,
        score,
        total_ecological,
        carbon_percentage,
        _loaded_at,
        _source_file
    )
    SELECT
        raw_data:countryCode::NUMBER,
        raw_data:countryName::VARCHAR,
        raw_data:year::NUMBER,
        raw_data:record::VARCHAR,
        raw_data:cropLand::FLOAT,
        raw_data:grazingLand::FLOAT,
        raw_data:forestLand::FLOAT,
        raw_data:fishingGround::FLOAT,
        raw_data:builtupLand::FLOAT,
        raw_data:carbon::FLOAT,
        raw_data:value::FLOAT,
        raw_data:score::VARCHAR,
        -- Computed: Total ecological footprint (excluding carbon)
        COALESCE(raw_data:cropLand::FLOAT, 0) +
        COALESCE(raw_data:grazingLand::FLOAT, 0) +
        COALESCE(raw_data:forestLand::FLOAT, 0) +
        COALESCE(raw_data:fishingGround::FLOAT, 0) +
        COALESCE(raw_data:builtupLand::FLOAT, 0),
        -- Computed: Carbon as percentage of total
        CASE 
            WHEN raw_data:value::FLOAT > 0 
            THEN (raw_data:carbon::FLOAT / raw_data:value::FLOAT) * 100
            ELSE 0 
        END,
        _loaded_at,
        _source_file
    FROM GFN.RAW.CARBON_FOOTPRINT_STREAM;

-- ============================================================================
-- 4. Create Task for Mart Aggregation
-- ============================================================================
-- Runs after processing task to update mart layer

CREATE OR REPLACE TASK GFN.RAW.UPDATE_MART_TASK
    WAREHOUSE = COMPUTE_WH
    AFTER GFN.RAW.PROCESS_CARBON_FOOTPRINT_TASK
    AS
    MERGE INTO GFN.MART.CARBON_FOOTPRINT_SUMMARY AS target
    USING (
        SELECT
            country_code,
            country_name,
            year,
            SUM(CASE WHEN record = 'EFConsTotGHA' THEN value ELSE 0 END) AS total_footprint,
            SUM(CASE WHEN record = 'EFConsTotGHA' THEN carbon ELSE 0 END) AS carbon_footprint,
            SUM(CASE WHEN record = 'BiocapTotGHA' THEN value ELSE 0 END) AS biocapacity,
            SUM(CASE WHEN record = 'EFConsTotGHA' THEN value ELSE 0 END) -
            SUM(CASE WHEN record = 'BiocapTotGHA' THEN value ELSE 0 END) AS ecological_deficit,
            CASE 
                WHEN SUM(CASE WHEN record = 'EFConsTotGHA' THEN value ELSE 0 END) > 0
                THEN SUM(CASE WHEN record = 'EFConsTotGHA' THEN carbon ELSE 0 END) /
                     SUM(CASE WHEN record = 'EFConsTotGHA' THEN value ELSE 0 END)
                ELSE 0
            END AS carbon_intensity
        FROM GFN.STAGING.CARBON_FOOTPRINT
        WHERE _loaded_at >= DATEADD(hour, -1, CURRENT_TIMESTAMP())
        GROUP BY country_code, country_name, year
    ) AS source
    ON target.country_code = source.country_code 
       AND target.year = source.year
    WHEN MATCHED THEN UPDATE SET
        target.total_footprint = source.total_footprint,
        target.carbon_footprint = source.carbon_footprint,
        target.biocapacity = source.biocapacity,
        target.ecological_deficit = source.ecological_deficit,
        target.carbon_intensity = source.carbon_intensity,
        target._updated_at = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN INSERT (
        country_code, country_name, year,
        total_footprint, carbon_footprint, biocapacity,
        ecological_deficit, carbon_intensity, _updated_at
    ) VALUES (
        source.country_code, source.country_name, source.year,
        source.total_footprint, source.carbon_footprint, source.biocapacity,
        source.ecological_deficit, source.carbon_intensity, CURRENT_TIMESTAMP()
    );

-- ============================================================================
-- 5. Enable Tasks
-- ============================================================================

ALTER TASK GFN.RAW.UPDATE_MART_TASK RESUME;
ALTER TASK GFN.RAW.PROCESS_CARBON_FOOTPRINT_TASK RESUME;

-- ============================================================================
-- 6. Verify Setup
-- ============================================================================

-- Check pipe status
SELECT SYSTEM$PIPE_STATUS('GFN.RAW.CARBON_FOOTPRINT_PIPE');

-- Check task status
SHOW TASKS IN SCHEMA GFN.RAW;

-- Check stream status
SHOW STREAMS IN SCHEMA GFN.RAW;
