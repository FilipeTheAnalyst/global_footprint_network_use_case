-- ============================================================================
-- GFN Pipeline - Snowpipe Configuration (Idempotent & Backfill-Ready)
-- ============================================================================
-- Snowpipe provides continuous, serverless data ingestion from S3.
-- When new files land in S3, Snowpipe automatically loads them.
--
-- Flow: S3 → SNS → Snowpipe → RAW table → Stream → Task → STAGING (MERGE)
--
-- Key Features:
--   - AUTO_INGEST for automatic file detection
--   - MERGE-based loading for idempotency (supports backfill)
--   - Stream + Task pattern for continuous processing
--   - Batch processing with configurable schedule
--
-- S3 Structure:
--   s3://gfn-data-lake/transformed/gfn_footprint_{timestamp}_transformed.json
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE GFN;
USE SCHEMA RAW;
USE WAREHOUSE COMPUTE_WH;

-- ============================================================================
-- 1. Create Snowpipe for Auto-Ingestion
-- ============================================================================
-- Snowpipe watches the transformed/ folder and loads new files automatically.
-- The COPY INTO uses MATCH_BY_COLUMN_NAME for flexible schema mapping.

CREATE OR REPLACE PIPE GFN.RAW.GFN_FOOTPRINT_PIPE
    AUTO_INGEST = TRUE
    AWS_SNS_TOPIC = 'arn:aws:sns:us-east-1:<YOUR_AWS_ACCOUNT_ID>:gfn-snowpipe-notifications'
    COMMENT = 'Auto-ingest transformed GFN footprint data from S3'
    AS
    COPY INTO GFN.RAW.GFN_FOOTPRINT_RAW (
        _source_file,
        _row_number,
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
        transformed_at
    )
    FROM (
        SELECT
            METADATA$FILENAME,
            METADATA$FILE_ROW_NUMBER,
            $1:country_code::NUMBER,
            $1:country_name::VARCHAR,
            $1:short_name::VARCHAR,
            $1:iso_alpha2::VARCHAR,
            $1:year::NUMBER,
            $1:record_type::VARCHAR,
            $1:crop_land::FLOAT,
            $1:grazing_land::FLOAT,
            $1:forest_land::FLOAT,
            $1:fishing_ground::FLOAT,
            $1:builtup_land::FLOAT,
            $1:carbon::FLOAT,
            $1:value::FLOAT,
            $1:score::VARCHAR,
            $1:carbon_pct_of_total::FLOAT,
            $1:extracted_at::TIMESTAMP_TZ,
            $1:transformed_at::TIMESTAMP_TZ
        FROM @gfn_transformed_stage
    )
    FILE_FORMAT = (TYPE = 'JSON' STRIP_OUTER_ARRAY = TRUE)
    ON_ERROR = 'CONTINUE';

-- Get the SQS ARN for S3 event notifications
-- Copy this value and configure S3 bucket notifications
SHOW PIPES LIKE 'GFN_FOOTPRINT_PIPE';

-- The notification_channel column contains the SQS ARN
-- Configure S3 to send events to this SQS queue

-- ============================================================================
-- 2. Create Stream for Change Data Capture
-- ============================================================================
-- Stream tracks new inserts on the raw table for downstream processing.
-- APPEND_ONLY since Snowpipe only inserts (no updates/deletes).

CREATE OR REPLACE STREAM GFN.RAW.GFN_FOOTPRINT_STREAM
    ON TABLE GFN.RAW.GFN_FOOTPRINT_RAW
    APPEND_ONLY = TRUE
    COMMENT = 'CDC stream for new footprint data from Snowpipe';

-- ============================================================================
-- 3. Create Task for Idempotent MERGE to Staging
-- ============================================================================
-- Task runs every 5 minutes to process new data from stream.
-- Uses MERGE for idempotent loading - supports backfill by updating existing records.

CREATE OR REPLACE TASK GFN.RAW.PROCESS_FOOTPRINT_TO_STAGING_TASK
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = '5 MINUTE'
    COMMENT = 'MERGE new footprint data from RAW to STAGING (idempotent)'
    WHEN SYSTEM$STREAM_HAS_DATA('GFN.RAW.GFN_FOOTPRINT_STREAM')
    AS
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
        FROM GFN.RAW.GFN_FOOTPRINT_STREAM
        -- Qualify to get the latest record per key in case of duplicates in batch
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY country_code, year, record_type 
            ORDER BY transformed_at DESC NULLS LAST, _loaded_at DESC
        ) = 1
    ) AS source
    ON target.country_code = source.country_code 
       AND target.year = source.year
       AND target.record_type = source.record_type
    -- Update existing records (backfill scenario)
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
    -- Insert new records
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

-- ============================================================================
-- 4. Create Task for Mart Aggregation
-- ============================================================================
-- Runs after staging task to update mart layer with summary metrics.
-- Pivots record types into columns for easy analysis.

CREATE OR REPLACE TASK GFN.RAW.UPDATE_MART_SUMMARY_TASK
    WAREHOUSE = COMPUTE_WH
    AFTER GFN.RAW.PROCESS_FOOTPRINT_TO_STAGING_TASK
    COMMENT = 'Aggregate staging data into mart summary (idempotent MERGE)'
    AS
    MERGE INTO GFN.MART.GFN_FOOTPRINT_SUMMARY AS target
    USING (
        SELECT
            country_code,
            MAX(country_name) AS country_name,
            MAX(iso_alpha2) AS iso_alpha2,
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
            -- Per Capita versions
            MAX(CASE WHEN record_type LIKE '%PerCap%' AND record_type LIKE '%EF%' THEN value END) AS footprint_per_capita,
            MAX(CASE WHEN record_type LIKE '%CarbonPerCap%' OR record_type = 'EFCcarbPerCap' THEN value END) AS carbon_per_capita,
            MAX(CASE WHEN record_type LIKE '%BiocapPerCap%' OR record_type = 'BioCaptotPerCap' THEN value END) AS biocapacity_per_capita,
            -- Carbon percentage
            MAX(carbon_pct_of_total) AS carbon_percentage,
            -- Component footprints (from land breakdown)
            MAX(crop_land) AS cropland_footprint,
            MAX(grazing_land) AS grazing_footprint,
            MAX(forest_land) AS forest_footprint,
            MAX(fishing_ground) AS fishing_footprint,
            MAX(builtup_land) AS builtup_footprint
        FROM GFN.STAGING.GFN_FOOTPRINT
        WHERE _updated_at >= DATEADD(hour, -2, CURRENT_TIMESTAMP())
        GROUP BY country_code, year
    ) AS source
    ON target.country_code = source.country_code 
       AND target.year = source.year
    WHEN MATCHED THEN UPDATE SET
        target.country_name = source.country_name,
        target.iso_alpha2 = source.iso_alpha2,
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
-- 5. Enable Tasks (in correct order - child first, then parent)
-- ============================================================================

ALTER TASK GFN.RAW.UPDATE_MART_SUMMARY_TASK RESUME;
ALTER TASK GFN.RAW.PROCESS_FOOTPRINT_TO_STAGING_TASK RESUME;

-- ============================================================================
-- 6. Manual Backfill Procedure
-- ============================================================================
-- Use this procedure to manually backfill data from files already in S3.
-- Useful for initial load or re-processing historical data.

CREATE OR REPLACE PROCEDURE GFN.RAW.BACKFILL_FROM_S3(
    file_pattern VARCHAR DEFAULT '.*\\.json$'
)
RETURNS TABLE(file_name VARCHAR, records_loaded NUMBER, status VARCHAR)
LANGUAGE SQL
COMMENT = 'Manually load files from S3 for backfill. Idempotent via MERGE.'
AS
$$
DECLARE
    result_cursor CURSOR FOR
        SELECT 
            relative_path AS file_name,
            0 AS records_loaded,
            'PENDING' AS status
        FROM DIRECTORY(@gfn_transformed_stage)
        WHERE REGEXP_LIKE(relative_path, file_pattern);
BEGIN
    -- Create temp table for results
    CREATE OR REPLACE TEMPORARY TABLE backfill_results (
        file_name VARCHAR,
        records_loaded NUMBER,
        status VARCHAR
    );
    
    -- Load each file using COPY INTO
    FOR file_rec IN result_cursor DO
        BEGIN
            COPY INTO GFN.RAW.GFN_FOOTPRINT_RAW
            FROM @gfn_transformed_stage
            FILES = (file_rec.file_name)
            FILE_FORMAT = (TYPE = 'JSON' STRIP_OUTER_ARRAY = TRUE)
            ON_ERROR = 'CONTINUE';
            
            INSERT INTO backfill_results VALUES (
                file_rec.file_name,
                (SELECT COUNT(*) FROM GFN.RAW.GFN_FOOTPRINT_RAW WHERE _source_file LIKE '%' || file_rec.file_name),
                'SUCCESS'
            );
        EXCEPTION
            WHEN OTHER THEN
                INSERT INTO backfill_results VALUES (
                    file_rec.file_name,
                    0,
                    'FAILED: ' || SQLERRM
                );
        END;
    END FOR;
    
    RETURN TABLE(SELECT * FROM backfill_results);
END;
$$;

-- ============================================================================
-- 7. Refresh Staging from Raw (Full Rebuild)
-- ============================================================================
-- Use this procedure to rebuild staging from raw data.
-- Useful after schema changes or data corrections.

CREATE OR REPLACE PROCEDURE GFN.RAW.REFRESH_STAGING_FROM_RAW()
RETURNS VARCHAR
LANGUAGE SQL
COMMENT = 'Rebuild staging table from raw data (idempotent MERGE)'
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
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY country_code, year, record_type 
            ORDER BY transformed_at DESC NULLS LAST, _loaded_at DESC
        ) = 1
    ) AS source
    ON target.country_code = source.country_code 
       AND target.year = source.year
       AND target.record_type = source.record_type
    WHEN MATCHED THEN UPDATE SET
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
    
    RETURN 'Staging refresh complete. Rows affected: ' || SQLROWCOUNT;
END;
$$;

-- ============================================================================
-- 8. Verify Setup
-- ============================================================================

-- Check pipe status
SELECT SYSTEM$PIPE_STATUS('GFN.RAW.GFN_FOOTPRINT_PIPE');

-- Check task status
SHOW TASKS IN SCHEMA GFN.RAW;

-- Check stream status
SHOW STREAMS IN SCHEMA GFN.RAW;

-- Check for pending files
SELECT COUNT(*) AS pending_files FROM DIRECTORY(@gfn_transformed_stage);
