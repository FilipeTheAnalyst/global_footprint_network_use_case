-- ============================================================================
-- GFN Pipeline - Monitoring & Alerting
-- ============================================================================
-- Comprehensive monitoring for the Snowpipe ingestion pipeline.
-- Includes: dashboards, alerts, data quality checks, and audit logging.
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE GFN;
USE WAREHOUSE COMPUTE_WH;

-- ============================================================================
-- 1. Create Monitoring Schema
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS MONITORING;
USE SCHEMA MONITORING;

-- ============================================================================
-- 2. Pipeline Execution Log
-- ============================================================================

CREATE OR REPLACE TABLE MONITORING.PIPELINE_LOG (
    log_id              NUMBER AUTOINCREMENT PRIMARY KEY,
    log_timestamp       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    pipeline_stage      VARCHAR(50),      -- EXTRACT, TRANSFORM, LOAD, SNOWPIPE
    status              VARCHAR(20),      -- SUCCESS, FAILED, WARNING
    records_processed   NUMBER,
    execution_time_ms   NUMBER,
    source_file         VARCHAR(500),
    error_message       VARCHAR(4000),
    metadata            VARIANT
);

-- ============================================================================
-- 3. Data Quality Metrics Table
-- ============================================================================

CREATE OR REPLACE TABLE MONITORING.DATA_QUALITY_METRICS (
    metric_id           NUMBER AUTOINCREMENT PRIMARY KEY,
    check_timestamp     TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    table_name          VARCHAR(100),
    metric_name         VARCHAR(100),
    metric_value        FLOAT,
    threshold           FLOAT,
    status              VARCHAR(20),      -- PASS, FAIL, WARNING
    details             VARCHAR(4000)
);

-- ============================================================================
-- 4. Snowpipe Monitoring View
-- ============================================================================

CREATE OR REPLACE VIEW MONITORING.V_SNOWPIPE_STATUS AS
SELECT
    pipe_catalog_name AS database_name,
    pipe_schema_name AS schema_name,
    pipe_name,
    definition,
    is_autoingest_enabled,
    notification_channel_name,
    created,
    last_altered
FROM SNOWFLAKE.ACCOUNT_USAGE.PIPES
WHERE pipe_catalog_name = 'GFN'
  AND deleted IS NULL;

-- ============================================================================
-- 5. Snowpipe Load History View
-- ============================================================================

CREATE OR REPLACE VIEW MONITORING.V_SNOWPIPE_LOAD_HISTORY AS
SELECT
    pipe_name,
    file_name,
    stage_location,
    row_count,
    row_parsed,
    file_size,
    first_error_message,
    first_error_line_number,
    error_count,
    status,
    last_load_time,
    DATEDIFF('second', first_commit_time, last_load_time) AS load_duration_seconds
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME => 'GFN.RAW.CARBON_FOOTPRINT_RAW',
    START_TIME => DATEADD(day, -7, CURRENT_TIMESTAMP())
))
ORDER BY last_load_time DESC;

-- ============================================================================
-- 6. Task Execution History View
-- ============================================================================

CREATE OR REPLACE VIEW MONITORING.V_TASK_HISTORY AS
SELECT
    name AS task_name,
    database_name,
    schema_name,
    state,
    scheduled_time,
    completed_time,
    DATEDIFF('second', scheduled_time, completed_time) AS duration_seconds,
    error_code,
    error_message,
    query_id
FROM SNOWFLAKE.ACCOUNT_USAGE.TASK_HISTORY
WHERE database_name = 'GFN'
  AND scheduled_time >= DATEADD(day, -7, CURRENT_TIMESTAMP())
ORDER BY scheduled_time DESC;

-- ============================================================================
-- 7. Data Freshness View
-- ============================================================================

CREATE OR REPLACE VIEW MONITORING.V_DATA_FRESHNESS AS
SELECT
    'RAW.CARBON_FOOTPRINT_RAW' AS table_name,
    COUNT(*) AS total_records,
    MAX(_loaded_at) AS last_load_time,
    DATEDIFF('minute', MAX(_loaded_at), CURRENT_TIMESTAMP()) AS minutes_since_last_load,
    CASE 
        WHEN DATEDIFF('minute', MAX(_loaded_at), CURRENT_TIMESTAMP()) > 60 THEN 'STALE'
        WHEN DATEDIFF('minute', MAX(_loaded_at), CURRENT_TIMESTAMP()) > 30 THEN 'WARNING'
        ELSE 'FRESH'
    END AS freshness_status
FROM GFN.RAW.CARBON_FOOTPRINT_RAW

UNION ALL

SELECT
    'STAGING.CARBON_FOOTPRINT' AS table_name,
    COUNT(*) AS total_records,
    MAX(_loaded_at) AS last_load_time,
    DATEDIFF('minute', MAX(_loaded_at), CURRENT_TIMESTAMP()) AS minutes_since_last_load,
    CASE 
        WHEN DATEDIFF('minute', MAX(_loaded_at), CURRENT_TIMESTAMP()) > 60 THEN 'STALE'
        WHEN DATEDIFF('minute', MAX(_loaded_at), CURRENT_TIMESTAMP()) > 30 THEN 'WARNING'
        ELSE 'FRESH'
    END AS freshness_status
FROM GFN.STAGING.CARBON_FOOTPRINT

UNION ALL

SELECT
    'MART.CARBON_FOOTPRINT_SUMMARY' AS table_name,
    COUNT(*) AS total_records,
    MAX(_updated_at) AS last_load_time,
    DATEDIFF('minute', MAX(_updated_at), CURRENT_TIMESTAMP()) AS minutes_since_last_load,
    CASE 
        WHEN DATEDIFF('minute', MAX(_updated_at), CURRENT_TIMESTAMP()) > 120 THEN 'STALE'
        WHEN DATEDIFF('minute', MAX(_updated_at), CURRENT_TIMESTAMP()) > 60 THEN 'WARNING'
        ELSE 'FRESH'
    END AS freshness_status
FROM GFN.MART.CARBON_FOOTPRINT_SUMMARY;

-- ============================================================================
-- 8. Data Quality Check Stored Procedure
-- ============================================================================

CREATE OR REPLACE PROCEDURE MONITORING.RUN_DATA_QUALITY_CHECKS()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    check_count NUMBER;
    null_count NUMBER;
    duplicate_count NUMBER;
    range_violations NUMBER;
BEGIN
    -- Check 1: Null country codes
    SELECT COUNT(*) INTO :null_count
    FROM GFN.STAGING.CARBON_FOOTPRINT
    WHERE country_code IS NULL;
    
    INSERT INTO MONITORING.DATA_QUALITY_METRICS 
        (table_name, metric_name, metric_value, threshold, status, details)
    VALUES (
        'STAGING.CARBON_FOOTPRINT',
        'null_country_codes',
        :null_count,
        0,
        CASE WHEN :null_count > 0 THEN 'FAIL' ELSE 'PASS' END,
        'Records with NULL country_code'
    );
    
    -- Check 2: Duplicate records (same country, year, record type)
    SELECT COUNT(*) INTO :duplicate_count
    FROM (
        SELECT country_code, year, record, COUNT(*) as cnt
        FROM GFN.STAGING.CARBON_FOOTPRINT
        GROUP BY country_code, year, record
        HAVING COUNT(*) > 1
    );
    
    INSERT INTO MONITORING.DATA_QUALITY_METRICS 
        (table_name, metric_name, metric_value, threshold, status, details)
    VALUES (
        'STAGING.CARBON_FOOTPRINT',
        'duplicate_records',
        :duplicate_count,
        0,
        CASE WHEN :duplicate_count > 0 THEN 'WARNING' ELSE 'PASS' END,
        'Duplicate country/year/record combinations'
    );
    
    -- Check 3: Value range violations (negative values)
    SELECT COUNT(*) INTO :range_violations
    FROM GFN.STAGING.CARBON_FOOTPRINT
    WHERE value < 0 OR carbon < 0;
    
    INSERT INTO MONITORING.DATA_QUALITY_METRICS 
        (table_name, metric_name, metric_value, threshold, status, details)
    VALUES (
        'STAGING.CARBON_FOOTPRINT',
        'negative_values',
        :range_violations,
        0,
        CASE WHEN :range_violations > 0 THEN 'WARNING' ELSE 'PASS' END,
        'Records with negative value or carbon'
    );
    
    -- Check 4: Year range validation
    SELECT COUNT(*) INTO :range_violations
    FROM GFN.STAGING.CARBON_FOOTPRINT
    WHERE year < 1960 OR year > YEAR(CURRENT_DATE()) + 1;
    
    INSERT INTO MONITORING.DATA_QUALITY_METRICS 
        (table_name, metric_name, metric_value, threshold, status, details)
    VALUES (
        'STAGING.CARBON_FOOTPRINT',
        'invalid_years',
        :range_violations,
        0,
        CASE WHEN :range_violations > 0 THEN 'FAIL' ELSE 'PASS' END,
        'Records with year outside valid range (1960-current)'
    );
    
    RETURN 'Data quality checks completed successfully';
END;
$$;

-- ============================================================================
-- 9. Scheduled Data Quality Task
-- ============================================================================

CREATE OR REPLACE TASK MONITORING.DATA_QUALITY_CHECK_TASK
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = '60 MINUTE'
    AS
    CALL MONITORING.RUN_DATA_QUALITY_CHECKS();

ALTER TASK MONITORING.DATA_QUALITY_CHECK_TASK RESUME;

-- ============================================================================
-- 10. Alert Notification Integration (Email)
-- ============================================================================
-- Note: Requires email notification integration setup

CREATE OR REPLACE NOTIFICATION INTEGRATION gfn_email_alerts
    TYPE = EMAIL
    ENABLED = TRUE
    ALLOWED_RECIPIENTS = ('data-team@yourcompany.com');

-- ============================================================================
-- 11. Alert for Pipeline Failures
-- ============================================================================

CREATE OR REPLACE ALERT MONITORING.PIPELINE_FAILURE_ALERT
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = '5 MINUTE'
    IF (EXISTS (
        SELECT 1 
        FROM MONITORING.V_TASK_HISTORY 
        WHERE error_code IS NOT NULL 
          AND scheduled_time >= DATEADD(minute, -10, CURRENT_TIMESTAMP())
    ))
    THEN
        CALL SYSTEM$SEND_EMAIL(
            'gfn_email_alerts',
            'data-team@yourcompany.com',
            'GFN Pipeline Alert: Task Failure Detected',
            'One or more pipeline tasks have failed. Please check the MONITORING.V_TASK_HISTORY view for details.'
        );

ALTER ALERT MONITORING.PIPELINE_FAILURE_ALERT RESUME;

-- ============================================================================
-- 12. Alert for Data Staleness
-- ============================================================================

CREATE OR REPLACE ALERT MONITORING.DATA_STALENESS_ALERT
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = '30 MINUTE'
    IF (EXISTS (
        SELECT 1 
        FROM MONITORING.V_DATA_FRESHNESS 
        WHERE freshness_status = 'STALE'
    ))
    THEN
        CALL SYSTEM$SEND_EMAIL(
            'gfn_email_alerts',
            'data-team@yourcompany.com',
            'GFN Pipeline Alert: Data Staleness Detected',
            'Data in one or more tables is stale. Please check the MONITORING.V_DATA_FRESHNESS view for details.'
        );

ALTER ALERT MONITORING.DATA_STALENESS_ALERT RESUME;

-- ============================================================================
-- 13. Monitoring Dashboard Query
-- ============================================================================
-- Use this query in Snowsight to create a dashboard

CREATE OR REPLACE VIEW MONITORING.V_PIPELINE_DASHBOARD AS
SELECT
    -- Overall Status
    (SELECT COUNT(*) FROM GFN.STAGING.CARBON_FOOTPRINT) AS total_records,
    (SELECT COUNT(DISTINCT country_code) FROM GFN.STAGING.CARBON_FOOTPRINT) AS unique_countries,
    (SELECT COUNT(DISTINCT year) FROM GFN.STAGING.CARBON_FOOTPRINT) AS unique_years,
    
    -- Freshness
    (SELECT MAX(_loaded_at) FROM GFN.RAW.CARBON_FOOTPRINT_RAW) AS last_raw_load,
    (SELECT MAX(_loaded_at) FROM GFN.STAGING.CARBON_FOOTPRINT) AS last_staging_load,
    
    -- Quality
    (SELECT COUNT(*) FROM MONITORING.DATA_QUALITY_METRICS 
     WHERE status = 'FAIL' AND check_timestamp >= DATEADD(day, -1, CURRENT_TIMESTAMP())) AS quality_failures_24h,
    
    -- Pipeline Health
    (SELECT COUNT(*) FROM MONITORING.V_TASK_HISTORY 
     WHERE error_code IS NOT NULL AND scheduled_time >= DATEADD(day, -1, CURRENT_TIMESTAMP())) AS task_failures_24h;
