# Snowpipe Setup Guide

Production setup for AWS S3 → Snowflake integration using Snowpipe.

---

## Prerequisites

- AWS Account with S3 bucket access
- Snowflake account with ACCOUNTADMIN privileges
- GFN API key (from [footprintnetwork.org](https://www.footprintnetwork.org/))

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                              SNOWPIPE ARCHITECTURE                                   │
├─────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                      │
│   S3 Bucket                    SNS Topic                 Snowflake                  │
│   ─────────                    ─────────                 ─────────                  │
│   gfn-data-lake/               gfn-snowpipe              GFN_PIPELINE               │
│   └── transformed/  ──────────▶ (notification) ─────────▶ FOOTPRINT_DATA_PIPE      │
│       └── *.json               (ObjectCreated)           (auto-ingest)              │
│                                                                                      │
│                                                          │                          │
│                                                          ▼                          │
│                                                   ┌─────────────────┐               │
│                                                   │ RAW.FOOTPRINT_  │               │
│                                                   │ DATA_RAW        │               │
│                                                   └────────┬────────┘               │
│                                                            │                        │
│                                                            │ Stream + Task          │
│                                                            ▼                        │
│                                                   ┌─────────────────┐               │
│                                                   │ TRANSFORMED.    │               │
│                                                   │ FOOTPRINT_DATA  │               │
│                                                   └────────┬────────┘               │
│                                                            │                        │
│                                                            │ Task                   │
│                                                            ▼                        │
│                                                   ┌─────────────────┐               │
│                                                   │ ANALYTICS.      │               │
│                                                   │ FOOTPRINT_      │               │
│                                                   │ SUMMARY         │               │
│                                                   └─────────────────┘               │
│                                                                                      │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

---

## Step 1: AWS S3 Bucket Setup

### 1.1 Create S3 Bucket

```bash
aws s3 mb s3://gfn-data-lake --region us-east-1
```

### 1.2 Create Folder Structure

```bash
aws s3api put-object --bucket gfn-data-lake --key raw/
aws s3api put-object --bucket gfn-data-lake --key transformed/
aws s3api put-object --bucket gfn-data-lake --key failed/
```

### 1.3 Enable Event Notifications (After SNS Setup)

```bash
aws s3api put-bucket-notification-configuration \
  --bucket gfn-data-lake \
  --notification-configuration '{
    "TopicConfigurations": [{
      "TopicArn": "arn:aws:sns:us-east-1:ACCOUNT_ID:gfn-snowpipe",
      "Events": ["s3:ObjectCreated:*"],
      "Filter": {
        "Key": {
          "FilterRules": [{
            "Name": "prefix",
            "Value": "transformed/"
          }]
        }
      }
    }]
  }'
```

---

## Step 2: Snowflake Storage Integration

### 2.1 Create Storage Integration

Run in Snowflake (requires ACCOUNTADMIN):

```sql
USE ROLE ACCOUNTADMIN;

CREATE OR REPLACE STORAGE INTEGRATION GFN_S3_INTEGRATION
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::ACCOUNT_ID:role/snowflake-gfn-role'
  STORAGE_ALLOWED_LOCATIONS = ('s3://gfn-data-lake/transformed/');
```

### 2.2 Get Integration Details

```sql
DESC INTEGRATION GFN_S3_INTEGRATION;
```

Note the values for:
- `STORAGE_AWS_IAM_USER_ARN` (e.g., `arn:aws:iam::123456789012:user/abc123`)
- `STORAGE_AWS_EXTERNAL_ID` (e.g., `GFN_SFCRole=2_xyz123`)

---

## Step 3: AWS IAM Role Setup

### 3.1 Create IAM Role

Create `snowflake-gfn-role` with trust policy:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "AWS": "STORAGE_AWS_IAM_USER_ARN_FROM_STEP_2"
      },
      "Action": "sts:AssumeRole",
      "Condition": {
        "StringEquals": {
          "sts:ExternalId": "STORAGE_AWS_EXTERNAL_ID_FROM_STEP_2"
        }
      }
    }
  ]
}
```

### 3.2 Attach S3 Policy

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:GetObjectVersion",
        "s3:ListBucket",
        "s3:GetBucketLocation"
      ],
      "Resource": [
        "arn:aws:s3:::gfn-data-lake",
        "arn:aws:s3:::gfn-data-lake/transformed/*"
      ]
    }
  ]
}
```

---

## Step 4: Snowflake Database Setup

### 4.1 Create Database and Schemas

```sql
-- Create database
CREATE DATABASE IF NOT EXISTS GFN_PIPELINE;
USE DATABASE GFN_PIPELINE;

-- Create schemas
CREATE SCHEMA IF NOT EXISTS RAW;
CREATE SCHEMA IF NOT EXISTS TRANSFORMED;
CREATE SCHEMA IF NOT EXISTS ANALYTICS;
CREATE SCHEMA IF NOT EXISTS MONITORING;
```

### 4.2 Create External Stage

```sql
USE SCHEMA RAW;

CREATE OR REPLACE STAGE GFN_TRANSFORMED_STAGE
  STORAGE_INTEGRATION = GFN_S3_INTEGRATION
  URL = 's3://gfn-data-lake/transformed/'
  FILE_FORMAT = (TYPE = 'JSON');
```

### 4.3 Create Raw Table

```sql
CREATE OR REPLACE TABLE RAW.FOOTPRINT_DATA_RAW (
    raw_data VARIANT,
    _loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _source_file VARCHAR
);
```

### 4.4 Create Transformed Table

```sql
CREATE OR REPLACE TABLE TRANSFORMED.FOOTPRINT_DATA (
    unique_key VARCHAR PRIMARY KEY,
    country_code INTEGER,
    country_name VARCHAR,
    iso_alpha2 VARCHAR(2),
    year INTEGER,
    record_type VARCHAR,
    carbon_footprint_gha FLOAT,
    score VARCHAR(10),
    extracted_at TIMESTAMP_NTZ,
    transformed_at TIMESTAMP_NTZ,
    _loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
```

### 4.5 Create Analytics Table

```sql
CREATE OR REPLACE TABLE ANALYTICS.FOOTPRINT_SUMMARY (
    country_code INTEGER,
    country_name VARCHAR,
    iso_alpha2 VARCHAR(2),
    year INTEGER,
    total_carbon_footprint_gha FLOAT,
    record_count INTEGER,
    latest_score VARCHAR(10),
    _updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (country_code, year)
);
```

---

## Step 5: Snowpipe Setup

### 5.1 Create Snowpipe

```sql
USE SCHEMA RAW;

CREATE OR REPLACE PIPE FOOTPRINT_DATA_PIPE
  AUTO_INGEST = TRUE
  AS
  COPY INTO RAW.FOOTPRINT_DATA_RAW (raw_data, _source_file)
  FROM (
    SELECT $1, METADATA$FILENAME
    FROM @GFN_TRANSFORMED_STAGE
  )
  FILE_FORMAT = (TYPE = 'JSON');
```

### 5.2 Get Snowpipe Notification Channel

```sql
SHOW PIPES;
-- Note the notification_channel value (SQS ARN)
```

---

## Step 6: AWS SNS Setup

### 6.1 Create SNS Topic

```bash
aws sns create-topic --name gfn-snowpipe --region us-east-1
```

### 6.2 Subscribe Snowpipe SQS

```bash
aws sns subscribe \
  --topic-arn arn:aws:sns:us-east-1:ACCOUNT_ID:gfn-snowpipe \
  --protocol sqs \
  --notification-endpoint SNOWPIPE_SQS_ARN_FROM_STEP_5
```

### 6.3 Configure S3 Event Notifications

```bash
aws s3api put-bucket-notification-configuration \
  --bucket gfn-data-lake \
  --notification-configuration '{
    "TopicConfigurations": [{
      "TopicArn": "arn:aws:sns:us-east-1:ACCOUNT_ID:gfn-snowpipe",
      "Events": ["s3:ObjectCreated:*"],
      "Filter": {
        "Key": {
          "FilterRules": [{
            "Name": "prefix",
            "Value": "transformed/"
          }]
        }
      }
    }]
  }'
```

---

## Step 7: Stream and Tasks

### 7.1 Create Stream

```sql
CREATE OR REPLACE STREAM RAW.FOOTPRINT_DATA_STREAM
  ON TABLE RAW.FOOTPRINT_DATA_RAW;
```

### 7.2 Create Processing Task

```sql
CREATE OR REPLACE TASK TRANSFORMED.PROCESS_FOOTPRINT_DATA_TASK
  WAREHOUSE = COMPUTE_WH
  SCHEDULE = '1 MINUTE'
  WHEN SYSTEM$STREAM_HAS_DATA('RAW.FOOTPRINT_DATA_STREAM')
AS
MERGE INTO TRANSFORMED.FOOTPRINT_DATA AS target
USING (
    SELECT
        raw_data:unique_key::VARCHAR AS unique_key,
        raw_data:country_code::INTEGER AS country_code,
        raw_data:country_name::VARCHAR AS country_name,
        raw_data:iso_alpha2::VARCHAR AS iso_alpha2,
        raw_data:year::INTEGER AS year,
        raw_data:record_type::VARCHAR AS record_type,
        raw_data:carbon_footprint_gha::FLOAT AS carbon_footprint_gha,
        raw_data:score::VARCHAR AS score,
        raw_data:extracted_at::TIMESTAMP_NTZ AS extracted_at,
        raw_data:transformed_at::TIMESTAMP_NTZ AS transformed_at
    FROM RAW.FOOTPRINT_DATA_STREAM
    WHERE METADATA$ACTION = 'INSERT'
) AS source
ON target.unique_key = source.unique_key
WHEN MATCHED THEN UPDATE SET
    target.carbon_footprint_gha = source.carbon_footprint_gha,
    target.score = source.score,
    target.extracted_at = source.extracted_at,
    target.transformed_at = source.transformed_at,
    target._updated_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (
    unique_key, country_code, country_name, iso_alpha2, year,
    record_type, carbon_footprint_gha, score, extracted_at, transformed_at
) VALUES (
    source.unique_key, source.country_code, source.country_name,
    source.iso_alpha2, source.year, source.record_type,
    source.carbon_footprint_gha, source.score,
    source.extracted_at, source.transformed_at
);
```

### 7.3 Create Analytics Task

```sql
CREATE OR REPLACE TASK ANALYTICS.UPDATE_FOOTPRINT_SUMMARY_TASK
  WAREHOUSE = COMPUTE_WH
  AFTER TRANSFORMED.PROCESS_FOOTPRINT_DATA_TASK
AS
MERGE INTO ANALYTICS.FOOTPRINT_SUMMARY AS target
USING (
    SELECT
        country_code,
        country_name,
        iso_alpha2,
        year,
        SUM(carbon_footprint_gha) AS total_carbon_footprint_gha,
        COUNT(*) AS record_count,
        MAX(score) AS latest_score
    FROM TRANSFORMED.FOOTPRINT_DATA
    GROUP BY country_code, country_name, iso_alpha2, year
) AS source
ON target.country_code = source.country_code AND target.year = source.year
WHEN MATCHED THEN UPDATE SET
    target.total_carbon_footprint_gha = source.total_carbon_footprint_gha,
    target.record_count = source.record_count,
    target.latest_score = source.latest_score,
    target._updated_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (
    country_code, country_name, iso_alpha2, year,
    total_carbon_footprint_gha, record_count, latest_score
) VALUES (
    source.country_code, source.country_name, source.iso_alpha2, source.year,
    source.total_carbon_footprint_gha, source.record_count, source.latest_score
);
```

### 7.4 Enable Tasks

```sql
ALTER TASK ANALYTICS.UPDATE_FOOTPRINT_SUMMARY_TASK RESUME;
ALTER TASK TRANSFORMED.PROCESS_FOOTPRINT_DATA_TASK RESUME;
```

---

## Step 8: Verification

### 8.1 Test File Upload

```bash
# Upload a test file
aws s3 cp test_data.json s3://gfn-data-lake/transformed/
```

### 8.2 Check Snowpipe Status

```sql
-- Check pipe status
SELECT SYSTEM$PIPE_STATUS('RAW.FOOTPRINT_DATA_PIPE');

-- Check load history
SELECT * FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME => 'FOOTPRINT_DATA_RAW',
    START_TIME => DATEADD(HOUR, -24, CURRENT_TIMESTAMP())
));
```

### 8.3 Verify Data Flow

```sql
-- Check raw data
SELECT COUNT(*) FROM RAW.FOOTPRINT_DATA_RAW;

-- Check transformed data
SELECT COUNT(*) FROM TRANSFORMED.FOOTPRINT_DATA;

-- Check analytics
SELECT * FROM ANALYTICS.FOOTPRINT_SUMMARY LIMIT 10;
```

---

## Monitoring

### Snowpipe Load History

```sql
SELECT
    FILE_NAME,
    STATUS,
    ROW_COUNT,
    FIRST_ERROR_MESSAGE,
    LAST_LOAD_TIME
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME => 'FOOTPRINT_DATA_RAW',
    START_TIME => DATEADD(DAY, -7, CURRENT_TIMESTAMP())
))
ORDER BY LAST_LOAD_TIME DESC;
```

### Task History

```sql
SELECT
    NAME,
    STATE,
    SCHEDULED_TIME,
    COMPLETED_TIME,
    ERROR_MESSAGE
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    SCHEDULED_TIME_RANGE_START => DATEADD(DAY, -1, CURRENT_TIMESTAMP())
))
ORDER BY SCHEDULED_TIME DESC;
```

---

## Troubleshooting

### Snowpipe Not Ingesting

1. Check pipe status:
   ```sql
   SELECT SYSTEM$PIPE_STATUS('RAW.FOOTPRINT_DATA_PIPE');
   ```

2. Verify S3 event notifications:
   ```bash
   aws s3api get-bucket-notification-configuration --bucket gfn-data-lake
   ```

3. Check SNS subscription:
   ```bash
   aws sns list-subscriptions-by-topic --topic-arn arn:aws:sns:us-east-1:ACCOUNT_ID:gfn-snowpipe
   ```

### Task Not Running

1. Check task status:
   ```sql
   SHOW TASKS;
   ```

2. Verify stream has data:
   ```sql
   SELECT SYSTEM$STREAM_HAS_DATA('RAW.FOOTPRINT_DATA_STREAM');
   ```

3. Check task history for errors:
   ```sql
   SELECT * FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY())
   WHERE NAME = 'PROCESS_FOOTPRINT_DATA_TASK'
   ORDER BY SCHEDULED_TIME DESC
   LIMIT 10;
   ```

### IAM Permission Issues

1. Verify role trust relationship
2. Check S3 bucket policy
3. Test with AWS CLI:
   ```bash
   aws sts assume-role --role-arn arn:aws:iam::ACCOUNT_ID:role/snowflake-gfn-role \
     --role-session-name test --external-id EXTERNAL_ID
   ```

---

## Cost Optimization

| Component | Cost Factor | Optimization |
|-----------|-------------|--------------|
| Snowpipe | Per-file overhead | Batch files (larger, fewer) |
| Tasks | Warehouse runtime | Use smallest warehouse |
| Storage | S3 + Snowflake | Lifecycle policies |
| Compute | Query execution | Cluster keys, partitioning |

### Recommended Settings

```sql
-- Use XS warehouse for tasks
ALTER WAREHOUSE COMPUTE_WH SET WAREHOUSE_SIZE = 'X-SMALL';

-- Auto-suspend after 60 seconds
ALTER WAREHOUSE COMPUTE_WH SET AUTO_SUSPEND = 60;

-- Enable auto-resume
ALTER WAREHOUSE COMPUTE_WH SET AUTO_RESUME = TRUE;
```
