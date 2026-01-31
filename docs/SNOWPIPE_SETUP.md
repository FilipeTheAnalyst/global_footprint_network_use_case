# Snowpipe Setup Guide

This guide walks through setting up Snowpipe for automatic data ingestion from S3 to Snowflake.

## Architecture Overview

```
S3 (processed/) → SNS → Snowpipe → RAW → Stream → Task → STAGING → Task → MART
```

**Key Benefits:**
- **Serverless**: No warehouse needed for ingestion
- **Sub-minute latency**: Data available in seconds
- **Automatic deduplication**: Files are tracked and never reprocessed
- **Cost-effective**: Pay only for compute used

## Prerequisites

1. AWS Account with S3 bucket (`gfn-data-lake`)
2. Snowflake Account with ACCOUNTADMIN privileges
3. AWS CLI configured

## Step 1: Deploy AWS IAM Role

```bash
# Deploy CloudFormation stack (first time - leave parameters empty)
aws cloudformation create-stack \
    --stack-name gfn-snowpipe-role \
    --template-body file://infrastructure/aws/snowpipe_iam_role.json \
    --capabilities CAPABILITY_NAMED_IAM \
    --parameters \
        ParameterKey=SnowflakeAccountArn,ParameterValue=PLACEHOLDER \
        ParameterKey=SnowflakeExternalId,ParameterValue=PLACEHOLDER
```

## Step 2: Create Snowflake Storage Integration

Run in Snowflake:

```sql
-- Run: infrastructure/snowflake/01_setup_storage.sql

-- After running, get the integration details:
DESC STORAGE INTEGRATION gfn_s3_integration;
```

Copy these values:
- `STORAGE_AWS_IAM_USER_ARN` (e.g., `arn:aws:iam::123456789012:user/abc123`)
- `STORAGE_AWS_EXTERNAL_ID` (e.g., `GFN_SFCRole=2_abcdefg...`)

## Step 3: Update AWS IAM Role Trust Policy

```bash
# Update the CloudFormation stack with actual values
aws cloudformation update-stack \
    --stack-name gfn-snowpipe-role \
    --template-body file://infrastructure/aws/snowpipe_iam_role.json \
    --capabilities CAPABILITY_NAMED_IAM \
    --parameters \
        ParameterKey=SnowflakeAccountArn,ParameterValue=<STORAGE_AWS_IAM_USER_ARN> \
        ParameterKey=SnowflakeExternalId,ParameterValue=<STORAGE_AWS_EXTERNAL_ID>
```

## Step 4: Configure S3 Event Notifications

```bash
# Get the SNS topic ARN
SNS_ARN=$(aws cloudformation describe-stacks \
    --stack-name gfn-snowpipe-role \
    --query 'Stacks[0].Outputs[?OutputKey==`SNSTopicArn`].OutputValue' \
    --output text)

# Configure S3 to send events to SNS
aws s3api put-bucket-notification-configuration \
    --bucket gfn-data-lake \
    --notification-configuration '{
        "TopicConfigurations": [
            {
                "TopicArn": "'$SNS_ARN'",
                "Events": ["s3:ObjectCreated:*"],
                "Filter": {
                    "Key": {
                        "FilterRules": [
                            {"Name": "prefix", "Value": "processed/"},
                            {"Name": "suffix", "Value": ".json"}
                        ]
                    }
                }
            }
        ]
    }'
```

## Step 5: Create Snowpipe and Tasks

Run in Snowflake:

```sql
-- Run: infrastructure/snowflake/02_snowpipe.sql
-- Run: infrastructure/snowflake/03_monitoring.sql
```

## Step 6: Verify Setup

```sql
-- Check pipe status
SELECT SYSTEM$PIPE_STATUS('GFN.RAW.CARBON_FOOTPRINT_PIPE');

-- Check tasks are running
SHOW TASKS IN SCHEMA GFN.RAW;

-- Monitor the dashboard
SELECT * FROM GFN.MONITORING.V_PIPELINE_DASHBOARD;
```

## Testing the Pipeline

1. **Trigger extraction:**
   ```bash
   make test-extract
   ```

2. **Check S3:**
   ```bash
   aws s3 ls s3://gfn-data-lake/processed/ --recursive
   ```

3. **Check Snowflake (after ~1 minute):**
   ```sql
   SELECT COUNT(*) FROM GFN.RAW.CARBON_FOOTPRINT_RAW;
   SELECT COUNT(*) FROM GFN.STAGING.CARBON_FOOTPRINT;
   ```

## Monitoring

### Snowpipe Load History
```sql
SELECT * FROM GFN.MONITORING.V_SNOWPIPE_LOAD_HISTORY;
```

### Task Execution History
```sql
SELECT * FROM GFN.MONITORING.V_TASK_HISTORY;
```

### Data Freshness
```sql
SELECT * FROM GFN.MONITORING.V_DATA_FRESHNESS;
```

### Data Quality
```sql
-- Run quality checks manually
CALL GFN.MONITORING.RUN_DATA_QUALITY_CHECKS();

-- View results
SELECT * FROM GFN.MONITORING.DATA_QUALITY_METRICS
ORDER BY check_timestamp DESC
LIMIT 20;
```

## Troubleshooting

### Snowpipe not loading files

1. Check pipe status:
   ```sql
   SELECT SYSTEM$PIPE_STATUS('GFN.RAW.CARBON_FOOTPRINT_PIPE');
   ```

2. Check for errors:
   ```sql
   SELECT * FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
       TABLE_NAME => 'GFN.RAW.CARBON_FOOTPRINT_RAW',
       START_TIME => DATEADD(hour, -24, CURRENT_TIMESTAMP())
   )) WHERE error_count > 0;
   ```

3. Verify S3 notifications:
   ```bash
   aws s3api get-bucket-notification-configuration --bucket gfn-data-lake
   ```

### Tasks not running

1. Check task state:
   ```sql
   SHOW TASKS IN SCHEMA GFN.RAW;
   ```

2. Resume if suspended:
   ```sql
   ALTER TASK GFN.RAW.PROCESS_CARBON_FOOTPRINT_TASK RESUME;
   ALTER TASK GFN.RAW.UPDATE_MART_TASK RESUME;
   ```

3. Check task history for errors:
   ```sql
   SELECT * FROM GFN.MONITORING.V_TASK_HISTORY
   WHERE error_code IS NOT NULL;
   ```

## Cost Optimization

1. **Snowpipe**: Charged per file loaded (~$0.06 per 1000 files)
2. **Tasks**: Use smallest warehouse that meets SLA
3. **Streams**: No additional cost
4. **Alerts**: Minimal cost for monitoring queries

## Security Best Practices

1. Use dedicated IAM role with least privilege
2. Enable S3 bucket encryption (SSE-S3 or SSE-KMS)
3. Use Snowflake network policies to restrict access
4. Rotate credentials regularly
5. Enable audit logging in both AWS and Snowflake
