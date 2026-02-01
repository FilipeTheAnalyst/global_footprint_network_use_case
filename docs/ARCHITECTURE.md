# Architecture Documentation

## Overview

This project implements a **dual-architecture approach** for the Global Footprint Network data pipeline:

| Environment | Data Loading | Orchestration | Database | Purpose |
|-------------|--------------|---------------|----------|---------|
| **Production** | Snowpipe | AWS Step Functions + Lambda | Snowflake | Scalable, event-driven, enterprise-grade |
| **Local Development** | dlt (data load tool) | Python scripts | DuckDB | Rapid iteration, testing, flexibility |

This separation allows for:
- **Production reliability** with managed services and automatic scaling
- **Development agility** with fast feedback loops and local testing
- **Technology showcase** demonstrating proficiency in multiple approaches

---

## Production Architecture

### AWS Orchestration + Snowpipe Loading

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                       AWS ORCHESTRATION + SNOWPIPE LOADING                           │
├─────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                      │
│   EventBridge          Step Functions              Lambda Functions                  │
│   ───────────          ───────────────             ─────────────────                 │
│   Daily Schedule  ───▶  State Machine  ───▶  Extract ───▶ Transform ───▶ Notify     │
│   (cron)               (orchestration)       (API→S3)    (validate)    (SNS→SQS)    │
│                              │                                                       │
│                              ▼                                                       │
│                         SQS + DLQ                                                    │
│                         (retry/errors)                                               │
│                                                                                      │
│   ┌─────────────────────────────────────────────────────────────────────────────┐   │
│   │                           S3 DATA LAKE                                       │   │
│   │     raw/                    transformed/                (audit trail)        │   │
│   │     └── immutable  ───▶     └── validated  ───▶         replay capability    │   │
│   └─────────────────────────────────────────────────────────────────────────────┘   │
│                                      │                                               │
│                                      ▼                                               │
│                              ┌──────────────┐                                        │
│                              │   Snowpipe   │                                        │
│                              │  • Auto-     │                                        │
│                              │    ingest    │                                        │
│                              │  • Event-    │                                        │
│                              │    driven    │                                        │
│                              └──────┬───────┘                                        │
│                                     │                                                │
│                                     ▼                                                │
│                              ┌──────────────┐                                        │
│                              │  Snowflake   │                                        │
│                              │  RAW → TRANSFORMED                                    │
│                              │  (Stream + Task)                                      │
│                              └──────────────┘                                        │
│                                                                                      │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

### Component Details

#### EventBridge Scheduler
- **Purpose**: Trigger daily pipeline execution
- **Schedule**: `cron(0 6 * * ? *)` - Daily at 6 AM UTC
- **Target**: Step Functions state machine

#### Step Functions State Machine
- **Purpose**: Orchestrate multi-step ETL workflow
- **Features**:
  - Visual workflow monitoring
  - Built-in retry logic with exponential backoff
  - Parallel execution support
  - Error handling with catch blocks

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    STEP FUNCTIONS STATE MACHINE                          │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│   ┌──────────┐     ┌───────────┐     ┌──────────┐     ┌──────────┐     │
│   │  Start   │────▶│  Extract  │────▶│Transform │────▶│  Notify  │     │
│   └──────────┘     │  Lambda   │     │  Lambda  │     │   SNS    │     │
│                    └─────┬─────┘     └────┬─────┘     └────┬─────┘     │
│                          │                │                │           │
│                          ▼                ▼                ▼           │
│                    ┌──────────┐     ┌──────────┐     ┌──────────┐     │
│                    │  Retry   │     │  Retry   │     │   DLQ    │     │
│                    │  (3x)    │     │  (3x)    │     │  (fail)  │     │
│                    └──────────┘     └──────────┘     └──────────┘     │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

#### Lambda Functions

| Function | Purpose | Input | Output |
|----------|---------|-------|--------|
| `extract` | Fetch data from GFN API | API endpoint | S3 raw/ |
| `transform` | Validate and transform data | S3 raw/ | S3 transformed/ |
| `notify` | Send completion notification | Transform result | SNS topic |

#### Snowpipe (Continuous Data Loading)

**Why Snowpipe over dlt for Production:**

| Aspect | Snowpipe | dlt |
|--------|----------|-----|
| **Latency** | Near real-time (seconds) | Batch-oriented |
| **Scalability** | Auto-scales with Snowflake | Limited by compute |
| **Cost** | Pay per file loaded | Compute costs |
| **Maintenance** | Fully managed | Requires runtime |
| **Integration** | Native S3 events | Requires orchestration |

**Snowpipe Configuration:**
```sql
-- Auto-ingest from S3 transformed/ bucket
CREATE PIPE gfn_pipe
  AUTO_INGEST = TRUE
  AS COPY INTO raw.ecological_footprint
  FROM @gfn_stage/transformed/
  FILE_FORMAT = (TYPE = 'PARQUET');
```

See [SNOWPIPE_SETUP.md](./SNOWPIPE_SETUP.md) for complete setup instructions.

---

## Local Development Architecture

### dlt + DuckDB Stack

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                         LOCAL DEVELOPMENT ARCHITECTURE                               │
├─────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                      │
│   ┌──────────────┐     ┌──────────────┐     ┌──────────────┐     ┌──────────────┐  │
│   │   GFN API    │────▶│     dlt      │────▶│   DuckDB     │────▶│    Soda      │  │
│   │   (source)   │     │  (extract/   │     │   (local     │     │   (quality   │  │
│   │              │     │   load)      │     │    warehouse)│     │    checks)   │  │
│   └──────────────┘     └──────────────┘     └──────────────┘     └──────────────┘  │
│                              │                                                       │
│                              ▼                                                       │
│                    ┌─────────────────────┐                                          │
│                    │   dlt Features      │                                          │
│                    │   • Schema evolution│                                          │
│                    │   • Data contracts  │                                          │
│                    │   • Incremental     │                                          │
│                    │   • Multi-dest      │                                          │
│                    └─────────────────────┘                                          │
│                                                                                      │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

### Why dlt for Local Development

| Feature | Benefit |
|---------|---------|
| **Schema Evolution** | Automatically handles API changes without code updates |
| **Data Contracts** | Define and enforce data expectations at extraction |
| **Incremental Loading** | Efficient updates with state management |
| **Multi-Destination** | Same code works with DuckDB, Snowflake, BigQuery |
| **Python Native** | Easy debugging and testing |
| **Fast Iteration** | No cloud dependencies for development |

### DuckDB as Local Warehouse

**Why DuckDB:**
- **Zero setup**: Single file database, no server required
- **SQL compatible**: Same queries work in Snowflake
- **Fast**: Columnar storage optimized for analytics
- **Portable**: Share database files for reproducibility
- **Technical challenge requirement**: Demonstrates versatility

```python
# Local development with dlt + DuckDB
import dlt
from dlt.destinations import duckdb

pipeline = dlt.pipeline(
    pipeline_name="gfn_local",
    destination=duckdb("data/gfn.duckdb"),
    dataset_name="ecological_footprint"
)

# Same extraction logic, different destination
pipeline.run(extract_gfn_data())
```

---

## Infrastructure as Code

### Terraform (Recommended)

This project uses **Terraform** for infrastructure provisioning. See the `terraform/` directory for complete configurations.

#### Terraform vs CloudFormation Comparison

| Aspect | Terraform | CloudFormation |
|--------|-----------|----------------|
| **Multi-Cloud Support** | ✅ AWS, GCP, Azure, Snowflake | ❌ AWS only |
| **Vendor Lock-in** | ✅ None - portable HCL | ❌ AWS-specific |
| **Snowflake Provider** | ✅ Native support | ❌ Not available |
| **State Management** | Remote backends (S3, Terraform Cloud) | Managed by AWS |
| **Language** | HCL (declarative, readable) | JSON/YAML (verbose) |
| **Community Modules** | ✅ Extensive registry | ⚠️ Limited |
| **Plan/Preview** | ✅ `terraform plan` | ⚠️ Change sets (limited) |
| **Import Existing** | ✅ `terraform import` | ⚠️ Complex |
| **Learning Curve** | Moderate | Lower for AWS-only |

#### Why Terraform for This Project

1. **Snowflake Integration**: Native provider for Snowflake resources (pipes, stages, warehouses)
2. **Multi-Cloud Ready**: Same tool for AWS infrastructure and Snowflake configuration
3. **No Vendor Lock-in**: Portable skills and configurations
4. **Better State Management**: Remote state with locking prevents conflicts
5. **Mature Ecosystem**: Rich module registry and community support

#### Terraform Structure

```
terraform/
├── main.tf              # Provider configuration
├── variables.tf         # Input variables
├── outputs.tf           # Output values
├── s3.tf                # S3 buckets and policies
├── lambda.tf            # Lambda functions
├── step_functions.tf    # State machine definition
├── eventbridge.tf       # Scheduler rules
├── sqs.tf               # Queues and DLQ
├── sns.tf               # Notification topics
├── iam.tf               # IAM roles and policies
├── snowflake.tf         # Snowflake resources (pipe, stage)
└── modules/
    └── data_pipeline/   # Reusable pipeline module
```

#### Key Terraform Resources

```hcl
# Example: Snowflake Pipe with Terraform
resource "snowflake_pipe" "gfn_pipe" {
  database = snowflake_database.gfn.name
  schema   = snowflake_schema.raw.name
  name     = "GFN_ECOLOGICAL_FOOTPRINT_PIPE"
  
  copy_statement = <<-SQL
    COPY INTO ${snowflake_table.ecological_footprint.name}
    FROM @${snowflake_stage.gfn_stage.name}/transformed/
    FILE_FORMAT = (TYPE = 'PARQUET')
  SQL
  
  auto_ingest = true
}

# Example: Step Functions State Machine
resource "aws_sfn_state_machine" "gfn_pipeline" {
  name     = "gfn-data-pipeline"
  role_arn = aws_iam_role.step_functions.arn
  
  definition = jsonencode({
    StartAt = "Extract"
    States = {
      Extract = {
        Type     = "Task"
        Resource = aws_lambda_function.extract.arn
        Next     = "Transform"
        Retry    = [{ ErrorEquals = ["States.ALL"], MaxAttempts = 3 }]
      }
      # ... additional states
    }
  })
}
```

#### CloudFormation Alternative

While Terraform is recommended, CloudFormation can be used for AWS-only deployments:

**Pros:**
- Native AWS integration
- No additional tooling required
- AWS-managed state
- Drift detection built-in

**Cons:**
- Cannot manage Snowflake resources
- Verbose JSON/YAML syntax
- Limited preview capabilities
- AWS vendor lock-in

---

## Data Flow

### Production Flow

```
1. EventBridge triggers Step Functions (daily 6 AM UTC)
2. Extract Lambda fetches data from GFN API
3. Raw data stored in S3 raw/ (immutable, partitioned by date)
4. Transform Lambda validates and transforms data
5. Transformed data stored in S3 transformed/ (Parquet format)
6. S3 event notification triggers Snowpipe
7. Snowpipe auto-ingests into Snowflake RAW schema
8. Snowflake Stream + Task transforms to TRANSFORMED schema
9. SNS notification sent on completion
10. Errors routed to SQS DLQ for investigation
```

### Local Development Flow

```
1. Run `make extract` or `python -m gfn_pipeline.extract`
2. dlt fetches data from GFN API
3. Data validated against contracts
4. Loaded into DuckDB (data/gfn.duckdb)
5. Soda checks run for quality validation
6. Results available for local querying
```

---

## Two-Tier Data Validation

### Validation Architecture

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                           TWO-TIER VALIDATION                                        │
├─────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                      │
│   TIER 1: Staging Checks (Pre-Load)          TIER 2: Quality Checks (Post-Load)     │
│   ─────────────────────────────────          ──────────────────────────────────     │
│                                                                                      │
│   ┌─────────────────────────────┐            ┌─────────────────────────────┐        │
│   │  soda/staging_checks.yml   │            │  soda/checks.yml            │        │
│   │                             │            │                             │        │
│   │  • Schema validation        │            │  • Row count thresholds     │        │
│   │  • Required fields          │            │  • Freshness checks         │        │
│   │  • Data type checks         │            │  • Referential integrity    │        │
│   │  • Null constraints         │            │  • Business rules           │        │
│   │  • Format validation        │            │  • Anomaly detection        │        │
│   └──────────────┬──────────────┘            └──────────────┬──────────────┘        │
│                  │                                          │                        │
│                  ▼                                          ▼                        │
│   ┌─────────────────────────────┐            ┌─────────────────────────────┐        │
│   │  validators.py              │            │  Soda Core                  │        │
│   │  (Python validation)        │            │  (SQL-based checks)         │        │
│   └─────────────────────────────┘            └─────────────────────────────┘        │
│                                                                                      │
│   Runs: Before S3 transformed/               Runs: After Snowflake load             │
│   Fails: Prevents bad data loading           Fails: Alerts for investigation        │
│                                                                                      │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

### Staging Checks (Pre-Load)

Located in `soda/staging_checks.yml`:
- Schema validation (required columns, data types)
- Null constraints on critical fields
- Format validation (ISO country codes, years)
- Value range checks (positive numbers, valid ranges)

### Quality Checks (Post-Load)

Located in `soda/checks.yml`:
- Row count monitoring with thresholds
- Data freshness validation
- Referential integrity between tables
- Business rule validation
- Statistical anomaly detection

---

## S3 Data Lake Structure

```
s3://gfn-data-lake/
├── raw/
│   └── ecological_footprint/
│       └── year=2024/
│           └── month=01/
│               └── day=15/
│                   └── data_20240115_060000.json
├── transformed/
│   └── ecological_footprint/
│       └── year=2024/
│           └── month=01/
│               └── day=15/
│                   └── data_20240115_060000.parquet
├── failed/
│   └── ecological_footprint/
│       └── 2024/01/15/
│           └── error_20240115_060000.json
└── metadata/
    └── schemas/
        └── ecological_footprint_v1.json
```

### Partitioning Strategy

- **Time-based partitioning**: `year/month/day` for efficient querying
- **Immutable raw layer**: Never modify, only append
- **Transformed layer**: Parquet format for Snowpipe compatibility
- **Failed layer**: Quarantine for investigation

---

## Error Handling

### Retry Strategy

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                              ERROR HANDLING FLOW                                     │
├─────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                      │
│   Lambda Execution                                                                   │
│        │                                                                             │
│        ▼                                                                             │
│   ┌─────────┐     Success     ┌─────────────┐                                       │
│   │ Execute │────────────────▶│ Next State  │                                       │
│   └────┬────┘                 └─────────────┘                                       │
│        │                                                                             │
│        │ Failure                                                                     │
│        ▼                                                                             │
│   ┌─────────────────────────────────────────────────────────────────┐               │
│   │                    RETRY POLICY                                  │               │
│   │   Attempt 1: Wait 1s  → Retry                                   │               │
│   │   Attempt 2: Wait 2s  → Retry (exponential backoff)             │               │
│   │   Attempt 3: Wait 4s  → Retry                                   │               │
│   │   Attempt 4: FAIL     → Route to DLQ                            │               │
│   └─────────────────────────────────────────────────────────────────┘               │
│        │                                                                             │
│        ▼                                                                             │
│   ┌─────────────┐     ┌─────────────┐     ┌─────────────┐                           │
│   │  SQS DLQ    │────▶│  CloudWatch │────▶│    Alert    │                           │
│   │  (persist)  │     │  (log)      │     │  (SNS/Slack)│                           │
│   └─────────────┘     └─────────────┘     └─────────────┘                           │
│                                                                                      │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

### Error Categories

| Error Type | Handling | Recovery |
|------------|----------|----------|
| Transient (network, timeout) | Automatic retry | Exponential backoff |
| Data validation | Route to failed/ | Manual review |
| Schema mismatch | Alert + DLQ | Schema evolution |
| API rate limit | Retry with backoff | Adjust schedule |
| Infrastructure | Alert + manual | Terraform redeploy |

---

## Observability

### Monitoring Stack

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                              OBSERVABILITY                                           │
├─────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                      │
│   ┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐              │
│   │   CloudWatch    │     │   Snowflake     │     │     Soda        │              │
│   │   ───────────   │     │   ───────────   │     │   ───────────   │              │
│   │   • Lambda logs │     │   • Query hist  │     │   • Check results│             │
│   │   • Metrics     │     │   • Pipe status │     │   • Anomalies   │              │
│   │   • Alarms      │     │   • Copy hist   │     │   • Trends      │              │
│   │   • Dashboards  │     │   • Costs       │     │   • Alerts      │              │
│   └─────────────────┘     └─────────────────┘     └─────────────────┘              │
│                                                                                      │
│   Key Metrics:                                                                       │
│   • Pipeline execution time                                                          │
│   • Records processed per run                                                        │
│   • Error rate and types                                                             │
│   • Snowpipe latency                                                                 │
│   • Data quality scores                                                              │
│                                                                                      │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

### Alerting Rules

| Metric | Threshold | Action |
|--------|-----------|--------|
| Pipeline failure | Any | SNS → Email/Slack |
| Execution time | > 10 min | Warning alert |
| Error rate | > 5% | Critical alert |
| DLQ depth | > 0 | Investigation alert |
| Soda check failure | Any critical | Block + alert |

---

## Project Structure

```
global_footprint_network_use_case/
├── src/
│   └── gfn_pipeline/
│       ├── __init__.py
│       ├── extract.py          # API extraction logic
│       ├── transform.py        # Data transformation
│       ├── load.py             # dlt loading (local)
│       ├── validators.py       # Staging validation
│       └── utils.py            # Shared utilities
├── terraform/
│   ├── main.tf                 # Provider config
│   ├── s3.tf                   # S3 resources
│   ├── lambda.tf               # Lambda functions
│   ├── step_functions.tf       # State machine
│   ├── snowflake.tf            # Snowflake resources
│   └── ...
├── soda/
│   ├── checks.yml              # Post-load quality checks
│   └── staging_checks.yml      # Pre-load validation
├── tests/
│   ├── test_pipeline.py        # Integration tests
│   ├── test_extract.py         # Extraction tests
│   └── test_transform.py       # Transform tests
├── docs/
│   ├── ARCHITECTURE.md         # This document
│   └── SNOWPIPE_SETUP.md       # Snowpipe configuration
├── data/
│   └── gfn.duckdb              # Local DuckDB database
├── Makefile                    # Development commands
├── pyproject.toml              # Python dependencies
└── README.md                   # Project overview
```

---

## Quick Reference

### Commands

```bash
# Local Development
make extract          # Run extraction with dlt → DuckDB
make test             # Run all tests with LocalStack
make soda             # Run Soda quality checks

# Production Deployment
terraform init        # Initialize Terraform
terraform plan        # Preview changes
terraform apply       # Deploy infrastructure

# Monitoring
make logs             # View CloudWatch logs
make status           # Check pipeline status
```

### Environment Variables

| Variable | Purpose | Required |
|----------|---------|----------|
| `AWS_REGION` | AWS region | Yes |
| `SNOWFLAKE_ACCOUNT` | Snowflake account | Production |
| `SNOWFLAKE_USER` | Snowflake username | Production |
| `SNOWFLAKE_PASSWORD` | Snowflake password | Production |
| `GFN_API_KEY` | GFN API key (if required) | Optional |

---

## References

- [dlt Documentation](https://dlthub.com/docs)
- [Snowpipe Documentation](https://docs.snowflake.com/en/user-guide/data-load-snowpipe)
- [Terraform AWS Provider](https://registry.terraform.io/providers/hashicorp/aws)
- [Terraform Snowflake Provider](https://registry.terraform.io/providers/Snowflake-Labs/snowflake)
- [Soda Core Documentation](https://docs.soda.io/soda-core)
- [AWS Step Functions](https://docs.aws.amazon.com/step-functions)
