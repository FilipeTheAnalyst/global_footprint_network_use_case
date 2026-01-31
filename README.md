# Global Footprint Network Data Pipeline

A production-ready ETL pipeline for extracting, transforming, and loading Global Footprint Network ecological data. Supports two deployment approaches:

1. **dlt + DuckDB** - Lightweight, local-first approach using dlt framework
2. **AWS LocalStack** - Production-like AWS infrastructure simulation with Lambda, Step Functions, and S3

## Table of Contents

- [Architecture](#architecture)
- [Quick Start](#quick-start)
- [Approach 1: dlt + DuckDB Pipeline](#approach-1-dlt--duckdb-pipeline)
- [Approach 2: AWS LocalStack Pipeline](#approach-2-aws-localstack-pipeline)
- [API Efficiency](#api-efficiency)
- [Configuration](#configuration)
- [Testing](#testing)
- [Production Deployment](#production-deployment)
- [Troubleshooting](#troubleshooting)

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                              TWO PIPELINE APPROACHES                             │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  ┌─────────────────────────────┐    ┌─────────────────────────────────────────┐ │
│  │   APPROACH 1: dlt + DuckDB  │    │   APPROACH 2: AWS LocalStack            │ │
│  │                             │    │                                          │ │
│  │   ┌─────────┐               │    │   ┌──────────────────────────────────┐  │ │
│  │   │ GFN API │               │    │   │     Step Functions Orchestrator  │  │ │
│  │   └────┬────┘               │    │   └───────────────┬──────────────────┘  │ │
│  │        ▼                    │    │                   ▼                      │ │
│  │   ┌─────────┐               │    │   ┌──────────┐   ┌───────────┐   ┌────┐ │ │
│  │   │   dlt   │               │    │   │ Lambda   │──▶│  Lambda   │──▶│Load│ │ │
│  │   │ Source  │               │    │   │ Extract  │   │ Transform │   │    │ │ │
│  │   └────┬────┘               │    │   └────┬─────┘   └─────┬─────┘   └──┬─┘ │ │
│  │        ▼                    │    │        ▼               ▼            ▼   │ │
│  │   ┌─────────┐               │    │   ┌────────────────────────────────────┐│ │
│  │   │ DuckDB  │               │    │   │              S3 Bucket             ││ │
│  │   │ or      │               │    │   │  raw/ → processed/ → Snowflake    ││ │
│  │   │Snowflake│               │    │   └────────────────────────────────────┘│ │
│  │   └─────────┘               │    │                                          │ │
│  │                             │    │   EventBridge (daily cron)               │ │
│  │   Best for:                 │    │   SQS (queue triggers)                   │ │
│  │   - Local development       │    │   SNS (notifications)                    │ │
│  │   - Quick prototyping       │    │                                          │ │
│  │   - Small datasets          │    │   Best for:                              │ │
│  │                             │    │   - Production simulation                │ │
│  └─────────────────────────────┘    │   - AWS deployment testing               │ │
│                                      │   - Full infrastructure testing          │ │
│                                      └─────────────────────────────────────────┘ │
├─────────────────────────────────────────────────────────────────────────────────┤
│                              SHARED COMPONENTS                                   │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐    ┌──────────────┐   │
│  │   Dynamic    │    │   Bulk API   │    │    Soda      │    │   GitHub     │   │
│  │   Type       │    │   Endpoint   │    │   Quality    │    │   Actions    │   │
│  │   Discovery  │    │   /data/all  │    │   Checks     │    │   CI/CD      │   │
│  └──────────────┘    └──────────────┘    └──────────────┘    └──────────────┘   │
└─────────────────────────────────────────────────────────────────────────────────┘
```

## Quick Start

### Prerequisites

- Python 3.11+
- Docker & Docker Compose
- [uv](https://github.com/astral-sh/uv) (Python package manager)
- GFN API Key (get from [data.footprintnetwork.org](https://data.footprintnetwork.org))

### 1. Clone and Install

```bash
git clone <repository-url>
cd global_footprint_network_use_case

# Install dependencies with uv
make install
# or manually:
uv sync
```

### 2. Configure Environment

```bash
# Copy example environment file
cp .env.example .env

# Edit .env and add your API key
# Required:
GFN_API_KEY=your_api_key_here

# Optional (for Snowflake):
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_USER=your_user
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_DATABASE=GFN
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
```

### 3. Choose Your Approach

**Option A: Quick Local Run (dlt + DuckDB)**
```bash
# Extract recent years to local DuckDB
uv run python -m gfn_pipeline.pipeline_async --start-year 2020 --end-year 2024
```

**Option B: Full AWS Simulation (LocalStack)**
```bash
# Start LocalStack and setup infrastructure
make setup

# Run the pipeline
make run-lambda
```

---

## Approach 1: dlt + DuckDB Pipeline

The lightweight approach using [dlt](https://dlthub.com/) for data loading and DuckDB for local storage.

### Features

- **Dynamic Type Discovery**: Record types are discovered from the API, not hardcoded
- **Bulk API Endpoint**: Uses efficient `/data/all/{year}` endpoint (~66 API calls for 64 years)
- **Parallel Fetching**: Fetches multiple years concurrently
- **Incremental Merge**: Supports both full refresh and incremental updates

### Running the Pipeline

```bash
# Basic run (2010-2024, DuckDB destination)
uv run python -m gfn_pipeline.pipeline_async

# Full historical extraction (1961-2024)
uv run python -m gfn_pipeline.pipeline_async --start-year 1961 --end-year 2024

# Specific year range
uv run python -m gfn_pipeline.pipeline_async --start-year 2015 --end-year 2020

# Full refresh (replace all data)
uv run python -m gfn_pipeline.pipeline_async --full-refresh

# Load to Snowflake
uv run python -m gfn_pipeline.pipeline_async --destination snowflake

# List available record types from API
uv run python -m gfn_pipeline.pipeline_async --list-types

# Extract specific record types only
uv run python -m gfn_pipeline.pipeline_async --record-types EFConsTotGHA BiocapTotGHA
```

### Output Schema

The pipeline creates three tables in DuckDB:

| Table | Description | Primary Key |
|-------|-------------|-------------|
| `gfn.countries` | Country reference data | `country_code` |
| `gfn.record_types` | Dynamically discovered record types | `record_type` |
| `gfn.footprint_data` | All footprint/biocapacity data | `(country_code, year, record_type)` |

### Querying Results

```bash
# Open DuckDB CLI
uv run duckdb gfn_footprint.duckdb

# Example queries
SELECT COUNT(*) FROM gfn.footprint_data;
SELECT DISTINCT record_type FROM gfn.footprint_data;
SELECT * FROM gfn.countries LIMIT 10;
```

---

## Approach 2: AWS LocalStack Pipeline

Production-like AWS infrastructure simulation using LocalStack.

### Features

- **Step Functions Orchestration**: State machine manages ETL workflow
- **Lambda Functions**: Separate Extract, Transform, Load functions
- **S3 Data Lake**: Raw and processed data stored in S3
- **SQS Queues**: Decoupled message-based triggers
- **EventBridge**: Scheduled daily extraction
- **SNS Notifications**: Pipeline success/failure alerts

### Docker Setup

#### Step 1: Start LocalStack

```bash
# Start LocalStack container
docker-compose up -d localstack

# Verify LocalStack is running
curl http://localhost:4566/_localstack/health
```

#### Step 2: Setup AWS Infrastructure

```bash
# Create all AWS resources (S3, SQS, Lambda, Step Functions, etc.)
uv run python -m infrastructure.setup_localstack

# Or use make
make setup
```

This creates:
- S3 bucket: `gfn-data-lake` with `raw/` and `processed/` prefixes
- SQS queues: `gfn-extract-queue`, `gfn-transform-queue`, `gfn-load-queue`, `gfn-dlq`
- Lambda functions: `gfn-extract`, `gfn-transform`, `gfn-load`
- Step Functions: `gfn-pipeline-orchestrator`
- EventBridge rule: Daily extraction at 6 AM UTC
- SNS topic: `gfn-pipeline-notifications`

#### Step 3: Run the Pipeline

**Option A: Invoke Lambda Directly**
```bash
# Extract data
uv run awslocal lambda invoke --function-name gfn-extract \
  --payload '{"start_year": 2020, "end_year": 2024}' output.json

# Check result
cat output.json
```

**Option B: Start Step Functions Execution**
```bash
# Start state machine execution
uv run awslocal stepfunctions start-execution \
  --state-machine-arn arn:aws:states:us-east-1:000000000000:stateMachine:gfn-pipeline-orchestrator \
  --input '{"start_year": 2020, "end_year": 2024}'
```

**Option C: Use CLI Handlers**
```bash
# Run extract locally (saves to LocalStack S3)
uv run python -m infrastructure.lambda_handlers extract --start-year 2020 --end-year 2024

# Run transform on extracted data
uv run python -m infrastructure.lambda_handlers transform --s3-key raw/footprint/2024/01/30/data.json

# Run load
uv run python -m infrastructure.lambda_handlers load --s3-key processed/footprint/2024/01/30/data_processed.json
```

#### Step 4: Verify Results

```bash
# List S3 contents
uv run awslocal s3 ls s3://gfn-data-lake/ --recursive

# Check SQS queues
uv run awslocal sqs list-queues

# View Lambda logs
uv run awslocal logs tail /aws/lambda/gfn-extract --follow
```

### Full Docker Workflow

```bash
# Build and run everything in Docker
make docker-build
make docker-up
make docker-setup
make docker-pipeline

# Or one command
make docker-all
```

---

## API Efficiency

Both approaches use the efficient bulk API endpoint:

| Approach | API Calls (64 years) | Time |
|----------|---------------------|------|
| Per country/type | ~3,000+ | Hours |
| **Bulk /data/all/{year}** | **~66** | **~2-3 min** |

### Dynamic Type Discovery

Record types are **discovered dynamically** from the API:

```python
# Types are fetched from:
# 1. /types endpoint (metadata)
# 2. /data/all/{year} sample (actual types in data)

# No hardcoded fallback values - if API fails, pipeline fails clearly
```

---

## Configuration

### Environment Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `GFN_API_KEY` | Yes | - | Global Footprint Network API key |
| `DUCKDB_PATH` | No | `gfn_footprint.duckdb` | DuckDB database path |
| `LOCALSTACK_ENDPOINT` | No | `http://localhost:4566` | LocalStack endpoint |
| `AWS_REGION` | No | `us-east-1` | AWS region |
| `S3_BUCKET` | No | `gfn-data-lake` | S3 bucket name |
| `SNOWFLAKE_ACCOUNT` | No | - | Snowflake account identifier |
| `SNOWFLAKE_USER` | No | - | Snowflake username |
| `SNOWFLAKE_PASSWORD` | No | - | Snowflake password |
| `SNOWFLAKE_DATABASE` | No | `GFN` | Snowflake database |
| `SNOWFLAKE_WAREHOUSE` | No | `COMPUTE_WH` | Snowflake warehouse |
| `SNOWFLAKE_SCHEMA` | No | `RAW` | Snowflake schema |

### dlt Configuration

dlt configuration is in `.dlt/config.toml` and `.dlt/secrets.toml`:

```toml
# .dlt/secrets.toml
[sources.gfn]
api_key = "your_api_key"

[destination.snowflake.credentials]
database = "GFN"
password = "your_password"
username = "your_user"
host = "your_account.snowflakecomputing.com"
warehouse = "COMPUTE_WH"
role = "ACCOUNTADMIN"
```

---

## Testing

### Run All Tests

```bash
# Unit tests only
uv run pytest tests/ -v -m "not integration"

# Integration tests (requires LocalStack)
make setup  # Start LocalStack first
uv run pytest tests/ -v -m integration

# All tests
uv run pytest tests/ -v

# With coverage
uv run pytest tests/ -v --cov=src --cov=infrastructure
```

### Test Categories

| Marker | Description | Requirements |
|--------|-------------|--------------|
| (none) | Unit tests | None |
| `@pytest.mark.integration` | LocalStack tests | LocalStack running |
| `@pytest.mark.asyncio` | Async tests | pytest-asyncio |

---

## Production Deployment

### GitHub Actions Workflows

| Workflow | Trigger | Description |
|----------|---------|-------------|
| `ci.yml` | PR/push | Linting, type checking, unit tests |
| `daily-pipeline.yml` | Daily 6 AM UTC | Full pipeline run |
| `data-quality.yml` | After pipeline | Soda data quality checks |

### Required GitHub Secrets

| Secret | Description |
|--------|-------------|
| `GFN_API_KEY` | GFN API key |
| `SNOWFLAKE_ACCOUNT` | Snowflake account |
| `SNOWFLAKE_USER` | Snowflake username |
| `SNOWFLAKE_PASSWORD` | Snowflake password |
| `SLACK_WEBHOOK_URL` | *(Optional)* Slack alerts |

### Snowflake Setup

```bash
# Setup Snowflake schema and tables
uv run python -m infrastructure.setup_snowflake_production
```

---

## Troubleshooting

### Common Issues

**1. API Authentication Error (403)**
```python
# CORRECT: Empty username, API key as password
auth = aiohttp.BasicAuth("", api_key)

# WRONG
auth = aiohttp.BasicAuth("user", api_key)
```

**2. LocalStack Not Running**
```bash
# Check health
curl http://localhost:4566/_localstack/health

# Restart
docker-compose down
docker-compose up -d localstack
```

**3. DuckDB Version Conflict**
```bash
# dlt requires duckdb>=1.1.0,<1.2.0
uv pip install "duckdb>=1.1.0,<1.2.0"
```

**4. No Record Types Discovered**
```bash
# Test API connectivity
curl -u ":YOUR_API_KEY" https://api.footprintnetwork.org/v1/types

# List available types
uv run python -m gfn_pipeline.pipeline_async --list-types
```

**5. Lambda Timeout**
```bash
# Increase timeout in setup_localstack.py
# Default is 300s (5 min) for extract
```

### Debug Commands

```bash
# Check dlt pipeline state
uv run dlt pipeline gfn_footprint info

# View dlt logs
cat ~/.dlt/pipelines/gfn_footprint/logs/*

# LocalStack logs
docker-compose logs -f localstack

# Test API endpoint
curl -u ":$GFN_API_KEY" "https://api.footprintnetwork.org/v1/data/all/2020" | head
```

---

## Project Structure

```
├── .github/workflows/        # GitHub Actions CI/CD
│   ├── ci.yml               # PR/push checks
│   ├── daily-pipeline.yml   # Scheduled pipeline
│   └── data-quality.yml     # Soda checks
├── src/gfn_pipeline/        # Core pipeline code
│   ├── main.py              # Legacy PipelineRunner
│   └── pipeline_async.py    # dlt-based async pipeline (MAIN)
├── infrastructure/          # AWS infrastructure
│   ├── lambda_handlers.py   # Lambda functions for ETL
│   ├── setup_localstack.py  # LocalStack setup script
│   ├── load_to_snowflake.py # Snowflake loader utility
│   └── snowflake/           # Snowflake SQL scripts
├── api/                     # FastAPI endpoints
├── soda/                    # Data quality checks
├── tests/                   # Unit and integration tests
├── docker-compose.yml       # LocalStack + services
├── Makefile                 # Build and run commands
└── pyproject.toml           # Python dependencies
```

---

## License

MIT
