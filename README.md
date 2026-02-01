# GFN Carbon Footprint Ingestion Pipeline

A production-ready data pipeline for extracting carbon footprint data from the Global Footprint Network (GFN) API and loading it into Snowflake.

---

## Features

- **Dual Architecture Design**:
  - **Production**: Snowpipe + AWS Step Functions for enterprise-scale, event-driven loading
  - **Local Development**: dlt + DuckDB for rapid iteration and testing
- **Infrastructure as Code**: Terraform (recommended) with CloudFormation alternative
- **AWS Orchestration**: EventBridge, Step Functions, Lambda, SQS, SNS
- **Async extraction** with rate limiting and retry logic
- **Two-tier data validation**: Soda staging checks (pre-load) + quality checks (post-load)
- **Data contracts** for schema validation
- **Idempotent processing** with deduplication by unique key
- **Historical backfill** support (1961-2024)
- **Local development** with LocalStack + DuckDB

> See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for detailed architecture documentation including Terraform vs CloudFormation comparison.

---

## Quick Start

### Prerequisites

- Python 3.11+
- [uv](https://docs.astral.sh/uv/) package manager
- Docker (for LocalStack)
- GFN API key from [footprintnetwork.org](https://www.footprintnetwork.org/)

### Installation

```bash
git clone <repository-url>
cd global_footprint_network_use_case

# Install dependencies
make install
# or directly: uv sync
```

### Configuration

Create a `.env` file:

```bash
# GFN API (required)
GFN_API_KEY=your_api_key_here

# AWS (for LocalStack or production)
AWS_ACCESS_KEY_ID=test
AWS_SECRET_ACCESS_KEY=test
AWS_DEFAULT_REGION=us-east-1
S3_BUCKET=gfn-data-lake

# Snowflake (for production)
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_USER=your_user
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_DATABASE=GFN_PIPELINE
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
```

---

## Usage

### Pipeline Architecture

The pipeline supports **dual architectures** optimized for different environments:

**Production (Snowpipe + AWS)**:
```
EventBridge → Step Functions → Lambda Extract → S3 Raw
                            → Lambda Transform → S3 Staged
                            → SNS → Snowpipe → Snowflake
```

**Local Development (dlt + DuckDB)**:
```
Extract (async) → S3 Raw → Soda Checks → S3 Staged → dlt → DuckDB
```

**Key Features:**
- **Schema Evolution**: New API fields automatically become columns
- **Incremental Loads**: dlt tracks state, only processes new/changed data
- **Data Contracts**: Enforce required fields with `--with-contracts`
- **Two-tier Validation**: Soda staging checks + quality checks
- **Audit Trail**: Raw JSON preserved in S3 for replay/debugging

> See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for detailed architecture comparison.

### Basic Commands

```bash
# Start LocalStack + setup AWS resources
make setup

# Run pipeline (DuckDB destination with S3 storage)
make run

# Run pipeline to Snowflake
make run-snowflake

# Run pipeline to both DuckDB and Snowflake
make run-both

# Run with Soda data quality checks
make run-soda

# Run with data contracts
make run-contracts

# Production run (Snowflake + Soda + contracts)
make run-production

# Full refresh (replace all data)
make run-full-refresh
```

### Direct Extraction (No S3)

For quick local development without S3 dependencies:

```bash
# Run dlt pipeline directly to DuckDB
make run-direct

# Run dlt pipeline to Snowflake
make run-direct-snowflake
```

### Backfill Operations

Historical data extraction for any year range (1961-2024):

```bash
# Recent years (2020-2024)
make backfill-recent

# Full history (1961-2024)
make backfill-full

# Custom range
make backfill YEARS="2010-2020"
```

**Performance**:
- ~66 API calls for full history (bulk endpoint)
- ~2-3 minutes for complete extraction
- Idempotent: safe to re-run

### Query Results

```bash
# Query DuckDB results
make duckdb-query
make duckdb-summary
make duckdb-top-emitters
```

### AWS LocalStack (Production Simulation)

Full AWS stack simulation with Lambda, S3, SQS, and Step Functions:

```bash
# Start LocalStack + setup AWS resources
make setup

# Run full Lambda pipeline (Extract → Transform → Load)
make lambda-invoke-pipeline

# Or run individual Lambda steps
make lambda-invoke-extract
make lambda-invoke-transform S3_KEY=raw/gfn_footprint_...json
make lambda-invoke-load S3_KEY=transformed/gfn_footprint_...json

# Check S3 files
make aws-s3-ls
make aws-s3-raw
make aws-s3-staged
```

**S3 Structure**:
```
s3://gfn-data-lake/
├── raw/                    # Raw API responses (immutable audit trail)
│   └── gfn_footprint_{timestamp}.json
├── staged/                 # Validated data ready for dlt
│   └── gfn_footprint_{timestamp}_staged.json
└── transformed/            # Legacy: for Snowpipe
    └── gfn_footprint_{timestamp}_transformed.json
```

---

## Testing

```bash
# Run all tests (~7s)
make test

# Unit tests only (no LocalStack required)
make test-unit

# Integration tests only (requires LocalStack)
make test-integration
```

**Test Categories:**
- **Unit tests**: Core logic with mocked dependencies (run by default)
- **Integration tests**: Require LocalStack running (marked with `@pytest.mark.integration`)

```bash
# Run only unit tests (fast)
uv run pytest tests/ -v -m "not integration"

# Run integration tests (requires LocalStack)
make setup
uv run pytest tests/ -v -m integration
```

### Data Quality Checks (Soda)

The project uses a **two-tier validation** approach with Soda:

1. **Staging Validation** (pre-load): Python-based checks defined in `soda/staging_checks.yml`
2. **Post-load Validation**: Soda Core checks against Snowflake defined in `soda/checks.yml`

```bash
# Run Soda checks against Snowflake
make soda-check
```

**Checks include:**
- Row count validation (expect data for ~184 countries × 28 record types × ~60 years)
- Data freshness (records loaded within 24 hours)
- Required columns not null (country_code, country_name, year, record_type)
- Value range validation (year 1960-2030, value ≥ 0)
- Record type validation (28 valid types)
- Duplicate detection (unique country-year-type combinations)
- Schema validation (required columns present)

---

## Project Structure

```
global_footprint_network_use_case/
├── src/gfn_pipeline/           # Core pipeline code
│   ├── main.py                 # CLI entry point (dlt + S3 + Soda)
│   ├── validators.py           # Soda staging validators (loads YAML)
│   └── pipeline_async.py       # Async extraction with dlt (direct mode)
├── infrastructure/             # AWS infrastructure
│   ├── lambda_handlers.py      # Lambda functions
│   ├── setup_localstack.py     # LocalStack setup
│   ├── terraform/              # Terraform IaC (recommended)
│   └── snowflake/              # Snowflake SQL scripts
├── tests/                      # Test suite
│   └── test_pipeline.py        # Unit + integration tests
├── soda/                       # Data quality checks
│   ├── configuration.yml       # Soda Snowflake connection config
│   ├── staging_checks.yml      # PRE-LOAD: Staging layer validation (YAML)
│   └── checks.yml              # POST-LOAD: Snowflake table validation
├── data/                       # Local data storage
│   ├── raw/                    # Raw extracts
│   └── transformed/            # Processed data
├── docs/                       # Documentation
│   ├── ARCHITECTURE.md         # System architecture
│   └── SNOWPIPE_SETUP.md       # Snowpipe setup guide
├── docker-compose.yml          # LocalStack services
├── Makefile                    # Build commands
└── pyproject.toml              # Dependencies
```

---

## Snowflake Integration

### Loading Approaches

| Approach | Environment | Use Case |
|----------|-------------|----------|
| **Snowpipe (Production)** | AWS Production | Enterprise-scale, event-driven, near real-time |
| **dlt (Local Development)** | LocalStack + DuckDB | Rapid iteration, testing, schema evolution |

**Production Architecture (Snowpipe)**:
- Event-driven loading via SNS notifications
- Native AWS/Snowflake integration
- No compute costs for loading (Snowflake handles it)
- Enterprise-scale throughput

**Local Development (dlt)**:
- Automatic schema evolution (new API fields become columns)
- Built-in merge/upsert with primary keys
- State tracking for incremental loads
- Multi-destination support (DuckDB for local, Snowflake for staging)
- Data contracts for schema validation

> See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for detailed architecture comparison and rationale.

### Schema Overview

| Schema | Purpose |
|--------|---------|
| `GFN_DATA` | Main data loaded by dlt (local) or Snowpipe (production) |
| `_DLT_*` | dlt internal tables (state, loads, versions) - local dev only |

### Data Flow (Production - Snowpipe)

```
S3 (transformed/) → SNS → Snowpipe → RAW.FOOTPRINT_DATA_RAW
                                            │
                                            │ Stream + Task
                                            ▼
                                  TRANSFORMED.FOOTPRINT_DATA
```

### Data Flow (Local Development - dlt)

```
S3 (staged/) → Lambda (dlt) → DuckDB/Snowflake GFN_DATA.FOOTPRINT_DATA
                    │
                    └── Schema evolution, merge/upsert, state tracking
```

See [docs/SNOWPIPE_SETUP.md](docs/SNOWPIPE_SETUP.md) for detailed Snowpipe setup instructions.

---

## API Reference

### GFN API Authentication

```python
# Empty username, API key as password
import aiohttp
auth = aiohttp.BasicAuth("", api_key)
```

### Bulk Endpoint (Recommended)

```
GET https://api.footprintnetwork.org/v1/data/all/{year}
```

Returns all countries and record types for a given year (~200 records per call).

### Response Format

```json
{
  "countryCode": 238,
  "countryName": "Ethiopia",
  "isoa2": "ET",
  "year": 2010,
  "value": 5760020.03,
  "record": "CarbonBC"
}
```

---

## Makefile Commands

| Command | Description |
|---------|-------------|
| `make install` | Install dependencies |
| `make setup` | Start LocalStack and create resources |
| `make run` | Run pipeline (DuckDB + S3 + dlt) |
| `make run-snowflake` | Run pipeline (Snowflake destination) |
| `make run-both` | Run pipeline (DuckDB + Snowflake) |
| `make run-soda` | Run pipeline with Soda checks |
| `make run-contracts` | Run pipeline with data contracts |
| `make run-production` | Production: Snowflake + Soda + contracts |
| `make run-direct` | Run dlt directly (no S3) |
| `make backfill-recent` | Backfill 2020-2024 |
| `make backfill-full` | Backfill 1961-2024 |
| `make backfill YEARS=...` | Custom year range backfill |
| `make lambda-invoke-pipeline` | Run full Lambda ETL pipeline |
| `make lambda-invoke-extract` | Run extract Lambda only |
| `make test` | Run all tests |
| `make test-coverage` | Run tests with coverage |
| `make soda-check` | Run data quality checks |
| `make duckdb-query` | Query DuckDB results |
| `make duckdb-summary` | Show DuckDB summary stats |
| `make aws-s3-ls` | List S3 bucket contents |
| `make clean` | Clean generated files |
| `make docker-down` | Stop LocalStack |

---

## Troubleshooting

### API Authentication Errors

```
401 Unauthorized
```

**Solution**: Verify API key format. The GFN API uses Basic Auth with empty username:
```bash
curl -u ":YOUR_API_KEY" https://api.footprintnetwork.org/v1/data/all/2023
```

### LocalStack Connection Issues

```
Could not connect to LocalStack
```

**Solution**:
```bash
# Check LocalStack status
docker-compose ps

# Restart LocalStack
make docker-down && make setup
```

### Rate Limiting

```
429 Too Many Requests
```

**Solution**: The pipeline has built-in rate limiting (8 concurrent requests). If issues persist, reduce concurrency in `pipeline_async.py`.

### Snowpipe Not Loading

**Solution**: Check Snowpipe status:
```sql
SELECT SYSTEM$PIPE_STATUS('RAW.FOOTPRINT_DATA_PIPE');
```

See [docs/SNOWPIPE_SETUP.md](docs/SNOWPIPE_SETUP.md) for troubleshooting.

---

## Architecture

For detailed architecture documentation including Terraform vs CloudFormation comparison, see [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

### Production Architecture (Snowpipe + AWS)

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   TRIGGER   │────▶│   EXTRACT   │────▶│  TRANSFORM  │────▶│    LOAD     │
│             │     │             │     │             │     │             │
│ EventBridge │     │   Lambda    │     │   Lambda    │     │  Snowpipe   │
│ Step Funcs  │     │   GFN API   │     │   Soda QA   │     │  (via SNS)  │
└─────────────┘     └──────┬──────┘     └──────┬──────┘     └──────┬──────┘
                           │                   │                   │
                           ▼                   ▼                   ▼
                    ┌─────────────────────────────────────────────────────┐
                    │                    S3 DATA LAKE                      │
                    │         raw/ ────────▶ staged/ ──────────▶ Snowflake │
                    └─────────────────────────────────────────────────────┘
```

### Local Development Architecture (dlt + DuckDB)

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   TRIGGER   │────▶│   EXTRACT   │────▶│   STAGE     │────▶│    LOAD     │
│             │     │             │     │             │     │             │
│   Manual    │     │   Async     │     │   Soda QA   │     │    dlt      │
│  or Script  │     │   GFN API   │     │   Checks    │     │   DuckDB    │
└─────────────┘     └──────┬──────┘     └──────┬──────┘     └──────┬──────┘
                           │                   │                   │
                           ▼                   ▼                   ▼
                    ┌─────────────────────────────────────────────────────┐
                    │               LOCALSTACK S3 + DUCKDB                 │
                    │         raw/ ────────▶ staged/ ──────────▶ DuckDB   │
                    └─────────────────────────────────────────────────────┘
```

**Why dlt for local development:**
- Schema evolution and data contracts
- Multi-destination support (DuckDB for local, Snowflake for staging)
- Rapid iteration without cloud dependencies

**Why Snowpipe for production:**
- Event-driven, near real-time loading
- Native AWS/Snowflake integration
- Enterprise-scale throughput

---

## License

MIT License
