# GFN Carbon Footprint Ingestion Pipeline

A production-ready data pipeline for extracting carbon footprint data from the Global Footprint Network (GFN) API and loading it into Snowflake.

---

## Features

- **dlt + S3 Data Lake Architecture**: Schema evolution, incremental loads, audit trail
- **AWS Orchestration**: EventBridge, Step Functions, Lambda for production deployment
- **Async extraction** with rate limiting and retry logic
- **Soda data quality checks** on staging layer
- **Data contracts** for schema validation
- **Idempotent processing** with deduplication by unique key
- **Historical backfill** support (1961-2024)
- **Local development** with LocalStack + DuckDB
- **Optional Snowpipe**: Alternative for near real-time streaming (see [SNOWPIPE_SETUP.md](docs/SNOWPIPE_SETUP.md))

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

The pipeline uses a **dlt + S3 Data Lake** architecture:

```
Extract (async) → S3 Raw → Soda Checks → S3 Staged → dlt → Destination
```

**Key Features:**
- **Schema Evolution**: New API fields automatically become columns
- **Incremental Loads**: dlt tracks state, only processes new/changed data
- **Data Contracts**: Enforce required fields with `--with-contracts`
- **Soda Checks**: Data quality validation on staging layer
- **Audit Trail**: Raw JSON preserved in S3 for replay/debugging

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

| Approach | Use Case | Documentation |
|----------|----------|---------------|
| **dlt (Recommended)** | Batch loads, schema evolution, merge/upsert | This README |
| **Snowpipe (Alternative)** | Near real-time streaming, native AWS integration | [SNOWPIPE_SETUP.md](docs/SNOWPIPE_SETUP.md) |

**Why dlt is recommended:**
- Automatic schema evolution (new API fields become columns)
- Built-in merge/upsert with primary keys
- State tracking for incremental loads
- Works for both DuckDB (local) and Snowflake (production)
- Simpler architecture - fewer AWS resources needed

**When to use Snowpipe:**
- Near real-time streaming requirements (<1 min latency)
- Native AWS/Snowflake integration preferred
- No compute costs for loading (Snowflake handles it)

### Schema Overview (dlt)

| Schema | Purpose |
|--------|---------|
| `GFN_DATA` | Main data loaded by dlt |
| `_DLT_*` | dlt internal tables (state, loads, versions) |

### Data Flow (dlt - Recommended)

```
S3 (staged/) → Lambda (dlt) → Snowflake GFN_DATA.FOOTPRINT_DATA
                    │
                    └── Schema evolution, merge/upsert, state tracking
```

### Data Flow (Snowpipe - Alternative)

```
S3 (transformed/) → Snowpipe → RAW.FOOTPRINT_DATA_RAW
                                        │
                                        │ Stream + Task
                                        ▼
                              TRANSFORMED.FOOTPRINT_DATA
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

For detailed architecture documentation, see [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

### High-Level Overview (Recommended: AWS + dlt)

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   TRIGGER   │────▶│   EXTRACT   │────▶│   STAGE     │────▶│    LOAD     │
│             │     │             │     │             │     │             │
│ EventBridge │     │   Lambda    │     │   Lambda    │     │   Lambda    │
│ Step Funcs  │     │   GFN API   │     │   Soda QA   │     │    dlt      │
└─────────────┘     └──────┬──────┘     └──────┬──────┘     └──────┬──────┘
                           │                   │                   │
                           ▼                   ▼                   ▼
                    ┌─────────────────────────────────────────────────────┐
                    │                    S3 DATA LAKE                      │
                    │         raw/ ────────▶ staged/ ──────────▶ Snowflake │
                    └─────────────────────────────────────────────────────┘
```

### Alternative: Snowpipe for Streaming

For near real-time streaming scenarios, see [docs/SNOWPIPE_SETUP.md](docs/SNOWPIPE_SETUP.md).

```
S3 (transformed/) → SNS → SQS → Snowpipe → Snowflake RAW schema
```

---

## License

MIT License
