# Global Footprint Network Data Pipeline

A production-ready ETL pipeline for extracting, transforming, and loading Global Footprint Network ecological data into Snowflake.

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                              ORCHESTRATION LAYER                                 │
├─────────────────────────────────────────────────────────────────────────────────┤
│  GitHub Actions                                                                  │
│  ├── ci.yml           → PR/push: lint, test, type-check                        │
│  ├── daily-pipeline.yml → Cron: 0 6 * * * (daily at 6 AM UTC)                  │
│  └── data-quality.yml  → Soda checks after pipeline runs                        │
│                                                                                  │
│  LocalStack (Local Dev)                                                          │
│  └── EventBridge, SQS, Lambda simulation                                        │
├─────────────────────────────────────────────────────────────────────────────────┤
│                              PROCESSING LAYER                                    │
│  ┌─────────────────────────────────────────────────────────────────────────┐    │
│  │                         Python Pipeline                                  │    │
│  │  ┌──────────┐    ┌─────────────┐    ┌──────────┐    ┌───────────────┐  │    │
│  │  │ Extract  │───▶│  Transform  │───▶│   Load   │───▶│   Validate    │  │    │
│  │  │ (GFN API)│    │  (pandas)   │    │(Snowflake│    │   (Soda)      │  │    │
│  │  └──────────┘    └─────────────┘    └──────────┘    └───────────────┘  │    │
│  └─────────────────────────────────────────────────────────────────────────┘    │
├─────────────────────────────────────────────────────────────────────────────────┤
│                              STORAGE LAYER                                       │
│  ┌─────────────────────┐    ┌─────────────────────┐    ┌──────────────────┐     │
│  │   S3 (LocalStack)   │    │     DuckDB          │    │    Snowflake     │     │
│  │   gfn-data-lake/    │    │   (local dev)       │    │ GFN.RAW tables   │     │
│  └─────────────────────┘    └─────────────────────┘    └──────────────────┘     │
├─────────────────────────────────────────────────────────────────────────────────┤
│                            MONITORING LAYER                                      │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐    ┌──────────────┐   │
│  │   GitHub     │    │    Soda      │    │   Slack      │    │  CloudWatch  │   │
│  │   Actions    │    │   Checks     │    │   Alerts     │    │  (LocalStack)│   │
│  └──────────────┘    └──────────────┘    └──────────────┘    └──────────────┘   │
└─────────────────────────────────────────────────────────────────────────────────┘
```

## Quick Start

### Prerequisites
- Python 3.11+
- Docker & Docker Compose
- [uv](https://github.com/astral-sh/uv) (Python package manager)

### Local Development

```bash
# Install dependencies
make install

# Start LocalStack
make docker-up

# Run pipeline locally (DuckDB)
make run

# Run pipeline with Snowflake
make run-snowflake

# Run tests
make test

# Run linting
make lint
```

### Docker

```bash
# Build and run pipeline in Docker
make docker-build
make docker-pipeline

# Run with Snowflake destination
make docker-pipeline-snowflake
```

## GitHub Actions Workflows

The project uses GitHub Actions for CI/CD and scheduled pipeline runs.

### Workflows

| Workflow | Trigger | Description |
|----------|---------|-------------|
| `ci.yml` | PR/push | Linting, type checking, unit tests |
| `daily-pipeline.yml` | Daily 6 AM UTC | Full pipeline run against Snowflake |
| `data-quality.yml` | After pipeline | Soda data quality checks |

### Required Secrets

Configure these secrets in your GitHub repository settings:

| Secret | Description |
|--------|-------------|
| `SNOWFLAKE_ACCOUNT` | Snowflake account identifier (e.g., `abc12345.us-east-1`) |
| `SNOWFLAKE_USER` | Snowflake username |
| `SNOWFLAKE_PASSWORD` | Snowflake password |
| `SNOWFLAKE_DATABASE` | Target database (default: `GFN`) |
| `SNOWFLAKE_WAREHOUSE` | Compute warehouse (default: `COMPUTE_WH`) |
| `SNOWFLAKE_ROLE` | Role to use (default: `ACCOUNTADMIN`) |
| `SLACK_WEBHOOK_URL` | *(Optional)* Slack webhook for alerts |

## Project Structure

```
├── .github/workflows/     # GitHub Actions CI/CD
│   ├── ci.yml            # PR/push checks
│   ├── daily-pipeline.yml # Scheduled pipeline
│   └── data-quality.yml  # Soda checks
├── src/gfn_pipeline/     # Core pipeline code
│   ├── extract/          # Data extraction from GFN API
│   ├── transform/        # Data transformations
│   ├── load/             # Snowflake/DuckDB loaders
│   └── orchestrate/      # Pipeline orchestration
├── infrastructure/       # AWS/Snowflake setup scripts
├── soda/                 # Data quality checks
├── tests/                # Unit and integration tests
├── docker-compose.yml    # LocalStack + pipeline services
└── Makefile             # Build and run commands
```

## Documentation

- [Architecture](docs/architecture.md) - Detailed system design
- [Snowpipe Setup](docs/SNOWPIPE_SETUP.md) - Snowflake ingestion configuration

## License

MIT
