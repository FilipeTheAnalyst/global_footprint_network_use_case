# Architecture Documentation

Detailed technical architecture for the GFN Carbon Footprint Ingestion Pipeline.

---

## Pipeline Approaches

This project supports two pipeline architectures:

| Approach | Entry Point | Best For | Features |
|----------|-------------|----------|----------|
| **AWS + dlt** (Recommended) | `main.py` + Lambda | Production | AWS orchestration, dlt loading, schema evolution, Soda checks |
| dlt Direct | `pipeline_async.py` | Quick local testing | Fast iteration, no AWS dependencies |

---

## Recommended Architecture: AWS Orchestration + dlt Loading

The production architecture combines **AWS services for orchestration** with **dlt for data loading**:

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                         AWS ORCHESTRATION + dlt LOADING                              │
├─────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                      │
│   EventBridge          Step Functions              Lambda Functions                  │
│   ───────────          ───────────────             ─────────────────                 │
│   Daily Schedule  ───▶  State Machine  ───▶  Extract ───▶ Transform ───▶ Load       │
│   (cron)               (orchestration)       (API→S3)    (validate)    (dlt→SF)     │
│                              │                                                       │
│                              ▼                                                       │
│                         SQS + DLQ                                                    │
│                         (retry/errors)                                               │
│                                                                                      │
│   ┌─────────────────────────────────────────────────────────────────────────────┐   │
│   │                           S3 DATA LAKE                                       │   │
│   │     raw/                    staged/                     (audit trail)        │   │
│   │     └── immutable  ───▶     └── validated  ───▶         replay capability    │   │
│   └─────────────────────────────────────────────────────────────────────────────┘   │
│                                      │                                               │
│                                      ▼                                               │
│                              ┌──────────────┐                                        │
│                              │     dlt      │                                        │
│                              │  • Schema    │                                        │
│                              │    Evolution │                                        │
│                              │  • Merge     │                                        │
│                              │  • State     │                                        │
│                              └──────┬───────┘                                        │
│                                     │                                                │
│                         ┌───────────┴───────────┐                                    │
│                         ▼                       ▼                                    │
│                   ┌──────────┐            ┌──────────┐                               │
│                   │ Snowflake│            │  DuckDB  │                               │
│                   │  (prod)  │            │  (dev)   │                               │
│                   └──────────┘            └──────────┘                               │
│                                                                                      │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

### Why This Architecture?

| Component | Purpose | Why Not Alternatives |
|-----------|---------|---------------------|
| **EventBridge** | Scheduling | Native AWS, cron expressions, no servers |
| **Step Functions** | Orchestration | Visual workflow, built-in retry, state tracking |
| **Lambda** | Compute | Serverless, pay-per-use, auto-scaling |
| **SQS** | Queuing | Decoupling, retry logic, dead letter queue |
| **S3** | Storage | Durability, audit trail, replay capability |
| **dlt** | Loading | Schema evolution, incremental loads, multi-destination |
| **CloudFormation** | IaC | Native AWS, version controlled infrastructure |

---

## Why dlt Instead of Snowpipe?

We chose **dlt for loading** over Snowpipe for several reasons:

| Aspect | dlt | Snowpipe |
|--------|-----|----------|
| **Schema Evolution** | ✅ Automatic | ❌ Manual ALTER TABLE |
| **Multi-Destination** | ✅ DuckDB + Snowflake | ❌ Snowflake only |
| **Incremental Loads** | ✅ Built-in state tracking | ❌ Requires custom logic |
| **Data Contracts** | ✅ Native support | ❌ Not available |
| **Local Development** | ✅ Same code, DuckDB | ❌ Requires Snowflake |
| **Merge/Upsert** | ✅ Declarative | ⚠️ Requires Stream + Task |
| **Batch Processing** | ✅ Optimized | ⚠️ Per-file overhead |

**When to use Snowpipe instead:**
- Real-time/streaming requirements (sub-minute latency)
- Very high volume continuous ingestion
- Native Snowflake-only deployments

> 📘 **Note**: For Snowpipe setup instructions, see [SNOWPIPE_SETUP.md](SNOWPIPE_SETUP.md). This can be used to evolve the architecture into a streaming batch service if real-time requirements emerge.

---

## Data Flow Detail

### Phase 1: Extract (Lambda)

```
GFN API ──────▶ Extract Lambda ──────▶ S3 raw/
                    │
                    ├── Async HTTP (aiohttp)
                    ├── Rate limiting (8 concurrent)
                    ├── Exponential backoff retry
                    └── Bulk endpoint (~66 API calls for 64 years)
```

### Phase 2: Transform (Lambda)

```
S3 raw/ ──────▶ Transform Lambda ──────▶ Soda Checks ──────▶ S3 staged/
                     │                        │
                     ├── Snake case           ├── Row count ≥ 1
                     ├── Deduplication        ├── Required fields not null
                     ├── Enrichment           ├── Year range validation
                     └── Timestamps           └── Record type validation
                                                       │
                                                       ▼ (on failure)
                                                   SQS DLQ + Alert
```

**Transformations Applied:**

| From | To |
|------|-----|
| `countryCode` | `country_code` |
| `countryName` | `country_name` |
| `isoa2` | `iso_alpha2` |
| `record` | `record_type` |
| - | `extracted_at`, `transformed_at` |
| Duplicates | Deduplicated by `(country_code, year, record_type)` |

### Phase 3: Load (Lambda + dlt)

```
S3 staged/ ──────▶ Load Lambda ──────▶ dlt Pipeline ──────▶ Snowflake
                                            │
                                            ├── Schema evolution
                                            ├── Incremental merge
                                            ├── State tracking
                                            └── Data contracts (optional)
```

---

## Step Functions State Machine

```
┌─────────────────────────────────────────────────────────────────┐
│                    STEP FUNCTIONS WORKFLOW                       │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│   [Start] ──▶ [Extract] ──▶ [Transform] ──▶ [Soda] ──▶ [Load]   │
│                   │              │            │           │      │
│                   │              │            │           │      │
│                   ▼              ▼            ▼           ▼      │
│              (on error)    (on error)   (on fail)   (on error)  │
│                   │              │            │           │      │
│                   └──────────────┴────────────┴───────────┘      │
│                                  │                               │
│                                  ▼                               │
│                          [Handle Error]                          │
│                                  │                               │
│                          ┌──────┴──────┐                         │
│                          │             │                         │
│                   attempts < 3    attempts ≥ 3                   │
│                          │             │                         │
│                          ▼             ▼                         │
│                      [Retry]     [Send to DLQ]                   │
│                          │             │                         │
│                          └─────▶ [End] ◀┘                        │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### State Machine Definition

```json
{
  "StartAt": "Extract",
  "States": {
    "Extract": {
      "Type": "Task",
      "Resource": "arn:aws:lambda:REGION:ACCOUNT:function:gfn-extract",
      "Next": "Transform",
      "Retry": [{"ErrorEquals": ["States.ALL"], "MaxAttempts": 3}],
      "Catch": [{"ErrorEquals": ["States.ALL"], "Next": "HandleError"}]
    },
    "Transform": {
      "Type": "Task",
      "Resource": "arn:aws:lambda:REGION:ACCOUNT:function:gfn-transform",
      "Next": "SodaChecks"
    },
    "SodaChecks": {
      "Type": "Choice",
      "Choices": [
        {"Variable": "$.soda_passed", "BooleanEquals": true, "Next": "Load"}
      ],
      "Default": "HandleError"
    },
    "Load": {
      "Type": "Task",
      "Resource": "arn:aws:lambda:REGION:ACCOUNT:function:gfn-load",
      "End": true
    },
    "HandleError": {
      "Type": "Task",
      "Resource": "arn:aws:lambda:REGION:ACCOUNT:function:gfn-error-handler",
      "Next": "SendToDLQ"
    },
    "SendToDLQ": {
      "Type": "Task",
      "Resource": "arn:aws:sqs:REGION:ACCOUNT:gfn-dlq",
      "End": true
    }
  }
}
```

---

## S3 Data Lake Structure

```
s3://gfn-data-lake/
├── raw/                                    # Immutable audit trail
│   └── gfn_footprint_{YYYYMMDD_HHMMSS}.json
├── staged/                                 # Validated, ready for dlt
│   └── gfn_footprint_{YYYYMMDD_HHMMSS}_staged.json
└── failed/                                 # Dead letter queue
    └── {original_filename}_failed.json
```

### File Naming Convention

| Stage | Pattern | Example |
|-------|---------|---------|
| Raw | `gfn_footprint_{timestamp}.json` | `gfn_footprint_20260131_030733.json` |
| Staged | `gfn_footprint_{timestamp}_staged.json` | `gfn_footprint_20260131_030733_staged.json` |

---

## Soda Data Quality Checks

### Two-Tier Validation Architecture

The pipeline uses a **two-tier validation** approach:

| Tier | File | When | Purpose |
|------|------|------|---------|
| **Staging (Pre-load)** | `soda/staging_checks.yml` | Before loading to destination | Validate data in Python before dlt load |
| **Post-load** | `soda/checks.yml` | After loading to Snowflake | Validate data in Snowflake tables |

```
Extract → S3 Raw → [Staging Checks] → S3 Staged → dlt → Snowflake → [Post-load Checks]
                         │                                                │
                   staging_checks.yml                               checks.yml
                   (Python validator)                              (Soda Core)
```

### Staging Checks (Pre-load)

Defined in `soda/staging_checks.yml`, executed by `src/gfn_pipeline/validators.py`:

```yaml
# soda/staging_checks.yml
footprint_data:
  row_count: {min: 1}
  required_columns: [country_code, country_name, year, record_type]
  valid_year_range: {min: 1960, max: 2030}
  valid_record_types: [EFConsTotGHA, BiocapTotGHA, ...]
  non_negative_value: true
  unique_key: [country_code, year, record_type]

countries:
  row_count: {min: 1}
  required_columns: [country_code, country_name]
  unique_key: [country_code]
  min_country_coverage: 150
```

### Post-load Checks (Snowflake)

Defined in `soda/checks.yml`, executed via `make run-soda`:

| Table | Check | Rule |
|-------|-------|------|
| `footprint_data` | Row count | ≥ 1 |
| `footprint_data` | Required columns | `country_code`, `country_name`, `year`, `record_type` not null |
| `footprint_data` | Year range | 1960 ≤ year ≤ 2030 |
| `footprint_data` | Record types | Must be in valid list (28 types) |
| `footprint_data` | Value | ≥ 0 (non-negative) |
| `footprint_data` | Unique key | No duplicates on `(country_code, year, record_type)` |
| `countries` | Row count | ≥ 1 |
| `countries` | Required columns | `country_code`, `country_name` not null |

### Running Soda Checks

```bash
# Enable Soda checks (fail on error)
make run-soda

# Soda checks in warn-only mode
make run-soda-warn

# Production: Snowflake + Soda + contracts
make run-production
```

---

## dlt Features

### Schema Evolution

When the GFN API adds new fields, dlt automatically evolves the schema:

```
Day 1: API returns { a, b, c }     → Schema: columns a, b, c
Day 2: API returns { a, b, c, d }  → Schema: columns a, b, c, d (auto-added)
```

### Incremental Loading

```
Run 1 (Initial):     Load 2010-2024, all records     → State: last_year = 2024
Run 2 (Incremental): Load 2023-2024 only, new data   → State: last_year = 2024
Run 3 (Full Refresh): Load 2010-2024, replace all    → State: reset
```

### Data Contracts

Optional strict schema enforcement:

```python
@dlt.resource(
    columns={
        "country_code": {"data_type": "bigint", "nullable": False},
        "year": {"data_type": "bigint", "nullable": False},
        "value": {"data_type": "double", "nullable": True},
    }
)
def footprint_data_resource(...):
    ...
```

---

## Error Handling

```
Error ──▶ Attempt 1 (immediate) ──▶ Attempt 2 (1 min) ──▶ Attempt 3 (5 min) ──▶ DLQ
                                                                                  │
                                                                                  ▼
                                                              CloudWatch Alarm ──▶ SNS ──▶ Alert
```

### Retry Configuration

| Attempt | Delay | Action |
|---------|-------|--------|
| 1 | Immediate | Retry same Lambda |
| 2 | 1 minute | Retry with backoff |
| 3 | 5 minutes | Final attempt |
| 4+ | - | Send to Dead Letter Queue |

---

## Observability Stack

| Component | Purpose | Implementation |
|-----------|---------|----------------|
| **Metrics** | Performance tracking | CloudWatch Metrics (duration, errors, throttles) |
| **Logs** | Debugging | CloudWatch Logs (structured JSON) |
| **Tracing** | End-to-end visibility | AWS X-Ray |
| **Alerting** | Incident response | CloudWatch Alarms → SNS → Email/Slack |
| **Data Quality** | Validation | Soda Checks |

---

## Local Development

```bash
# Start LocalStack + create resources
make setup

# Run dlt + S3 pipeline (recommended)
make run

# Run with Soda checks
make run-soda

# Run production config
make run-production

# Backfill operations
make backfill-recent          # 2020-2024
make backfill-full            # 1961-2024
make backfill YEARS=2010-2015 # Custom range

# Testing (all tests ~7s)
make test                     # All tests
make test-unit                # Unit tests only (no LocalStack)
make test-integration         # Integration tests (requires LocalStack)
```

LocalStack provides:
- S3 (data lake simulation)
- SQS (queue simulation)
- EventBridge (scheduling simulation)
- Step Functions (orchestration simulation)

---

## Project Structure

```
global_footprint_network_use_case/
│
├── src/gfn_pipeline/                    # Core pipeline package
│   ├── __init__.py                      # Exports: run_pipeline, gfn_source
│   ├── main.py                          # ✓ RECOMMENDED: dlt + S3 + Soda
│   ├── validators.py                    # Soda staging validators (loads YAML)
│   └── pipeline_async.py                # Async extraction with dlt (direct mode)
│
├── infrastructure/                      # AWS infrastructure
│   ├── lambda_handlers.py               # Lambda functions (extract/transform/load)
│   ├── setup_localstack.py              # LocalStack setup for local dev
│   ├── setup_snowflake_production.py    # Snowflake production setup
│   ├── cloudformation/                  # CloudFormation templates (IaC)
│   └── snowflake/                       # Snowflake SQL scripts
│
├── tests/                               # Test suite
│   └── test_pipeline.py                 # Unit and integration tests
│
├── soda/                                # Data quality (two-tier validation)
│   ├── configuration.yml                # Soda Snowflake connection config
│   ├── staging_checks.yml               # PRE-LOAD: Staging layer validation
│   └── checks.yml                       # POST-LOAD: Snowflake table validation
│
├── docs/                                # Documentation
│   ├── ARCHITECTURE.md                  # This file
│   └── SNOWPIPE_SETUP.md                # Snowpipe setup (streaming alternative)
│
├── docker-compose.yml                   # LocalStack + services
├── Makefile                             # Build/run commands
└── pyproject.toml                       # Dependencies (uv)
```

---

## Alternative: Snowpipe for Streaming

For real-time/streaming requirements, the architecture can evolve to use Snowpipe:

```
CURRENT (Batch with dlt):
S3 staged/ ──▶ dlt ──▶ Snowflake

ALTERNATIVE (Streaming with Snowpipe):
S3 transformed/ ──▶ SNS ──▶ Snowpipe ──▶ RAW table ──▶ Stream ──▶ Task ──▶ TRANSFORMED table
```

**When to consider Snowpipe:**
- Sub-minute latency requirements
- Continuous high-volume ingestion
- Native Snowflake ecosystem preference

> 📘 See [SNOWPIPE_SETUP.md](SNOWPIPE_SETUP.md) for detailed Snowpipe configuration.

---

## Idempotency Strategy

The pipeline is fully idempotent at multiple levels:

| Level | Mechanism | Implementation |
|-------|-----------|----------------|
| File | S3 object keys | Timestamp-based naming prevents overwrites |
| Record | Primary key | `(country_code, year, record_type)` |
| Load | dlt merge | Updates existing, inserts new |

### dlt Merge Pattern

```python
@dlt.resource(
    write_disposition="merge",
    primary_key=["country_code", "year", "record_type"],
)
def footprint_data_resource(...):
    ...
```

---

## API Efficiency

| Approach | API Calls (64 years) | Time |
|----------|---------------------|------|
| Per country/type | ~3,000+ | Hours |
| **Bulk /data/all/{year}** | **~66** | **~2-3 min** |

The pipeline uses the efficient bulk endpoint:
```
GET https://api.footprintnetwork.org/v1/data/all/{year}
```

Returns all countries and record types for a given year in a single call.
