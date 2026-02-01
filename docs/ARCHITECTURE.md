# Architecture Documentation

Detailed technical architecture for the GFN Carbon Footprint Ingestion Pipeline.

---

## Pipeline Approaches

This project supports two pipeline architectures:

| Approach | Entry Point | Best For | Features |
|----------|-------------|----------|----------|
| **dlt + S3** (Recommended) | `main.py` | Production | Schema evolution, Soda checks, incremental loads |
| dlt Direct | `pipeline_async.py` | Quick local testing | Fast iteration, no S3 |

---

## System Overview

### Recommended Architecture (dlt + S3 Data Lake)

```mermaid
flowchart LR
    subgraph Trigger
        EB[EventBridge<br/>Scheduler]
        API[API Gateway<br/>Manual]
    end
    
    subgraph Extract
        EL[Extract<br/>Lambda/CLI]
    end
    
    subgraph S3["S3 Data Lake"]
        RAW[(raw/)]
        STAGED[(staged/)]
    end
    
    subgraph Quality
        SODA[Soda<br/>Checks]
    end
    
    subgraph Transform
        DLT[dlt<br/>Pipeline]
    end
    
    subgraph Destinations
        DUCK[(DuckDB)]
        SF[(Snowflake)]
    end
    
    EB --> EL
    API --> EL
    EL -->|JSON| RAW
    RAW --> SODA
    SODA -->|Validated| STAGED
    STAGED --> DLT
    DLT --> DUCK
    DLT --> SF
```

### Data Flow with Soda Checks

```mermaid
flowchart TB
    subgraph "1. Extract"
        GFN[GFN API] -->|Async HTTP| DATA[Raw Data]
    end
    
    subgraph "2. Store Raw"
        DATA -->|Immutable| S3RAW[S3: raw/]
    end
    
    subgraph "3. Transform"
        S3RAW --> TRANSFORM[Transform<br/>• Snake case<br/>• Dedup<br/>• Enrich]
    end
    
    subgraph "4. Soda Checks"
        TRANSFORM --> SODA{Soda<br/>Validation}
        SODA -->|Pass| S3STAGED[S3: staged/]
        SODA -->|Fail| ALERT[Alert &<br/>Stop Pipeline]
    end
    
    subgraph "5. Load via dlt"
        S3STAGED --> DLT[dlt Pipeline<br/>• Schema Evolution<br/>• Incremental Merge<br/>• State Tracking]
    end
    
    subgraph "6. Destinations"
        DLT --> DUCK[(DuckDB)]
        DLT --> SF[(Snowflake)]
    end
    
    style SODA fill:#f9f,stroke:#333
    style ALERT fill:#f66,stroke:#333
```

---

## S3 Data Lake Structure

```
s3://gfn-data-lake/
├── raw/                                    # Immutable audit trail
│   └── gfn_footprint_{YYYYMMDD_HHMMSS}.json
├── staged/                                 # Validated, ready for dlt
│   └── gfn_footprint_{YYYYMMDD_HHMMSS}_staged.json
├── transformed/                            # Legacy: for Snowpipe
│   └── gfn_footprint_{YYYYMMDD_HHMMSS}_transformed.json
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

### Staging Layer Checks

The pipeline runs Soda-style checks on the staging layer before loading to destinations:

```mermaid
flowchart LR
    subgraph "Soda Checks"
        direction TB
        RC[Row Count<br/>≥ 1 record]
        REQ[Required Columns<br/>Not Null]
        YEAR[Year Range<br/>1960-2030]
        TYPE[Record Types<br/>Valid Values]
        VAL[Values<br/>Non-negative]
        DUP[Duplicates<br/>Unique Keys]
    end
    
    DATA[Transformed<br/>Data] --> RC
    RC --> REQ
    REQ --> YEAR
    YEAR --> TYPE
    TYPE --> VAL
    VAL --> DUP
    DUP --> RESULT{All Pass?}
    RESULT -->|Yes| LOAD[Continue to Load]
    RESULT -->|No| FAIL[Fail Pipeline]
```

### Check Definitions

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
| `countries` | Coverage | ≥ 150 unique countries |

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

## Data Transformation Flow

```mermaid
flowchart LR
    subgraph "GFN API Response"
        API["{ countryCode: 238,<br/>  countryName: '...',<br/>  isoa2: 'ET',<br/>  year: 2010,<br/>  value: 5760020,<br/>  record: '...' }"]
    end
    
    subgraph "Transform"
        T["{ country_code: 238,<br/>  country_name: '...',<br/>  iso_alpha2: 'ET',<br/>  year: 2010,<br/>  value: 5760020.03,<br/>  record_type: '...',<br/>  extracted_at: '...',<br/>  transformed_at: '...' }"]
    end
    
    subgraph "Snowflake Tables"
        RAW[RAW.FOOTPRINT_DATA_RAW<br/>VARIANT column]
        TRANS[TRANSFORMED.FOOTPRINT_DATA<br/>Structured columns]
        ANAL[ANALYTICS.FOOTPRINT_SUMMARY<br/>Aggregates]
    end
    
    API --> T
    T --> RAW
    RAW -->|Stream + Task| TRANS
    TRANS -->|Task| ANAL
```

### Transformations Applied

| Transformation | From | To |
|---------------|------|-----|
| Snake case | `countryCode` | `country_code` |
| Standardize | `isoa2` | `iso_alpha2` |
| Add timestamps | - | `extracted_at`, `transformed_at` |
| Deduplicate | - | By `(country_code, year, record_type)` |

---

## dlt Features

### Schema Evolution

```mermaid
flowchart LR
    subgraph "Day 1"
        API1["API Response<br/>{ a, b, c }"]
        SCHEMA1["Schema<br/>columns: a, b, c"]
    end
    
    subgraph "Day 2 - API adds field"
        API2["API Response<br/>{ a, b, c, d }"]
        SCHEMA2["Schema<br/>columns: a, b, c, d<br/>✓ Auto-evolved"]
    end
    
    API1 --> SCHEMA1
    API2 --> SCHEMA2
    SCHEMA1 -.->|"dlt detects<br/>new column"| SCHEMA2
```

### Incremental Loading

```mermaid
flowchart TB
    subgraph "Run 1 - Initial"
        R1[Load 2010-2024<br/>All records]
        S1[State: last_year = 2024]
    end
    
    subgraph "Run 2 - Incremental"
        R2[Load 2023-2024 only<br/>New/changed records]
        S2[State: last_year = 2024]
    end
    
    subgraph "Run 3 - Full Refresh"
        R3[Load 2010-2024<br/>Replace all]
        S3[State: reset]
    end
    
    R1 --> S1
    S1 -.->|"Next run"| R2
    R2 --> S2
```

### Data Contracts

```mermaid
flowchart LR
    subgraph "Contract Definition"
        C["columns:<br/>  country_code: bigint, NOT NULL<br/>  year: bigint, NOT NULL<br/>  value: double, nullable"]
    end
    
    subgraph "Validation"
        V{Contract<br/>Enabled?}
        PASS[✓ Load]
        FAIL[✗ Skip Record]
    end
    
    DATA[Record] --> V
    V -->|Yes| C
    C -->|Valid| PASS
    C -->|Invalid| FAIL
    V -->|No| PASS
```

---

## AWS Lambda Architecture (Legacy)

```mermaid
flowchart TB
    subgraph Triggers
        EB[EventBridge<br/>Cron: 0 6 * * ?]
        APIGW[API Gateway<br/>Manual]
    end
    
    subgraph "Extract Lambda"
        EL[Lambda<br/>• Async HTTP<br/>• Rate limiting<br/>• 8 concurrent<br/>• Retry backoff]
    end
    
    subgraph "Transform Lambda"
        TL[Lambda<br/>• Schema validation<br/>• Enrichment<br/>• Deduplication<br/>• Hash unique key]
    end
    
    subgraph "Load Lambda"
        LL[Lambda<br/>• Snowflake COPY<br/>• Merge logic]
    end
    
    subgraph Queues
        SQS1[SQS: gfn-transform]
        SQS2[SQS: gfn-load]
        DLQ[SQS: gfn-dlq]
    end
    
    subgraph Storage
        S3R[(S3: raw/)]
        S3T[(S3: transformed/)]
    end
    
    EB --> EL
    APIGW --> EL
    EL --> S3R
    EL --> SQS1
    SQS1 --> TL
    TL --> S3T
    TL --> SQS2
    SQS2 --> LL
    
    EL -.->|Failure| DLQ
    TL -.->|Failure| DLQ
    LL -.->|Failure| DLQ
```

---

## Snowflake Data Flow

```mermaid
flowchart TB
    subgraph "S3"
        S3T[(transformed/)]
    end
    
    subgraph "Snowpipe"
        SNS[SNS Topic]
        PIPE[FOOTPRINT_DATA_PIPE<br/>Auto-ingest]
    end
    
    subgraph "RAW Schema"
        RAW[(FOOTPRINT_DATA_RAW<br/>VARIANT + metadata)]
        STREAM[FOOTPRINT_DATA_STREAM]
    end
    
    subgraph "TRANSFORMED Schema"
        TASK1[PROCESS_FOOTPRINT_DATA_TASK]
        TRANS[(FOOTPRINT_DATA<br/>Structured columns)]
    end
    
    subgraph "ANALYTICS Schema"
        TASK2[UPDATE_ANALYTICS_TASK]
        ANAL[(FOOTPRINT_SUMMARY<br/>Aggregates)]
    end
    
    S3T -->|ObjectCreated| SNS
    SNS --> PIPE
    PIPE --> RAW
    RAW --> STREAM
    STREAM --> TASK1
    TASK1 -->|MERGE| TRANS
    TRANS --> TASK2
    TASK2 --> ANAL
```

---

## Error Handling

```mermaid
flowchart LR
    subgraph "Retry Strategy"
        A1[Attempt 1<br/>Immediate]
        A2[Attempt 2<br/>1 min delay]
        A3[Attempt 3<br/>5 min delay]
        DLQ[(Dead Letter<br/>Queue)]
    end
    
    subgraph "Alerting"
        CW[CloudWatch<br/>Alarms]
        SNS[SNS Topic]
        NOTIFY[Email/Slack/<br/>PagerDuty]
    end
    
    ERROR[Error] --> A1
    A1 -->|Fail| A2
    A2 -->|Fail| A3
    A3 -->|Fail| DLQ
    
    DLQ --> CW
    CW --> SNS
    SNS --> NOTIFY
```

---

## Observability Stack

```mermaid
flowchart TB
    subgraph "Metrics"
        CWM[CloudWatch Metrics<br/>• Lambda duration<br/>• Errors, invocations<br/>• Throttles]
    end
    
    subgraph "Logs"
        CWL[CloudWatch Logs<br/>• Structured JSON<br/>• Log Insights<br/>• Retention]
    end
    
    subgraph "Tracing"
        XRAY[AWS X-Ray<br/>• End-to-end tracing<br/>• Lambda → S3 → SF]
    end
    
    subgraph "Alerting"
        CWA[CloudWatch Alarms<br/>• Threshold-based<br/>• SNS → Email/Slack]
    end
    
    subgraph "Data Quality"
        SODA[Soda Checks<br/>• Row counts<br/>• Freshness<br/>• Schema validation]
    end
    
    subgraph "Snowflake"
        SFMON[MONITORING Schema<br/>• Pipeline dashboard<br/>• Load history<br/>• Task history]
    end
    
    PIPELINE[Pipeline] --> CWM
    PIPELINE --> CWL
    PIPELINE --> XRAY
    CWM --> CWA
    PIPELINE --> SODA
    PIPELINE --> SFMON
```

### Snowflake Monitoring Views

| View | Purpose |
|------|---------|
| `V_PIPELINE_DASHBOARD` | Overall pipeline health |
| `V_SNOWPIPE_LOAD_HISTORY` | File load status |
| `V_TASK_HISTORY` | Task execution history |
| `V_DATA_FRESHNESS` | Data recency by table |
| `DATA_QUALITY_METRICS` | Quality check results |

---

## Local Development

```mermaid
flowchart LR
    subgraph "LocalStack"
        LS[Docker Container<br/>• S3<br/>• SQS<br/>• EventBridge<br/>• SNS]
    end
    
    subgraph "Local DB"
        DUCK[(DuckDB<br/>• Fast iteration<br/>• No cloud costs)]
    end
    
    subgraph "Pipeline"
        CLI[CLI / Makefile<br/>• make run<br/>• make run-soda]
    end
    
    CLI --> LS
    CLI --> DUCK
```

### Commands

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
```

---

## Project Structure

```
global_footprint_network_use_case/
│
├── src/gfn_pipeline/                    # Core pipeline package
│   ├── __init__.py                      # Exports: run_pipeline, gfn_source
│   ├── main.py                          # ✓ RECOMMENDED: dlt + S3 + Soda
│   └── pipeline_async.py                # Async extraction with dlt (direct mode)
│
├── infrastructure/                      # AWS infrastructure
│   ├── lambda_handlers.py               # Lambda functions (extract/transform/load)
│   ├── setup_localstack.py              # LocalStack setup for local dev
│   ├── setup_snowflake_production.py    # Snowflake production setup
│   └── snowflake/                       # Snowflake SQL scripts
│
├── tests/                               # Test suite
│   └── test_pipeline.py                 # Unit and integration tests
│
├── soda/                                # Data quality
│   └── checks.yml                       # Soda check definitions (Snowflake)
│
├── data/                                # Local data storage
│   ├── raw/                             # Raw JSON extracts
│   └── processed/                       # Processed data
│
├── docs/                                # Documentation
│   ├── ARCHITECTURE.md                  # This file
│   └── SNOWPIPE_SETUP.md                # Snowpipe setup guide
│
├── docker-compose.yml                   # LocalStack + services
├── Makefile                             # Build/run commands
└── pyproject.toml                       # Dependencies (uv)
```

---

## Idempotency Strategy

The pipeline is fully idempotent at multiple levels:

| Level | Mechanism | Implementation |
|-------|-----------|----------------|
| File | `LOAD_HISTORY` table | Tracks processed files by name |
| Record | Primary key | `(country_code, year, record_type)` |
| Load | `MERGE` statement | Updates existing, inserts new |

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
