# AWS Architecture: GFN Carbon Footprint Ingestion Pipeline

## High-Level Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────────────────────────┐
│                                    AWS CLOUD                                             │
│                                                                                          │
│  ┌──────────────┐     ┌─────────────────────────────────────────────────────────────┐   │
│  │              │     │                    STEP FUNCTIONS                            │   │
│  │  EventBridge │────▶│  ┌─────────┐   ┌─────────┐   ┌─────────┐   ┌─────────────┐  │   │
│  │  (Schedule)  │     │  │ Fetch   │──▶│ Store   │──▶│Transform│──▶│ Load to     │  │   │
│  │  Daily/Weekly│     │  │Countries│   │ RAW S3  │   │ Data    │   │ Snowflake   │  │   │
│  └──────────────┘     │  └─────────┘   └─────────┘   └─────────┘   └─────────────┘  │   │
│         │             │       │             │             │              │           │   │
│         │             └───────┼─────────────┼─────────────┼──────────────┼───────────┘   │
│         │                     │             │             │              │               │
│         ▼                     ▼             ▼             ▼              ▼               │
│  ┌──────────────┐     ┌─────────────┐ ┌──────────┐ ┌──────────────┐ ┌──────────────┐    │
│  │ API Gateway  │     │   Lambda    │ │    S3    │ │    Glue      │ │  Snowflake   │    │
│  │ (On-demand   │────▶│  Functions  │ │  Bucket  │ │   (ETL)      │ │   (Target)   │    │
│  │  trigger)    │     │             │ │  Bronze  │ │   or         │ │              │    │
│  └──────────────┘     └─────────────┘ │  Silver  │ │   Lambda     │ └──────────────┘    │
│                              │        └──────────┘ └──────────────┘        ▲            │
│                              │             │              │                │            │
│                              ▼             ▼              ▼                │            │
│                       ┌─────────────────────────────────────────┐         │            │
│                       │           SECRETS MANAGER               │         │            │
│                       │  • GFN API Key                          │─────────┘            │
│                       │  • Snowflake Credentials                │                      │
│                       └─────────────────────────────────────────┘                      │
│                                                                                         │
│  ┌──────────────────────────────────────────────────────────────────────────────────┐  │
│  │                              MONITORING & ALERTING                                │  │
│  │  ┌────────────┐  ┌────────────┐  ┌────────────┐  ┌────────────┐  ┌────────────┐  │  │
│  │  │ CloudWatch │  │ CloudWatch │  │    SQS     │  │    SNS     │  │   Slack/   │  │  │
│  │  │   Logs     │  │   Alarms   │  │    DLQ     │  │   Topics   │  │   Email    │  │  │
│  │  └────────────┘  └────────────┘  └────────────┘  └────────────┘  └────────────┘  │  │
│  └──────────────────────────────────────────────────────────────────────────────────┘  │
│                                                                                         │
└─────────────────────────────────────────────────────────────────────────────────────────┘

                                         │
                                         ▼
                              ┌─────────────────────┐
                              │  GFN Public API     │
                              │  api.footprint      │
                              │  network.org/v1     │
                              └─────────────────────┘
```

## Component Details

### 1. TRIGGERING SYSTEM

| Component | Purpose | Configuration |
|-----------|---------|---------------|
| **EventBridge** | Scheduled execution | Cron: `0 6 * * MON` (weekly) or daily |
| **API Gateway** | On-demand/backfill triggers | REST endpoint for manual runs |

### 2. ORCHESTRATION (Step Functions)

State machine workflow:
1. **FetchCountries** - Get list of countries from `/countries`
2. **FanOut** - Parallel execution per country/year (Map state)
3. **FetchData** - Call `/data/{country}/{year}/EFCtot` 
4. **StoreRAW** - Write JSON/Parquet to S3 Bronze
5. **Transform** - Clean, dedupe, compute derived fields
6. **LoadSnowflake** - COPY INTO or Snowpipe

### 3. RAW DATA STORAGE (S3)

```
s3://gfn-data-lake/
├── bronze/                          # RAW (immutable)
│   └── carbon_footprint/
│       └── year=2010/
│           └── ingest_dt=2026-01-30/
│               └── part-00000.parquet
├── silver/                          # Cleaned, typed
│   └── carbon_footprint/
│       └── year=2010/
│           └── data.parquet
└── gold/                            # Aggregated (optional)
```

### 4. PROCESSING OPTIONS

| Option | When to Use | Pros | Cons |
|--------|-------------|------|------|
| **Lambda** | Small payloads (<15 min) | Serverless, cheap | Timeout limits |
| **Glue (Spark)** | Large transforms | Scalable, managed | Cold start, cost |
| **ECS Fargate** | Long-running, custom | Flexible | More ops overhead |

**Recommendation**: Lambda for API calls + S3 writes; Glue for heavy transforms.

### 5. SNOWFLAKE INTEGRATION

Two patterns:

**A) Snowpipe (Auto-ingest)**
```
S3 Event → SQS → Snowpipe → RAW table
```
- Pros: Near real-time, fully managed
- Cons: Less control over batching

**B) Orchestrated COPY (Recommended)**
```
Step Functions → Lambda → COPY INTO → MERGE into MART
```
- Pros: Full control, idempotent, audit trail
- Cons: Slightly higher latency

### 6. MONITORING SYSTEM

| Component | Monitors | Alert Condition |
|-----------|----------|-----------------|
| **CloudWatch Logs** | Lambda/Glue execution | Error patterns |
| **CloudWatch Alarms** | Step Functions failures | ExecutionsFailed > 0 |
| **SQS DLQ** | Failed messages | Messages in DLQ > 0 |
| **SNS** | Notifications | Triggers Slack/Email |
| **Custom Metrics** | Row counts, latency | Deviation from baseline |

---

## Data Flow Summary

```
GFN API ──▶ Lambda ──▶ S3 Bronze ──▶ Glue/Lambda ──▶ S3 Silver ──▶ Snowflake RAW ──▶ Snowflake MART
   │                      │                              │               │                │
   │                      │                              │               │                │
   ▼                      ▼                              ▼               ▼                ▼
HTTP Basic         Parquet files              Parquet files        COPY INTO          MERGE
Auth               partitioned by            cleaned, typed        staging          deduplicated
                   year/date                                                        + derived cols
```

---

## Snowflake Table Design

### RAW Layer
```sql
CREATE TABLE GFN.RAW.CARBON_FOOTPRINT_RAW (
    country_code INTEGER,
    country_name VARCHAR,
    iso_alpha2 VARCHAR(2),
    year INTEGER,
    carbon_footprint_gha FLOAT,
    total_footprint_gha FLOAT,
    score VARCHAR(10),
    record_hash VARCHAR(64),
    extracted_at TIMESTAMP_NTZ,
    loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
```

### MART Layer (Dashboard-ready)
```sql
CREATE TABLE GFN.MART.CARBON_FOOTPRINT (
    country_code INTEGER,
    country_name VARCHAR,
    iso_alpha2 VARCHAR(2),
    year INTEGER,
    carbon_footprint_gha FLOAT,
    total_footprint_gha FLOAT,
    carbon_pct_of_total FLOAT,  -- Derived: carbon/total * 100
    score VARCHAR(10),
    PRIMARY KEY (country_code, year)
);
```

---

## Cost Optimization

1. **S3 Lifecycle**: Move Bronze to Glacier after 90 days
2. **Lambda**: Use ARM64 (Graviton2) for 20% cost savings
3. **Glue**: Use Flex execution for non-time-critical jobs
4. **Snowflake**: Auto-suspend warehouse after 60s idle

---

## Security

- **Secrets Manager**: Store API key + Snowflake creds (rotate every 90 days)
- **IAM**: Least privilege (Lambda role can only write to specific S3 prefix)
- **S3**: SSE-KMS encryption, bucket policy restricts access
- **VPC**: Optional private endpoints for Snowflake
