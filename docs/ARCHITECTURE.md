┌─────────────────────────────────────────────────────────────────────────────────────────┐
│                                    AWS ARCHITECTURE                                      │
│                         GFN Carbon Footprint Ingestion Pipeline                          │
│                                                                                          │
│                              Updated: January 2026                                       │
└─────────────────────────────────────────────────────────────────────────────────────────┘


═══════════════════════════════════════════════════════════════════════════════════════════
                                    SYSTEM OVERVIEW
═══════════════════════════════════════════════════════════════════════════════════════════

    ┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
    │   TRIGGER   │────▶│   EXTRACT   │────▶│  TRANSFORM  │────▶│    LOAD     │
    │             │     │             │     │             │     │             │
    │ EventBridge │     │   Lambda    │     │   Lambda    │     │   Lambda    │
    │ API Gateway │     │   + SQS     │     │   + SQS     │     │  Snowpipe   │
    └─────────────┘     └──────┬──────┘     └──────┬──────┘     └──────┬──────┘
                               │                   │                   │
                               ▼                   ▼                   ▼
                        ┌─────────────────────────────────────────────────────┐
                        │                    S3 DATA LAKE                      │
                        │         raw/ ──▶ processed/ ──▶ Snowflake           │
                        └─────────────────────────────────────────────────────┘


═══════════════════════════════════════════════════════════════════════════════════════════
                                  DETAILED ARCHITECTURE
═══════════════════════════════════════════════════════════════════════════════════════════


                                    ┌─────────────────┐
                                    │   TRIGGERING    │
                                    ├─────────────────┤
                                    │                 │
                    ┌───────────────│  EventBridge    │───────────────┐
                    │               │  (Scheduler)    │               │
                    │               │                 │               │
                    │               │  Cron: Daily    │               │
                    │               │  0 6 * * ? *    │               │
                    │               │                 │               │
                    │               │  API Gateway    │               │
                    │               │  (Manual/Adhoc) │               │
                    │               └─────────────────┘               │
                    │                                                 │
                    ▼                                                 │
┌─────────────────────────────────┐                                   │
│         EXTRACTION              │                                   │
├─────────────────────────────────┤                                   │
│                                 │                                   │
│  ┌───────────┐    ┌──────────┐  │                                   │
│  │  Lambda   │───▶│   SQS    │  │                                   │
│  │ (Extract) │    │ (Buffer) │  │                                   │
│  └───────────┘    └────┬─────┘  │                                   │
│                        │        │                                   │
│  • Async HTTP (aiohttp)│        │                                   │
│  • Rate limiting       │        │                                   │
│  • Bulk API calls      │        │                                   │
│  • 8 concurrent req    │        │                                   │
│  • Retry with backoff  │        │                                   │
│                        │        │                                   │
└────────────────────────┼────────┘                                   │
                         │                                            │
                         ▼                                            │
┌─────────────────────────────────┐     ┌─────────────────────────────┐
│         RAW STORAGE             │     │       MONITORING            │
├─────────────────────────────────┤     ├─────────────────────────────┤
│                                 │     │                             │
│  ┌──────────────────────────┐   │     │  ┌───────────────────────┐  │
│  │      S3 Bucket           │   │     │  │    CloudWatch         │  │
│  │   (gfn-data-lake)        │   │     │  │                       │  │
│  │                          │   │     │  │  • Lambda Metrics     │  │
│  │  └─ raw/                 │   │     │  │  • Custom Dashboards  │  │
│  │     └─ carbon_footprint/ │   │     │  │  • Log Insights       │  │
│  │        └─ YYYY/          │   │     │  │  • Alarms (SNS)       │  │
│  │           └─ MM/         │   │     │  └───────────────────────┘  │
│  │              └─ data.json│   │     │                             │
│  └──────────────────────────┘   │     │  ┌───────────────────────┐  │
│                                 │     │  │    X-Ray Tracing      │  │
│  S3 Event Notification ─────────┼─────│  │  (End-to-end traces)  │  │
│                                 │     │  └───────────────────────┘  │
└─────────────────┬───────────────┘     └─────────────────────────────┘
                  │
                  ▼
┌─────────────────────────────────┐
│       TRANSFORMATION            │
├─────────────────────────────────┤
│                                 │
│  ┌───────────┐    ┌──────────┐  │
│  │  Lambda   │───▶│   SQS    │  │
│  │(Transform)│    │ (Buffer) │  │
│  └───────────┘    └────┬─────┘  │
│                        │        │
│  • Schema validation   │        │
│  • Data enrichment     │        │
│  • Deduplication       │        │
│  • Hash generation     │        │
│  • Parquet conversion  │        │
│                        │        │
└────────────────────────┼────────┘
                         │
                         ▼
┌─────────────────────────────────┐
│      PROCESSED STORAGE          │
├─────────────────────────────────┤
│                                 │
│  ┌──────────────────────────┐   │
│  │      S3 Bucket           │   │
│  │   (gfn-data-lake)        │   │
│  │                          │   │
│  │  └─ processed/           │   │
│  │     └─ carbon_footprint/ │   │
│  │        └─ YYYY/          │   │
│  │           └─ MM/         │   │
│  │              └─ data.parquet │
│  └──────────────────────────┘   │
│                                 │
└─────────────────┬───────────────┘
                  │
                  ▼
┌─────────────────────────────────┐     ┌─────────────────────────────┐
│       ORCHESTRATION             │     │      ERROR HANDLING         │
├─────────────────────────────────┤     ├─────────────────────────────┤
│                                 │     │                             │
│  ┌──────────────────────────┐   │     │  ┌───────────────────────┐  │
│  │    Step Functions        │   │     │  │    SQS DLQ            │  │
│  │                          │   │     │  │  (Dead Letter Queue)  │  │
│  │  ┌─────┐   ┌─────────┐   │   │     │  │                       │  │
│  │  │Start│──▶│ Extract │   │   │     │  │  • Failed messages    │  │
│  │  └─────┘   └────┬────┘   │   │     │  │  • 3 retry attempts   │  │
│  │                 │        │   │     │  │  • 14 day retention   │  │
│  │                 ▼        │   │     │  └───────────────────────┘  │
│  │           ┌──────────┐   │   │     │                             │
│  │           │Transform │   │   │     │  ┌───────────────────────┐  │
│  │           └────┬─────┘   │   │     │  │    SNS Topic          │  │
│  │                │         │   │     │  │  (Notifications)      │  │
│  │                ▼         │   │     │  │                       │  │
│  │           ┌──────────┐   │   │     │  │  • Success alerts     │  │
│  │           │   Load   │   │   │     │  │  • Failure alerts     │  │
│  │           └────┬─────┘   │   │     │  │  • Email/Slack/PD     │  │
│  │                │         │   │     │  └───────────────────────┘  │
│  │                ▼         │   │     │                             │
│  │           ┌──────────┐   │   │     └─────────────────────────────┘
│  │           │ Notify   │   │   │
│  │           └──────────┘   │   │
│  │                          │   │
│  └──────────────────────────┘   │
│                                 │
└─────────────────┬───────────────┘
                  │
                  ▼
┌─────────────────────────────────┐
│          LOADING                │
├─────────────────────────────────┤
│                                 │
│  ┌───────────┐                  │
│  │  Lambda   │                  │
│  │  (Load)   │                  │
│  └─────┬─────┘                  │
│        │                        │
│        │  Option A: Snowpipe    │
│        │  (Auto-ingest)         │
│        │                        │
│        │  Option B: COPY INTO   │
│        │  (Batch load)          │
│        │                        │
└────────┼────────────────────────┘
         │
         ▼
┌─────────────────────────────────┐
│        TARGET DATABASE          │
├─────────────────────────────────┤
│                                 │
│  ┌──────────────────────────┐   │
│  │       Snowflake          │   │
│  │                          │   │
│  │  ┌────────────────────┐  │   │
│  │  │    RAW Schema      │  │   │
│  │  │ CARBON_FOOTPRINT_  │  │   │
│  │  │ RAW                │  │   │
│  │  └─────────┬──────────┘  │   │
│  │            │             │   │
│  │            │ dbt         │   │
│  │            ▼             │   │
│  │  ┌────────────────────┐  │   │
│  │  │   MART Schema      │  │   │
│  │  │ CARBON_FOOTPRINT   │  │   │
│  │  │ (Aggregated)       │  │   │
│  │  └────────────────────┘  │   │
│  │                          │   │
│  └──────────────────────────┘   │
│                                 │
└─────────────────────────────────┘


═══════════════════════════════════════════════════════════════════════════════════════════
                                   PROJECT STRUCTURE
═══════════════════════════════════════════════════════════════════════════════════════════

    global_footprint_network_use_case/
    │
    ├── src/gfn_pipeline/              # Core pipeline package
    │   ├── __init__.py                # Exports: run_pipeline, gfn_source
    │   ├── main.py                    # PipelineRunner class (CLI entry point)
    │   └── pipeline_async.py          # Async extraction with dlt
    │
    ├── infrastructure/                # AWS infrastructure
    │   ├── lambda_handlers.py         # Lambda functions (extract/transform/load)
    │   ├── setup_localstack.py        # LocalStack setup for local dev
    │   ├── setup_snowflake_production.py  # Snowflake production setup
    │   └── load_to_snowflake.py       # Snowflake loading utilities
    │
    ├── tests/                         # Test suite
    │   └── test_pipeline.py           # Pipeline tests
    │
    ├── data/                          # Local data storage
    │   └── raw/                       # Raw JSON extracts
    │
    ├── docs/                          # Documentation
    │   └── ARCHITECTURE.md            # This file
    │
    ├── docker-compose.yml             # LocalStack + pipeline containers
    ├── Dockerfile                     # Pipeline container image
    ├── Makefile                       # Build/run commands
    └── pyproject.toml                 # Dependencies (uv)


═══════════════════════════════════════════════════════════════════════════════════════════
                              OBSERVABILITY STRATEGY
═══════════════════════════════════════════════════════════════════════════════════════════

┌─────────────────────────────────────────────────────────────────────────────────────────┐
│                     RECOMMENDED: AWS-NATIVE OBSERVABILITY STACK                          │
└─────────────────────────────────────────────────────────────────────────────────────────┘

For an AWS + Snowflake data pipeline, the RECOMMENDED approach is AWS-native tools:

    ┌─────────────────────────────────────────────────────────────────────────────────┐
    │                                                                                  │
    │   ┌──────────────┐    ┌──────────────┐    ┌──────────────┐    ┌──────────────┐  │
    │   │  CloudWatch  │    │    X-Ray     │    │  CloudWatch  │    │  Snowflake   │  │
    │   │   Metrics    │    │   Tracing    │    │    Logs      │    │  Query Hist  │  │
    │   └──────┬───────┘    └──────┬───────┘    └──────┬───────┘    └──────┬───────┘  │
    │          │                   │                   │                   │          │
    │          └───────────────────┴───────────────────┴───────────────────┘          │
    │                                      │                                           │
    │                                      ▼                                           │
    │                         ┌────────────────────────┐                               │
    │                         │  CloudWatch Dashboards │                               │
    │                         │  + Alarms + SNS        │                               │
    │                         └────────────────────────┘                               │
    │                                                                                  │
    └─────────────────────────────────────────────────────────────────────────────────┘


┌─────────────────────────────────────────────────────────────────────────────────────────┐
│                         OBSERVABILITY STACK                                              │
└─────────────────────────────────────────────────────────────────────────────────────────┘

    ┌────────────────────────────────────────────────────────────────────────────────┐
    │  LAYER              │  TOOL                    │  PURPOSE                      │
    ├────────────────────────────────────────────────────────────────────────────────┤
    │  Metrics            │  CloudWatch Metrics      │  Lambda duration, errors,     │
    │                     │                          │  invocations, throttles       │
    │                     │                          │                               │
    │  Logs               │  CloudWatch Logs         │  Structured JSON logs,        │
    │                     │  + Log Insights          │  searchable, retention        │
    │                     │                          │                               │
    │  Tracing            │  AWS X-Ray               │  End-to-end request tracing   │
    │                     │                          │  across Lambda → S3 → SNS     │
    │                     │                          │                               │
    │  Alerting           │  CloudWatch Alarms       │  Threshold-based alerts       │
    │                     │  + SNS                   │  → Email/Slack/PagerDuty      │
    │                     │                          │                               │
    │  Dashboards         │  CloudWatch Dashboards   │  Unified view of pipeline     │
    │                     │                          │  health and performance       │
    │                     │                          │                               │
    │  Data Quality       │  Soda                    │  Row counts, freshness,       │
    │                     │                          │  schema validation, anomalies │
    │                     │                          │                               │
    │  Cost Monitoring    │  AWS Cost Explorer       │  Lambda/S3/Snowflake costs    │
    │                     │  + Budgets               │  with alerts                  │
    └────────────────────────────────────────────────────────────────────────────────┘


┌─────────────────────────────────────────────────────────────────────────────────────────┐
│                         DATA QUALITY WITH SODA                                           │
└─────────────────────────────────────────────────────────────────────────────────────────┘

    Soda provides data quality monitoring with native Snowflake integration:

    ┌────────────────────────────────────────────────────────────────────────────────┐
    │  CHECK TYPE         │  EXAMPLE                 │  PURPOSE                      │
    ├────────────────────────────────────────────────────────────────────────────────┤
    │  Freshness          │  freshness < 24h         │  Ensure data is recent        │
    │  Row Count          │  row_count > 0           │  Detect empty tables          │
    │  Missing Values     │  missing_count = 0       │  Validate required fields     │
    │  Duplicates         │  duplicate_count = 0     │  Ensure uniqueness            │
    │  Schema             │  schema validation       │  Detect column changes        │
    │  Anomaly Detection  │  anomaly score < 0.5     │  Detect unusual patterns      │
    └────────────────────────────────────────────────────────────────────────────────┘

    Usage:
    ```bash
    make soda-check  # Run all data quality checks
    ```


═══════════════════════════════════════════════════════════════════════════════════════════
                                  LOCAL DEVELOPMENT
═══════════════════════════════════════════════════════════════════════════════════════════

    ┌─────────────────────────────────────────────────────────────────────────────────┐
    │                           LOCALSTACK SIMULATION                                  │
    └─────────────────────────────────────────────────────────────────────────────────┘

    LocalStack provides local AWS service emulation for development:

    ┌────────────────────┐     ┌────────────────────┐     ┌────────────────────┐
    │    LocalStack      │     │    Docker          │     │    DuckDB          │
    │                    │     │                    │     │                    │
    │  • S3              │     │  • Lambda runtime  │     │  • Local testing   │
    │  • SQS             │     │  • API container   │     │  • Fast iteration  │
    │  • EventBridge     │     │  • Network bridge  │     │  • No cloud costs  │
    │  • SNS             │     │                    │     │                    │
    └────────────────────┘     └────────────────────┘     └────────────────────┘

    Commands:
    ┌────────────────────────────────────────────────────────────────────────────────┐
    │  make localstack-up      # Start LocalStack                                    │
    │  make setup-localstack   # Create S3/SQS/EventBridge resources                 │
    │  make test-extract       # Test extraction Lambda                              │
    │  make test-transform     # Test transformation Lambda                          │
    │  make test-load          # Test loading Lambda                                 │
    │  make run-pipeline       # Run full pipeline locally                           │
    └────────────────────────────────────────────────────────────────────────────────┘


═══════════════════════════════════════════════════════════════════════════════════════════
                                  DATA FLOW SUMMARY
═══════════════════════════════════════════════════════════════════════════════════════════

    ┌─────────────────────────────────────────────────────────────────────────────────┐
    │                                                                                  │
    │   GFN API ──▶ Lambda ──▶ S3 (raw/) ──▶ Lambda ──▶ S3 (processed/) ──▶ Snowflake │
    │     │           │           │             │              │               │       │
    │     │           │           │             │              │               │       │
    │   REST API   Extract     JSON          Transform     Parquet          COPY      │
    │   (bulk)     (async)    (raw)          (enrich)     (columnar)       INTO       │
    │                                                                                  │
    └─────────────────────────────────────────────────────────────────────────────────┘

    Data Formats:
    • Raw:       JSON (preserves API response structure)
    • Processed: Parquet (columnar, compressed, schema-enforced)
    • Target:    Snowflake tables (RAW → MART via dbt)
