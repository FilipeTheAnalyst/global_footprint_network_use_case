# ADR-002: S3 as Staging Layer

## Status
Accepted

## Date
2024-01-15

## Context

Data flows from the GFN API to Snowflake. We need to decide whether to:

1. Load directly from API to Snowflake
2. Stage data in S3 first, then load to Snowflake

### Considerations
- Data auditability requirements
- Replay capability for failed loads
- Cost implications
- Complexity trade-offs

## Decision

**Stage all extracted data in S3 before loading to Snowflake.**

### Architecture
```
GFN API → S3 (raw/) → dlt → Snowflake
                ↓
         S3 (processed/)
```

### Rationale

1. **Audit Trail**: Raw API responses preserved for compliance
2. **Replay Capability**: Can re-process data without re-calling API
3. **Decoupling**: Extraction failures don't affect loading
4. **Cost Efficiency**: S3 storage is cheap; API rate limits are expensive
5. **Debugging**: Can inspect raw data when issues occur

## Implementation Details

### S3 Structure
```
s3://gfn-data-lake/
├── raw/
│   └── gfn/
│       └── year=2024/
│           └── month=01/
│               └── day=15/
│                   └── data_*.json
├── processed/
│   └── gfn/
│       └── (same structure)
└── failed/
    └── (quarantined records)
```

### Retention Policy
- `raw/`: 90 days (compliance requirement)
- `processed/`: 30 days
- `failed/`: 180 days (for investigation)

## Consequences

### Positive
- Full data lineage from API to warehouse
- Can replay any historical load
- Easier debugging of data issues
- Supports multiple downstream consumers

### Negative
- Additional S3 costs (~$0.023/GB/month)
- Slightly increased latency (write to S3 first)
- More moving parts to monitor

## Cost Analysis

For ~50K records/day at ~1KB each:
- Daily S3 storage: ~50MB
- Monthly storage: ~1.5GB
- Monthly cost: ~$0.04

**Conclusion**: Cost is negligible for the benefits gained.
