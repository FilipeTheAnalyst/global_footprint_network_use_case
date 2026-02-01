# ADR-004: Soda for Data Quality

## Status
Accepted

## Date
2024-01-17

## Context

Data quality validation is critical for a production pipeline. We need to:
- Validate data before loading to Snowflake
- Detect schema drift
- Alert on anomalies
- Maintain data contracts

### Options Considered
1. **Great Expectations** - Python-native, extensive features
2. **Soda Core** - YAML-based, simpler setup
3. **dbt Tests** - SQL-based, integrated with dbt
4. **Custom Python** - Hand-rolled validation

## Decision

**Use Soda Core for data quality checks.**

### Rationale

1. **YAML Configuration**: Non-engineers can write checks
2. **dlt Integration**: Works seamlessly with dlt pipelines
3. **Multi-Source**: Same checks work on DuckDB and Snowflake
4. **Anomaly Detection**: Built-in statistical checks
5. **Lightweight**: No heavy infrastructure required

## Implementation

### Check Categories

```yaml
# Schema checks
- schema:
    fail:
      when required column missing: [country_code, year, record]

# Freshness checks  
- freshness(extraction_timestamp) < 1d

# Volume checks
- row_count > 0
- row_count between 40000 and 60000

# Value checks
- invalid_percent(year) < 1%:
    valid min: 1961
    valid max: 2030
```

### Integration Points

1. **Pre-load**: Validate S3 data before Snowflake load
2. **Post-load**: Validate Snowflake tables after load
3. **Scheduled**: Daily anomaly detection scans

## Consequences

### Positive
- Declarative quality rules
- Easy to extend and maintain
- Good documentation of data expectations
- Integrates with alerting systems

### Negative
- Another tool to learn
- YAML can become verbose
- Some advanced checks require custom SQL

## Alternatives Rejected

### Great Expectations
- More powerful but more complex
- Heavier setup and maintenance
- Overkill for current scale

### dbt Tests
- Would require dbt adoption
- Less flexible for pre-load checks

### Custom Python
- More maintenance burden
- No standard patterns
- Harder to onboard new team members
