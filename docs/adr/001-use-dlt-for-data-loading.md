# ADR-001: Use dlt for Data Loading

## Status
Accepted

## Date
2024-01-15

## Context

We need a reliable way to load data from the GFN API into Snowflake. The options considered were:

1. **Raw Snowflake Python Connector** - Direct SQL execution
2. **Snowpipe** - Native Snowflake streaming ingestion
3. **dlt (data load tool)** - Python-native ELT framework
4. **Airbyte** - Managed connector platform

### Requirements
- Schema evolution support (API may add fields)
- Incremental loading (avoid full reloads)
- Merge/upsert capability (handle duplicates)
- Local development support
- Minimal infrastructure overhead

## Decision

**We chose dlt** for the following reasons:

### Advantages
1. **Schema Evolution**: Automatically detects and applies schema changes
2. **Incremental Loading**: Built-in state management for incremental extracts
3. **Multi-Destination**: Same code works for DuckDB (local) and Snowflake (prod)
4. **Merge Support**: Native merge/upsert with `write_disposition="merge"`
5. **Data Contracts**: Schema validation before loading
6. **Python-Native**: No external services required

### Trade-offs Accepted
- **Learning Curve**: Team needs to learn dlt patterns
- **Less Control**: Abstracts away some Snowflake-specific optimizations
- **Dependency**: Adds external library dependency

## Alternatives Rejected

### Snowpipe
- Requires S3 event notifications setup
- No built-in schema evolution
- More complex local development

### Raw Connector
- Manual schema management
- No incremental state tracking
- More boilerplate code

### Airbyte
- Infrastructure overhead (requires deployment)
- Overkill for single API source

## Consequences

### Positive
- 80% less boilerplate code for loading
- Automatic schema migrations
- Easy testing with DuckDB locally
- Built-in retry and error handling

### Negative
- Team dependency on dlt documentation
- Version upgrades may require code changes (e.g., API changes in v1.x)

## References
- [dlt Documentation](https://dlthub.com/docs)
- [dlt Snowflake Destination](https://dlthub.com/docs/dlt-ecosystem/destinations/snowflake)
