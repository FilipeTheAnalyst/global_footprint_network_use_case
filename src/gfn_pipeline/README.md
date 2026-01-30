# GFN Pipeline

Data pipeline for extracting carbon footprint data from the Global Footprint Network API.

## Quick Start

```bash
# Set your API key
export GFN_API_KEY=your_api_key_here

# Or create a .env file (see .env.example)

# Run the pipeline (DuckDB by default)
uv run python -m gfn_pipeline.pipeline_async

# Load to Snowflake
uv run python -m gfn_pipeline.pipeline_async --destination snowflake
```

## Pipeline Options

```bash
python -m gfn_pipeline.pipeline_async --help

Options:
  --destination {duckdb,snowflake,filesystem,bigquery}
  --start-year YEAR     First year to extract (default: 2010)
  --end-year YEAR       Last year to extract (default: 2024)
  --full-refresh        Replace all data instead of merge
  --no-parallel         Use sequential extraction (slower)
```

## Architecture

The pipeline uses:
- **dlt (data load tool)**: Schema contracts, state management, pluggable destinations
- **Async parallelism**: 8 concurrent requests with token bucket rate limiting
- **Bulk API endpoint**: 1 request per country (not per year) = 15x fewer requests

## Files

| File | Description |
|------|-------------|
| `pipeline_async.py` | **Main pipeline** - async parallel + dlt |
| `pipeline.py` | Legacy sequential version (deprecated) |
| `config.py` | Environment configuration |
| `load_snowflake.py` | Snowflake-specific loading logic |

## Performance

- Full extraction (2010-2024, 270 countries): ~60-80 seconds
- Incremental (no new data): ~2 seconds
- Records: ~2,800 (270 countries × ~10 years with data)
