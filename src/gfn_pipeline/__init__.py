"""Global Footprint Network ingestion pipeline.

Usage:
    from gfn_pipeline import run_pipeline
    
    # Run with defaults (DuckDB destination)
    run_pipeline()
    
    # Run with Snowflake
    run_pipeline(destination="snowflake")
"""
from gfn_pipeline.pipeline_async import run_pipeline, gfn_source

__all__ = ["run_pipeline", "gfn_source"]
