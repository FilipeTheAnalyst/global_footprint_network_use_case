GFN Carbon Footprint Pipeline
==============================

Ingests carbon footprint data from Global Footprint Network API into S3 and Snowflake.

QUICK START (local testing)
---------------------------
1) Install uv:
   brew install uv

2) Sync environment:
   uv sync

3) Run pipeline (local parquet + DuckDB):
   uv run python -m gfn_pipeline.pipeline --start-year 2010 --end-year 2015 --output-mode local --load-duckdb

SNOWFLAKE DEPLOYMENT
--------------------
Set environment variables:
   export SNOWFLAKE_ACCOUNT='your_account'
   export SNOWFLAKE_USER='your_user'
   export SNOWFLAKE_PASSWORD='your_password'
   export SNOWFLAKE_WAREHOUSE='your_warehouse'
   export SNOWFLAKE_DATABASE='GFN'
   export SNOWFLAKE_SCHEMA='RAW'

Run with Snowflake loading:
   uv run python -m gfn_pipeline.pipeline --start-year 2010 --end-year 2024 --output-mode local --load-snowflake

AWS S3 DEPLOYMENT
-----------------
   export S3_BUCKET='your-bucket'
   uv run python -m gfn_pipeline.pipeline --start-year 2010 --end-year 2024 --output-mode s3 --load-snowflake

SNOWFLAKE TABLES
----------------
RAW layer:  GFN.RAW.CARBON_FOOTPRINT_RAW
MART layer: GFN.MART.CARBON_FOOTPRINT (deduplicated, with carbon_pct_of_total)

PIP INSTALL (alternative)
-------------------------
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

API REFERENCE
-------------
- Docs: http://data.footprintnetwork.org/#/api
- Base URL: https://api.footprintnetwork.org/v1
- Auth: HTTP Basic (username=any, password=api_key)
