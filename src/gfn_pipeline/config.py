import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    # --- Source API ---
    # API docs: http://data.footprintnetwork.org/#/api
    # Base URL: https://api.footprintnetwork.org/v1/[endpoint]
    gfn_api_base_url: str = os.getenv("GFN_API_BASE_URL", "https://api.footprintnetwork.org/v1")
    gfn_api_key: str = os.getenv(
        "GFN_API_KEY",
        None,  # Set via GFN_API_KEY environment variable
    )
    gfn_api_username: str = os.getenv("GFN_API_USERNAME", "any-user-name")
    gfn_mock: bool = os.getenv("GFN_MOCK", "0") == "1"

    # --- Output ---
    output_mode: str = os.getenv("OUTPUT_MODE", "local")  # local|s3
    local_output_dir: str = os.getenv("LOCAL_OUTPUT_DIR", "./data")

    # --- S3 (for AWS deployment) ---
    s3_bucket: str = os.getenv("S3_BUCKET", "")
    s3_prefix: str = os.getenv("S3_PREFIX", "gfn")

    # --- DuckDB (local testing) ---
    duckdb_path: str = os.getenv("DUCKDB_PATH", "./gfn.duckdb")

    # --- Snowflake (target DB) ---
    snowflake_account: str = os.getenv("SNOWFLAKE_ACCOUNT", "")
    snowflake_user: str = os.getenv("SNOWFLAKE_USER", "")
    snowflake_password: str = os.getenv("SNOWFLAKE_PASSWORD", "")
    snowflake_warehouse: str = os.getenv("SNOWFLAKE_WAREHOUSE", "")
    snowflake_database: str = os.getenv("SNOWFLAKE_DATABASE", "GFN")
    snowflake_schema: str = os.getenv("SNOWFLAKE_SCHEMA", "RAW")

    # --- Extraction parameters ---
    start_year: int = int(os.getenv("START_YEAR", "2010"))
    end_year: int = int(os.getenv("END_YEAR", "2024"))
