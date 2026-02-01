"""
Load data from LocalStack S3 into Snowflake.

Since Snowflake trial accounts don't support External Access Integrations,
this script downloads data from LocalStack and uploads it to Snowflake's
internal stage, then loads it into the target table.

Usage:
    # Load latest transformed file
    uv run python -m infrastructure.load_to_snowflake

    # Load specific file
    uv run python -m infrastructure.load_to_snowflake --file transformed/gfn_footprint_20260131_030733_transformed.json
"""

import argparse
import os
import tempfile
from pathlib import Path

import boto3
from botocore.config import Config

# Try to load .env
try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass

try:
    import snowflake.connector

    HAS_SNOWFLAKE = True
except ImportError:
    HAS_SNOWFLAKE = False


# Configuration
LOCALSTACK_ENDPOINT = os.getenv("LOCALSTACK_ENDPOINT", "http://localhost:4566")
S3_BUCKET = os.getenv("S3_BUCKET", "gfn-data-lake")

SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE", "GFN")


def get_s3_client():
    """Get boto3 S3 client for LocalStack."""
    return boto3.client(
        "s3",
        endpoint_url=LOCALSTACK_ENDPOINT,
        region_name="us-east-1",
        aws_access_key_id="test",
        aws_secret_access_key="test",
        config=Config(signature_version="s3v4"),
    )


def get_snowflake_connection():
    """Get Snowflake connection."""
    if not HAS_SNOWFLAKE:
        raise ImportError("snowflake-connector-python not installed")

    return snowflake.connector.connect(
        account=SNOWFLAKE_ACCOUNT,
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
    )


def list_processed_files():
    """List all transformed JSON files in LocalStack S3."""
    s3 = get_s3_client()

    response = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix="transformed/")

    files = []
    for obj in response.get("Contents", []):
        if obj["Key"].endswith(".json") and not obj["Key"].endswith(".keep"):
            files.append(obj["Key"])

    return sorted(files)


def download_from_localstack(s3_key: str, local_path: str):
    """Download file from LocalStack S3."""
    s3 = get_s3_client()
    s3.download_file(S3_BUCKET, s3_key, local_path)
    print(f"  Downloaded: s3://{S3_BUCKET}/{s3_key}")


def load_to_snowflake(local_path: str, source_file: str):
    """Upload file to Snowflake stage and load into table."""
    conn = get_snowflake_connection()
    cursor = conn.cursor()

    try:
        # Use the RAW schema
        cursor.execute("USE SCHEMA GFN.RAW")

        # Upload to internal stage using PUT
        put_sql = f"PUT file://{local_path} @gfn_data_stage AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
        cursor.execute(put_sql)
        print("  Uploaded to stage: @gfn_data_stage")

        # Get the filename
        filename = Path(local_path).name

        # Load data using COPY INTO with MATCH_BY_COLUMN_NAME
        copy_sql = f"""
        COPY INTO CARBON_FOOTPRINT_RAW (
            country_code, country_name, iso_alpha2, year,
            carbon_footprint_gha, total_footprint_gha, score,
            extracted_at, _source_file
        )
        FROM (
            SELECT
                $1:country_code::INTEGER,
                $1:country_name::VARCHAR,
                $1:iso_alpha2::VARCHAR,
                $1:year::INTEGER,
                $1:carbon_footprint_gha::FLOAT,
                $1:total_footprint_gha::FLOAT,
                $1:score::VARCHAR,
                TRY_TO_TIMESTAMP($1:extracted_at::VARCHAR),
                '{source_file}'
            FROM @gfn_data_stage/{filename}
        )
        FILE_FORMAT = (TYPE = JSON)
        ON_ERROR = CONTINUE
        """
        cursor.execute(copy_sql)

        # Get load results
        result = cursor.fetchone()
        if result:
            print(f"  Loaded: {result}")

        # Clean up stage
        cursor.execute(f"REMOVE @gfn_data_stage/{filename}")

        return True

    except Exception as e:
        print(f"  Error: {e}")
        return False
    finally:
        cursor.close()
        conn.close()


def main():
    parser = argparse.ArgumentParser(description="Load data from LocalStack to Snowflake")
    parser.add_argument("--file", help="Specific S3 key to load (default: latest)")
    parser.add_argument("--all", action="store_true", help="Load all processed files")
    args = parser.parse_args()

    print("=" * 60)
    print("  LocalStack → Snowflake Data Loader")
    print("=" * 60)

    # Check prerequisites
    if not HAS_SNOWFLAKE:
        print("Error: snowflake-connector-python not installed")
        print("  Run: uv add snowflake-connector-python")
        return

    if not SNOWFLAKE_ACCOUNT:
        print("Error: SNOWFLAKE_ACCOUNT not set in environment")
        return

    # Get files to load
    if args.file:
        files = [args.file]
    elif args.all:
        files = list_processed_files()
        print(f"\nFound {len(files)} processed files")
    else:
        files = list_processed_files()
        if files:
            files = [files[-1]]  # Latest file only
            print("\nLoading latest file (use --all for all files)")
        else:
            print("No processed files found in LocalStack S3")
            return

    # Load each file
    success_count = 0
    for s3_key in files:
        print(f"\n[{files.index(s3_key) + 1}/{len(files)}] {s3_key}")

        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as tmp:
            tmp_path = tmp.name

        try:
            download_from_localstack(s3_key, tmp_path)
            if load_to_snowflake(tmp_path, s3_key):
                success_count += 1
        finally:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)

    print(f"\n{'=' * 60}")
    print(f"  Loaded {success_count}/{len(files)} files successfully")
    print("=" * 60)

    # Show sample data
    if success_count > 0:
        print("\nVerifying data in Snowflake...")
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM GFN.RAW.CARBON_FOOTPRINT_RAW")
        count = cursor.fetchone()[0]
        print(f"  Total records in table: {count}")
        cursor.close()
        conn.close()


if __name__ == "__main__":
    main()
