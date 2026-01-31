"""
Lambda Functions for GFN Pipeline.

These handlers are designed to run as AWS Lambda functions.
Each function is decoupled and communicates via S3/SQS.

Uses the efficient bulk API endpoint /data/all/{year} for extraction,
which returns ALL countries × ALL record types in a single call.

Local testing:
    python -m infrastructure.lambda_handlers extract
    python -m infrastructure.lambda_handlers transform --s3-key raw/footprint/2024/data.json
    python -m infrastructure.lambda_handlers load --s3-key processed/footprint/2024/data.json
"""
from __future__ import annotations

import asyncio
import json
import os
import time
import logging
from datetime import datetime, timezone
from typing import Any

import aiohttp
import boto3
from botocore.config import Config

# Configure logging
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
logger = logging.getLogger(__name__)

# Configuration
LOCALSTACK_ENDPOINT = os.getenv("AWS_ENDPOINT_URL") or os.getenv("LOCALSTACK_ENDPOINT", "http://localhost:4566")
S3_BUCKET = os.getenv("S3_BUCKET", "gfn-data-lake")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
GFN_API_KEY = os.getenv("GFN_API_KEY")
GFN_API_BASE_URL = "https://api.footprintnetwork.org/v1"


# When running inside Docker (Lambda), use host.docker.internal to reach LocalStack on host
def _get_endpoint_url() -> str | None:
    """Get the correct endpoint URL for LocalStack based on execution context."""
    endpoint = LOCALSTACK_ENDPOINT
    if not endpoint:
        return None

    # Check if we're running inside Lambda (Docker container)
    if os.getenv("AWS_LAMBDA_FUNCTION_NAME"):
        # Replace localhost with host.docker.internal for Docker networking
        endpoint = endpoint.replace("localhost", "host.docker.internal")
        endpoint = endpoint.replace("127.0.0.1", "host.docker.internal")

    return endpoint


def get_s3_client():
    """Get S3 client (LocalStack or AWS)."""
    endpoint_url = _get_endpoint_url()
    kwargs = {
        "region_name": AWS_REGION,
        "config": Config(signature_version="s3v4"),
    }
    if endpoint_url and ("localhost" in endpoint_url or "host.docker.internal" in endpoint_url):
        kwargs["endpoint_url"] = endpoint_url
        kwargs["aws_access_key_id"] = "test"
        kwargs["aws_secret_access_key"] = "test"
    return boto3.client("s3", **kwargs)


def get_sqs_client():
    """Get SQS client (LocalStack or AWS)."""
    endpoint_url = _get_endpoint_url()
    kwargs = {"region_name": AWS_REGION}
    if endpoint_url and ("localhost" in endpoint_url or "host.docker.internal" in endpoint_url):
        kwargs["endpoint_url"] = endpoint_url
        kwargs["aws_access_key_id"] = "test"
        kwargs["aws_secret_access_key"] = "test"
    return boto3.client("sqs", **kwargs)


def get_sfn_client():
    """Get Step Functions client (LocalStack or AWS)."""
    endpoint_url = _get_endpoint_url()
    kwargs = {"region_name": AWS_REGION}
    if endpoint_url and ("localhost" in endpoint_url or "host.docker.internal" in endpoint_url):
        kwargs["endpoint_url"] = endpoint_url
        kwargs["aws_access_key_id"] = "test"
        kwargs["aws_secret_access_key"] = "test"
    return boto3.client("stepfunctions", **kwargs)


# ============================================================================
# EXTRACT LAMBDA - Uses Bulk API Endpoint
# ============================================================================

async def _discover_record_types(
    session: aiohttp.ClientSession,
    auth: aiohttp.BasicAuth,
) -> dict[str, str]:
    """
    Dynamically discover available record types from the API.
    
    Uses the /data/all/{year} endpoint with a sample year to discover
    all available record types.
    """
    sample_year = 2020
    url = f"{GFN_API_BASE_URL}/data/all/{sample_year}"

    try:
        async with session.get(url, auth=auth) as resp:
            if resp.status != 200:
                logger.warning(f"Could not discover record types: status {resp.status}")
                return {}

            data = await resp.json()
            if not isinstance(data, list):
                return {}

            # Extract unique record types
            return {r["record"]: r["record"] for r in data if r.get("record")}
    except Exception as e:
        logger.warning(f"Error discovering record types: {e}")
        return {}


async def _fetch_year_bulk(
    session: aiohttp.ClientSession,
    auth: aiohttp.BasicAuth,
    year: int,
    semaphore: asyncio.Semaphore,
) -> list[dict]:
    """
    Fetch ALL data for ALL countries for a single year using bulk endpoint.
    
    This is ~200x more efficient than per-country fetching.
    """
    async with semaphore:
        await asyncio.sleep(0.5)  # Rate limiting

        url = f"{GFN_API_BASE_URL}/data/all/{year}"

        for attempt in range(3):
            try:
                async with session.get(url, auth=auth) as resp:
                    if resp.status == 429:
                        retry_after = int(resp.headers.get("Retry-After", 5))
                        logger.warning(f"Rate limited, waiting {retry_after}s...")
                        await asyncio.sleep(retry_after)
                        continue

                    if resp.status != 200:
                        logger.warning(f"Year {year} returned status {resp.status}")
                        return []

                    data = await resp.json()
                    records = data if isinstance(data, list) else [data]

                    extracted_at = datetime.now(timezone.utc).isoformat()

                    return [
                        {
                            "country_code": r.get("countryCode"),
                            "country_name": r.get("countryName"),
                            "short_name": r.get("shortName"),
                            "iso_alpha2": r.get("isoa2"),
                            "year": r.get("year"),
                            "record_type": r.get("record"),
                            # Land use breakdown
                            "crop_land": r.get("cropLand"),
                            "grazing_land": r.get("grazingLand"),
                            "forest_land": r.get("forestLand"),
                            "fishing_ground": r.get("fishingGround"),
                            "builtup_land": r.get("builtupLand"),
                            "carbon": r.get("carbon"),
                            # Aggregate value
                            "value": r.get("value"),
                            "score": r.get("score"),
                            "extracted_at": extracted_at,
                        }
                        for r in records
                        if r.get("year") and r.get("countryCode")
                    ]

            except asyncio.TimeoutError:
                logger.warning(f"Timeout for year {year}, attempt {attempt + 1}/3")
                if attempt < 2:
                    await asyncio.sleep(2 ** attempt)
                    continue
                return []
            except aiohttp.ClientError as e:
                logger.warning(f"Error for year {year}: {e}")
                if attempt < 2:
                    await asyncio.sleep(2 ** attempt)
                    continue
                return []

        return []


async def _extract_bulk(start_year: int, end_year: int) -> dict:
    """
    Extract all data from GFN API using bulk endpoint.
    
    Uses /data/all/{year} which returns ALL countries × ALL record types
    for a year in a single API call.
    """
    if not GFN_API_KEY:
        raise ValueError("GFN_API_KEY environment variable required")

    auth = aiohttp.BasicAuth("", GFN_API_KEY)
    connector = aiohttp.TCPConnector(limit=10)
    timeout = aiohttp.ClientTimeout(total=60)

    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        # Step 1: Discover available record types
        logger.info("Discovering record types from API...")
        record_types = await _discover_record_types(session, auth)
        logger.info(f"Found {len(record_types)} record types")

        # Step 2: Fetch countries for reference
        logger.info("Fetching countries...")
        async with session.get(f"{GFN_API_BASE_URL}/countries", auth=auth) as resp:
            countries_data = await resp.json()

        countries = [
            {
                "country_code": c.get("countryCode"),
                "country_name": c.get("countryName"),
                "short_name": c.get("shortName"),
                "iso_alpha2": c.get("isoa2"),
                "score": c.get("score"),
            }
            for c in countries_data
            if c.get("countryCode") is not None
        ]
        logger.info(f"Found {len(countries)} countries")

        # Step 3: Fetch all years in parallel batches
        years = list(range(start_year, end_year + 1))
        logger.info(f"Fetching {len(years)} years ({start_year}-{end_year})...")

        semaphore = asyncio.Semaphore(3)  # 3 concurrent requests
        tasks = [_fetch_year_bulk(session, auth, year, semaphore) for year in years]
        results = await asyncio.gather(*tasks)

        all_records = []
        for year, records in zip(years, results):
            all_records.extend(records)
            logger.info(f"  Year {year}: {len(records):,} records")

        return {
            "countries": countries,
            "footprint_data": all_records,
            "record_types": list(record_types.keys()),
            "metadata": {
                "start_year": start_year,
                "end_year": end_year,
                "total_records": len(all_records),
                "unique_countries": len(set(r["country_code"] for r in all_records)),
                "unique_record_types": len(set(r["record_type"] for r in all_records if r.get("record_type"))),
            }
        }


def handler_extract(event: dict, context: Any = None) -> dict:
    """
    Lambda handler for extraction.
    
    Uses the efficient bulk API endpoint /data/all/{year} which returns
    ALL countries × ALL record types in a single call.
    
    Input event:
        {
            "start_year": 2010,
            "end_year": 2024
        }
    
    Output (Step Functions compatible):
        {
            "status": "success",
            "records_count": 175000,
            "s3_key": "raw/footprint/2024/01/30/data.json",
            "s3_bucket": "gfn-data-lake",
            "metadata": {...}
        }
    """
    logger.info(f"Extract Lambda triggered: {json.dumps(event)}")

    start_year = event.get("start_year", 2010)
    end_year = event.get("end_year", 2024)

    # Extract data using bulk endpoint
    start_time = time.time()
    result = asyncio.run(_extract_bulk(start_year, end_year))
    extract_time = time.time() - start_time

    records = result["footprint_data"]
    logger.info(f"Extracted {len(records):,} records in {extract_time:.1f}s")

    if not records:
        return {
            "status": "no_data",
            "records_count": 0,
            "s3_key": None,
            "s3_bucket": S3_BUCKET,
        }

    # Save to S3
    s3 = get_s3_client()
    timestamp = datetime.now(timezone.utc)
    s3_key = f"raw/footprint/{timestamp.strftime('%Y/%m/%d')}/data_{timestamp.strftime('%H%M%S')}.json"

    s3.put_object(
        Bucket=S3_BUCKET,
        Key=s3_key,
        Body=json.dumps(result).encode(),
        ContentType="application/json",
        Metadata={
            "records_count": str(len(records)),
            "start_year": str(start_year),
            "end_year": str(end_year),
            "extract_time_seconds": str(round(extract_time, 2)),
            "record_types": str(len(result["record_types"])),
        },
    )

    logger.info(f"Saved to s3://{S3_BUCKET}/{s3_key}")

    # Send message to transform queue (for SQS-triggered workflow)
    sqs = get_sqs_client()
    try:
        queue_url = sqs.get_queue_url(QueueName="gfn-transform-queue")["QueueUrl"]
        sqs.send_message(
            QueueUrl=queue_url,
            MessageBody=json.dumps({
                "s3_bucket": S3_BUCKET,
                "s3_key": s3_key,
                "records_count": len(records),
            }),
        )
        logger.info("Sent message to transform queue")
    except Exception as e:
        logger.warning(f"Could not send to transform queue: {e}")

    # Return Step Functions compatible output
    return {
        "status": "success",
        "records_count": len(records),
        "s3_key": s3_key,
        "s3_bucket": S3_BUCKET,
        "extract_time_seconds": round(extract_time, 2),
        "metadata": result["metadata"],
    }


# ============================================================================
# TRANSFORM LAMBDA
# ============================================================================

def handler_transform(event: dict, context: Any = None) -> dict:
    """
    Lambda handler for transformation.
    
    Validates, enriches, and deduplicates extracted data.
    
    Input event (Step Functions or SQS):
        {
            "s3_bucket": "gfn-data-lake",
            "s3_key": "raw/footprint/2024/01/30/data.json"
        }
    
    Output (Step Functions compatible):
        {
            "status": "success",
            "records_count": 175000,
            "s3_key": "processed/footprint/2024/01/30/data.json",
            "s3_bucket": "gfn-data-lake"
        }
    """
    logger.info(f"Transform Lambda triggered: {json.dumps(event)}")

    # Handle SQS event wrapper
    if "Records" in event:
        body = json.loads(event["Records"][0]["body"])
        s3_bucket = body.get("s3_bucket", S3_BUCKET)
        s3_key = body["s3_key"]
    else:
        s3_bucket = event.get("s3_bucket", S3_BUCKET)
        s3_key = event["s3_key"]

    # Read raw data from S3
    s3 = get_s3_client()
    response = s3.get_object(Bucket=s3_bucket, Key=s3_key)
    raw_data = json.loads(response["Body"].read().decode())

    # Handle both old format (list) and new format (dict with keys)
    if isinstance(raw_data, dict):
        footprint_data = raw_data.get("footprint_data", [])
        countries = raw_data.get("countries", [])
        record_types = raw_data.get("record_types", [])
    else:
        footprint_data = raw_data
        countries = []
        record_types = []

    logger.info(f"Read {len(footprint_data):,} records from s3://{s3_bucket}/{s3_key}")

    # Transform: validate, enrich, deduplicate
    transformed = []
    seen = set()
    invalid_count = 0

    for record in footprint_data:
        # Validate required fields
        if not record.get("country_code") or not record.get("year"):
            invalid_count += 1
            continue

        # Deduplicate by (country_code, year, record_type)
        record_type = record.get("record_type", "unknown")
        key = (record["country_code"], record["year"], record_type)
        if key in seen:
            continue
        seen.add(key)

        # Enrich: add transformed timestamp and calculate derived fields
        enriched = {
            **record,
            "transformed_at": datetime.now(timezone.utc).isoformat(),
        }

        # Calculate carbon percentage for footprint types
        carbon = record.get("carbon")
        value = record.get("value")
        if carbon is not None and value and value > 0:
            enriched["carbon_pct_of_total"] = round(carbon / value * 100, 2)
        else:
            enriched["carbon_pct_of_total"] = None

        transformed.append(enriched)

    logger.info(f"Transformed {len(transformed):,} records "
                f"(removed {invalid_count} invalid, {len(footprint_data) - len(transformed) - invalid_count} duplicates)")

    # Build output data structure
    output_data = {
        "footprint_data": transformed,
        "countries": countries,
        "record_types": record_types,
        "metadata": {
            "source_key": s3_key,
            "records_transformed": len(transformed),
            "records_removed": len(footprint_data) - len(transformed),
            "transformed_at": datetime.now(timezone.utc).isoformat(),
        }
    }

    # Save to processed folder
    output_key = s3_key.replace("raw/", "processed/").replace(".json", "_processed.json")

    s3.put_object(
        Bucket=s3_bucket,
        Key=output_key,
        Body=json.dumps(output_data).encode(),
        ContentType="application/json",
        Metadata={
            "records_count": str(len(transformed)),
            "source_key": s3_key,
        },
    )

    logger.info(f"Saved to s3://{s3_bucket}/{output_key}")

    # Send message to load queue (for SQS-triggered workflow)
    sqs = get_sqs_client()
    try:
        queue_url = sqs.get_queue_url(QueueName="gfn-load-queue")["QueueUrl"]
        sqs.send_message(
            QueueUrl=queue_url,
            MessageBody=json.dumps({
                "s3_bucket": s3_bucket,
                "s3_key": output_key,
                "records_count": len(transformed),
            }),
        )
        logger.info("Sent message to load queue")
    except Exception as e:
        logger.warning(f"Could not send to load queue: {e}")

    # Return Step Functions compatible output
    return {
        "status": "success",
        "records_count": len(transformed),
        "s3_key": output_key,
        "s3_bucket": s3_bucket,
    }


# ============================================================================
# LOAD LAMBDA
# ============================================================================

def handler_load(event: dict, context: Any = None) -> dict:
    """
    Lambda handler for loading to destination (Snowflake or DuckDB).
    
    Input event (Step Functions or SQS):
        {
            "s3_bucket": "gfn-data-lake",
            "s3_key": "processed/footprint/2024/01/30/data.json"
        }
    
    Output (Step Functions compatible):
        {
            "status": "success",
            "records_loaded": 175000,
            "destination": "snowflake",
            "s3_key": "processed/footprint/2024/01/30/data.json"
        }
    """
    logger.info(f"Load Lambda triggered: {json.dumps(event)}")

    # Handle SQS event wrapper
    if "Records" in event:
        body = json.loads(event["Records"][0]["body"])
        s3_bucket = body.get("s3_bucket", S3_BUCKET)
        s3_key = body["s3_key"]
    else:
        s3_bucket = event.get("s3_bucket", S3_BUCKET)
        s3_key = event["s3_key"]

    # Read processed data
    s3 = get_s3_client()
    response = s3.get_object(Bucket=s3_bucket, Key=s3_key)
    raw_data = json.loads(response["Body"].read().decode())

    # Handle both formats
    if isinstance(raw_data, dict):
        data = raw_data.get("footprint_data", [])
    else:
        data = raw_data

    logger.info(f"Read {len(data):,} records from s3://{s3_bucket}/{s3_key}")

    # Determine destination
    snowflake_account = os.getenv("SNOWFLAKE_ACCOUNT")
    is_lambda = os.getenv("AWS_LAMBDA_FUNCTION_NAME") is not None

    if snowflake_account:
        # Production: Load to Snowflake
        records_loaded = _load_to_snowflake_bulk(data)
        destination = "snowflake"
    elif is_lambda:
        # Lambda without Snowflake: data already in S3 for Snowpipe
        records_loaded = len(data)
        destination = "s3_for_snowpipe"
        logger.info(f"Data available in S3 for Snowpipe: {len(data):,} records")
    else:
        # Local: Load to DuckDB
        records_loaded = _load_to_duckdb_bulk(data)
        destination = "duckdb"

    logger.info(f"Loaded {records_loaded:,} records to {destination}")

    # Return Step Functions compatible output
    return {
        "status": "success",
        "records_loaded": records_loaded,
        "destination": destination,
        "s3_key": s3_key,
        "s3_bucket": s3_bucket,
    }


def _load_to_duckdb_bulk(data: list[dict]) -> int:
    """Load data to local DuckDB with new schema."""
    import duckdb

    db_path = os.getenv("DUCKDB_PATH", "gfn_lambda.duckdb")
    conn = duckdb.connect(db_path)

    # Create table with new comprehensive schema
    conn.execute("""
        CREATE TABLE IF NOT EXISTS footprint_data (
            country_code INTEGER,
            country_name VARCHAR,
            short_name VARCHAR,
            iso_alpha2 VARCHAR,
            year INTEGER,
            record_type VARCHAR,
            crop_land DOUBLE,
            grazing_land DOUBLE,
            forest_land DOUBLE,
            fishing_ground DOUBLE,
            builtup_land DOUBLE,
            carbon DOUBLE,
            value DOUBLE,
            score VARCHAR,
            carbon_pct_of_total DOUBLE,
            extracted_at TIMESTAMP,
            transformed_at TIMESTAMP,
            PRIMARY KEY (country_code, year, record_type)
        )
    """)

    if data:
        # Use batch insert for efficiency
        conn.executemany("""
            INSERT OR REPLACE INTO footprint_data VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
            )
        """, [
            (
                r.get("country_code"),
                r.get("country_name"),
                r.get("short_name"),
                r.get("iso_alpha2"),
                r.get("year"),
                r.get("record_type"),
                r.get("crop_land"),
                r.get("grazing_land"),
                r.get("forest_land"),
                r.get("fishing_ground"),
                r.get("builtup_land"),
                r.get("carbon"),
                r.get("value"),
                r.get("score"),
                r.get("carbon_pct_of_total"),
                r.get("extracted_at"),
                r.get("transformed_at"),
            )
            for r in data
        ])

    count = conn.execute("SELECT COUNT(*) FROM footprint_data").fetchone()[0]
    conn.close()

    return len(data)


def _load_to_snowflake_bulk(data: list[dict]) -> int:
    """Load data to Snowflake using batch operations."""
    try:
        import snowflake.connector
    except ImportError:
        logger.error("snowflake-connector-python not installed")
        return 0

    conn = snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE", "GFN"),
        schema=os.getenv("SNOWFLAKE_SCHEMA", "RAW"),
    )

    cursor = conn.cursor()

    try:
        # Create table with comprehensive schema
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS FOOTPRINT_DATA_RAW (
                country_code INTEGER,
                country_name VARCHAR,
                short_name VARCHAR,
                iso_alpha2 VARCHAR,
                year INTEGER,
                record_type VARCHAR,
                crop_land DOUBLE,
                grazing_land DOUBLE,
                forest_land DOUBLE,
                fishing_ground DOUBLE,
                builtup_land DOUBLE,
                carbon DOUBLE,
                value DOUBLE,
                score VARCHAR,
                carbon_pct_of_total DOUBLE,
                extracted_at TIMESTAMP_TZ,
                transformed_at TIMESTAMP_TZ,
                loaded_at TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),
                PRIMARY KEY (country_code, year, record_type)
            )
        """)

        # Batch insert using executemany
        cursor.executemany("""
            MERGE INTO FOOTPRINT_DATA_RAW t
            USING (SELECT %s as country_code, %s as year, %s as record_type) s
            ON t.country_code = s.country_code AND t.year = s.year AND t.record_type = s.record_type
            WHEN MATCHED THEN UPDATE SET
                country_name = %s, short_name = %s, iso_alpha2 = %s,
                crop_land = %s, grazing_land = %s, forest_land = %s,
                fishing_ground = %s, builtup_land = %s, carbon = %s,
                value = %s, score = %s, carbon_pct_of_total = %s,
                extracted_at = %s, transformed_at = %s, loaded_at = CURRENT_TIMESTAMP()
            WHEN NOT MATCHED THEN INSERT (
                country_code, country_name, short_name, iso_alpha2, year, record_type,
                crop_land, grazing_land, forest_land, fishing_ground, builtup_land, carbon,
                value, score, carbon_pct_of_total, extracted_at, transformed_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, [
            (
                r.get("country_code"), r.get("year"), r.get("record_type"),
                r.get("country_name"), r.get("short_name"), r.get("iso_alpha2"),
                r.get("crop_land"), r.get("grazing_land"), r.get("forest_land"),
                r.get("fishing_ground"), r.get("builtup_land"), r.get("carbon"),
                r.get("value"), r.get("score"), r.get("carbon_pct_of_total"),
                r.get("extracted_at"), r.get("transformed_at"),
                # Insert values
                r.get("country_code"), r.get("country_name"), r.get("short_name"),
                r.get("iso_alpha2"), r.get("year"), r.get("record_type"),
                r.get("crop_land"), r.get("grazing_land"), r.get("forest_land"),
                r.get("fishing_ground"), r.get("builtup_land"), r.get("carbon"),
                r.get("value"), r.get("score"), r.get("carbon_pct_of_total"),
                r.get("extracted_at"), r.get("transformed_at"),
            )
            for r in data
        ])

        conn.commit()
        return len(data)

    except Exception as e:
        logger.error(f"Snowflake load error: {e}")
        conn.rollback()
        return 0
    finally:
        cursor.close()
        conn.close()


# ============================================================================
# CLI for local testing
# ============================================================================

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Test Lambda handlers locally")
    parser.add_argument("handler", choices=["extract", "transform", "load"])
    parser.add_argument("--start-year", type=int, default=2020)
    parser.add_argument("--end-year", type=int, default=2024)
    parser.add_argument("--s3-key", help="S3 key for transform/load")
    args = parser.parse_args()

    if args.handler == "extract":
        result = handler_extract({
            "start_year": args.start_year,
            "end_year": args.end_year,
        })
    elif args.handler == "transform":
        if not args.s3_key:
            print("Error: --s3-key required for transform")
            exit(1)
        result = handler_transform({"s3_key": args.s3_key})
    elif args.handler == "load":
        if not args.s3_key:
            print("Error: --s3-key required for load")
            exit(1)
        result = handler_load({"s3_key": args.s3_key})

    print(f"\nResult: {json.dumps(result, indent=2)}")
