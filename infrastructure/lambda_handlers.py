"""
Lambda Functions for GFN Pipeline.

These handlers are designed to run as AWS Lambda functions.
Each function is decoupled and communicates via S3/SQS.

Local testing:
    python -m infrastructure.lambda_handlers extract
    python -m infrastructure.lambda_handlers transform --s3-key raw/carbon_footprint/2024/data.json
    python -m infrastructure.lambda_handlers load --s3-key processed/carbon_footprint/2024/data.parquet
"""
from __future__ import annotations

import asyncio
import json
import os
import time
from datetime import datetime, timezone
from typing import Any

import aiohttp
import boto3
from botocore.config import Config

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


# ============================================================================
# EXTRACT LAMBDA
# ============================================================================

async def _fetch_country_data(
    session: aiohttp.ClientSession,
    auth: aiohttp.BasicAuth,
    country_code: str,
    start_year: int,
    end_year: int,
) -> list[dict]:
    """Fetch all years for a country."""
    url = f"{GFN_API_BASE_URL}/data/{country_code}/all/EFCtot"
    
    try:
        async with session.get(url, auth=auth) as resp:
            if resp.status != 200:
                return []
            
            data = await resp.json()
            records = data if isinstance(data, list) else [data]
            
            return [
                {
                    "country_code": r.get("countryCode"),
                    "country_name": r.get("countryName"),
                    "iso_alpha2": r.get("isoa2"),
                    "year": r.get("year"),
                    "carbon_footprint_gha": r.get("carbon"),
                    "total_footprint_gha": r.get("value"),
                    "score": r.get("score"),
                    "extracted_at": datetime.now(timezone.utc).isoformat(),
                }
                for r in records
                if r.get("year") and start_year <= r["year"] <= end_year
            ]
    except Exception:
        return []


async def _extract_all(start_year: int, end_year: int) -> list[dict]:
    """Extract all data from GFN API."""
    if not GFN_API_KEY:
        raise ValueError("GFN_API_KEY environment variable required")
    
    auth = aiohttp.BasicAuth("user", GFN_API_KEY)
    connector = aiohttp.TCPConnector(limit=10)
    timeout = aiohttp.ClientTimeout(total=30)
    
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        # Get countries
        async with session.get(f"{GFN_API_BASE_URL}/countries", auth=auth) as resp:
            countries = await resp.json()
        
        # Fetch all countries (with rate limiting)
        all_records = []
        semaphore = asyncio.Semaphore(5)
        
        async def fetch_with_limit(country):
            async with semaphore:
                await asyncio.sleep(0.2)  # Rate limit
                code = country.get("countryCode")
                if code and str(code).isdigit():
                    return await _fetch_country_data(session, auth, code, start_year, end_year)
                return []
        
        tasks = [fetch_with_limit(c) for c in countries]
        results = await asyncio.gather(*tasks)
        
        for records in results:
            all_records.extend(records)
        
        return all_records


def handler_extract(event: dict, context: Any = None) -> dict:
    """
    Lambda handler for extraction.
    
    Input event:
        {
            "start_year": 2010,
            "end_year": 2024,
            "incremental": true
        }
    
    Output:
        {
            "status": "success",
            "records_count": 2800,
            "s3_key": "raw/carbon_footprint/2024/01/30/data.json"
        }
    """
    print(f"Extract Lambda triggered: {json.dumps(event)}")
    
    start_year = event.get("start_year", 2010)
    end_year = event.get("end_year", 2024)
    
    # Extract data
    start_time = time.time()
    records = asyncio.run(_extract_all(start_year, end_year))
    extract_time = time.time() - start_time
    
    print(f"Extracted {len(records)} records in {extract_time:.1f}s")
    
    if not records:
        return {
            "status": "no_data",
            "records_count": 0,
            "s3_key": None,
        }
    
    # Save to S3
    s3 = get_s3_client()
    timestamp = datetime.now(timezone.utc)
    s3_key = f"raw/carbon_footprint/{timestamp.strftime('%Y/%m/%d')}/data_{timestamp.strftime('%H%M%S')}.json"
    
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=s3_key,
        Body=json.dumps(records).encode(),
        ContentType="application/json",
        Metadata={
            "records_count": str(len(records)),
            "start_year": str(start_year),
            "end_year": str(end_year),
            "extract_time_seconds": str(round(extract_time, 2)),
        },
    )
    
    print(f"Saved to s3://{S3_BUCKET}/{s3_key}")
    
    # Send message to transform queue
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
        print("Sent message to transform queue")
    except Exception as e:
        print(f"Warning: Could not send to transform queue: {e}")
    
    return {
        "status": "success",
        "records_count": len(records),
        "s3_key": s3_key,
        "extract_time_seconds": round(extract_time, 2),
    }


# ============================================================================
# TRANSFORM LAMBDA
# ============================================================================

def handler_transform(event: dict, context: Any = None) -> dict:
    """
    Lambda handler for transformation.
    
    Triggered by S3 event or SQS message.
    
    Input event (SQS):
        {
            "s3_bucket": "gfn-data-lake",
            "s3_key": "raw/carbon_footprint/2024/01/30/data.json"
        }
    
    Output:
        {
            "status": "success",
            "records_count": 2800,
            "s3_key": "processed/carbon_footprint/2024/01/30/data.parquet"
        }
    """
    print(f"Transform Lambda triggered: {json.dumps(event)}")
    
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
    
    print(f"Read {len(raw_data)} records from s3://{s3_bucket}/{s3_key}")
    
    # Transform: validate, enrich, deduplicate
    transformed = []
    seen = set()
    
    for record in raw_data:
        # Validate required fields
        if not record.get("country_code") or not record.get("year"):
            continue
        
        # Deduplicate
        key = (record["country_code"], record["year"])
        if key in seen:
            continue
        seen.add(key)
        
        # Enrich: calculate carbon percentage
        carbon = record.get("carbon_footprint_gha")
        total = record.get("total_footprint_gha")
        
        transformed.append({
            **record,
            "carbon_pct_of_total": (
                round(carbon / total * 100, 2)
                if carbon and total else None
            ),
            "transformed_at": datetime.now(timezone.utc).isoformat(),
        })
    
    print(f"Transformed {len(transformed)} records (removed {len(raw_data) - len(transformed)} invalid/duplicates)")
    
    # Save as Parquet to processed folder
    # For Lambda, we'll save as JSON (Parquet requires pandas/pyarrow)
    output_key = s3_key.replace("raw/", "processed/").replace(".json", "_processed.json")
    
    s3.put_object(
        Bucket=s3_bucket,
        Key=output_key,
        Body=json.dumps(transformed).encode(),
        ContentType="application/json",
        Metadata={
            "records_count": str(len(transformed)),
            "source_key": s3_key,
        },
    )
    
    print(f"Saved to s3://{s3_bucket}/{output_key}")
    
    # Send message to load queue
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
        print("Sent message to load queue")
    except Exception as e:
        print(f"Warning: Could not send to load queue: {e}")
    
    return {
        "status": "success",
        "records_count": len(transformed),
        "s3_key": output_key,
    }


# ============================================================================
# LOAD LAMBDA
# ============================================================================

def handler_load(event: dict, context: Any = None) -> dict:
    """
    Lambda handler for loading to Snowflake.
    
    Input event:
        {
            "s3_bucket": "gfn-data-lake",
            "s3_key": "processed/carbon_footprint/2024/01/30/data.parquet"
        }
    
    Output:
        {
            "status": "success",
            "records_loaded": 2800,
            "destination": "snowflake"
        }
    """
    print(f"Load Lambda triggered: {json.dumps(event)}")
    
    # Handle SQS event wrapper
    if "Records" in event:
        body = json.loads(event["Records"][0]["body"])
        s3_bucket = body.get("s3_bucket", S3_BUCKET)
        s3_key = body["s3_key"]
        records_count = body.get("records_count", 0)
    else:
        s3_bucket = event.get("s3_bucket", S3_BUCKET)
        s3_key = event["s3_key"]
        records_count = event.get("records_count", 0)
    
    # Read processed data
    s3 = get_s3_client()
    response = s3.get_object(Bucket=s3_bucket, Key=s3_key)
    data = json.loads(response["Body"].read().decode())
    
    print(f"Read {len(data)} records from s3://{s3_bucket}/{s3_key}")
    
    # Load to Snowflake (or DuckDB for local testing)
    snowflake_account = os.getenv("SNOWFLAKE_ACCOUNT")
    is_lambda = os.getenv("AWS_LAMBDA_FUNCTION_NAME") is not None
    
    if snowflake_account:
        # Production: Load to Snowflake
        records_loaded = _load_to_snowflake(data)
        destination = "snowflake"
    elif is_lambda:
        # Lambda without Snowflake: simulate load (data already in S3 for Snowpipe)
        records_loaded = len(data)
        destination = "s3_for_snowpipe"
        print(f"Data available in S3 for Snowpipe ingestion: {len(data)} records")
    else:
        # Local: Load to DuckDB
        records_loaded = _load_to_duckdb(data)
        destination = "duckdb"
    
    print(f"Loaded {records_loaded} records to {destination}")
    
    return {
        "status": "success",
        "records_loaded": records_loaded,
        "destination": destination,
        "s3_key": s3_key,
    }


def _load_to_duckdb(data: list[dict]) -> int:
    """Load data to local DuckDB."""
    import duckdb
    
    db_path = os.getenv("DUCKDB_PATH", "gfn_lambda.duckdb")
    conn = duckdb.connect(db_path)
    
    conn.execute("""
        CREATE TABLE IF NOT EXISTS carbon_footprint (
            country_code INTEGER,
            country_name VARCHAR,
            iso_alpha2 VARCHAR,
            year INTEGER,
            carbon_footprint_gha DOUBLE,
            total_footprint_gha DOUBLE,
            carbon_pct_of_total DOUBLE,
            score VARCHAR,
            extracted_at TIMESTAMP,
            transformed_at TIMESTAMP,
            PRIMARY KEY (country_code, year)
        )
    """)
    
    if data:
        conn.execute("""
            INSERT OR REPLACE INTO carbon_footprint 
            SELECT * FROM (
                SELECT 
                    UNNEST($1) as country_code,
                    UNNEST($2) as country_name,
                    UNNEST($3) as iso_alpha2,
                    UNNEST($4) as year,
                    UNNEST($5) as carbon_footprint_gha,
                    UNNEST($6) as total_footprint_gha,
                    UNNEST($7) as carbon_pct_of_total,
                    UNNEST($8) as score,
                    UNNEST($9) as extracted_at,
                    UNNEST($10) as transformed_at
            )
        """, [
            [r.get("country_code") for r in data],
            [r.get("country_name") for r in data],
            [r.get("iso_alpha2") for r in data],
            [r.get("year") for r in data],
            [r.get("carbon_footprint_gha") for r in data],
            [r.get("total_footprint_gha") for r in data],
            [r.get("carbon_pct_of_total") for r in data],
            [r.get("score") for r in data],
            [r.get("extracted_at") for r in data],
            [r.get("transformed_at") for r in data],
        ])
    
    count = conn.execute("SELECT COUNT(*) FROM carbon_footprint").fetchone()[0]
    conn.close()
    
    return len(data)


def _load_to_snowflake(data: list[dict]) -> int:
    """Load data to Snowflake using COPY INTO."""
    import snowflake.connector
    
    conn = snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE", "GFN"),
        schema=os.getenv("SNOWFLAKE_SCHEMA", "RAW"),
    )
    
    cursor = conn.cursor()
    
    # Create table if not exists
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS CARBON_FOOTPRINT_RAW (
            country_code INTEGER,
            country_name VARCHAR,
            iso_alpha2 VARCHAR,
            year INTEGER,
            carbon_footprint_gha DOUBLE,
            total_footprint_gha DOUBLE,
            carbon_pct_of_total DOUBLE,
            score VARCHAR,
            extracted_at TIMESTAMP_TZ,
            transformed_at TIMESTAMP_TZ,
            loaded_at TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
        )
    """)
    
    # Insert data
    for record in data:
        cursor.execute("""
            MERGE INTO CARBON_FOOTPRINT_RAW t
            USING (SELECT %s as country_code, %s as year) s
            ON t.country_code = s.country_code AND t.year = s.year
            WHEN MATCHED THEN UPDATE SET
                country_name = %s,
                iso_alpha2 = %s,
                carbon_footprint_gha = %s,
                total_footprint_gha = %s,
                carbon_pct_of_total = %s,
                score = %s,
                extracted_at = %s,
                transformed_at = %s,
                loaded_at = CURRENT_TIMESTAMP()
            WHEN NOT MATCHED THEN INSERT (
                country_code, country_name, iso_alpha2, year,
                carbon_footprint_gha, total_footprint_gha, carbon_pct_of_total,
                score, extracted_at, transformed_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            record.get("country_code"), record.get("year"),
            record.get("country_name"), record.get("iso_alpha2"),
            record.get("carbon_footprint_gha"), record.get("total_footprint_gha"),
            record.get("carbon_pct_of_total"), record.get("score"),
            record.get("extracted_at"), record.get("transformed_at"),
            record.get("country_code"), record.get("country_name"),
            record.get("iso_alpha2"), record.get("year"),
            record.get("carbon_footprint_gha"), record.get("total_footprint_gha"),
            record.get("carbon_pct_of_total"), record.get("score"),
            record.get("extracted_at"), record.get("transformed_at"),
        ))
    
    conn.commit()
    cursor.close()
    conn.close()
    
    return len(data)


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
