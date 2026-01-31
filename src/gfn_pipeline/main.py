"""
GFN Pipeline - Main Entry Point.

Unified entry point for running the pipeline with different destinations:
- DuckDB (local testing)
- Snowflake (production)
- Both (parallel loading)

Usage:
    python -m gfn_pipeline.main --destination duckdb
    python -m gfn_pipeline.main --destination snowflake
    python -m gfn_pipeline.main --destination both
    
Environment Variables:
    GFN_API_KEY: API key for Global Footprint Network
    PIPELINE_DESTINATION: Default destination (duckdb, snowflake, both)
    DUCKDB_PATH: Path to DuckDB database file
    SNOWFLAKE_*: Snowflake connection parameters
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(
            Path(os.getenv("LOG_DIR", "logs")) / f"pipeline_{datetime.now().strftime('%Y%m%d')}.log",
            mode="a",
        ) if os.path.exists(os.getenv("LOG_DIR", "logs")) else logging.NullHandler(),
    ],
)
logger = logging.getLogger("gfn_pipeline")


# =============================================================================
# Pipeline Runner
# =============================================================================

class PipelineRunner:
    """Main pipeline orchestrator supporting multiple destinations."""
    
    def __init__(
        self,
        destination: str = "duckdb",
        start_year: int = 2010,
        end_year: int = 2024,
        use_localstack: bool = True,
    ):
        self.destination = destination
        self.start_year = start_year
        self.end_year = end_year
        self.use_localstack = use_localstack
        self.metrics = {
            "start_time": None,
            "end_time": None,
            "records_extracted": 0,
            "records_transformed": 0,
            "records_loaded": 0,
            "errors": [],
        }
    
    def run(self) -> dict:
        """Execute the full pipeline."""
        self.metrics["start_time"] = datetime.now(timezone.utc).isoformat()
        logger.info(f"Starting pipeline: destination={self.destination}, years={self.start_year}-{self.end_year}")
        
        try:
            # Step 1: Extract
            raw_data = self._extract()
            self.metrics["records_extracted"] = len(raw_data)
            logger.info(f"Extracted {len(raw_data)} records")
            
            if not raw_data:
                logger.warning("No data extracted, skipping transform and load")
                return self._finalize_metrics("no_data")
            
            # Step 2: Store raw data (S3 or local)
            raw_path = self._store_raw(raw_data)
            logger.info(f"Stored raw data: {raw_path}")
            
            # Step 3: Transform
            transformed_data = self._transform(raw_data)
            self.metrics["records_transformed"] = len(transformed_data)
            logger.info(f"Transformed {len(transformed_data)} records")
            
            # Step 4: Store processed data
            processed_path = self._store_processed(transformed_data)
            logger.info(f"Stored processed data: {processed_path}")
            
            # Step 5: Load to destination(s)
            load_results = self._load(transformed_data)
            self.metrics["records_loaded"] = load_results.get("total_loaded", 0)
            logger.info(f"Loaded {self.metrics['records_loaded']} records to {self.destination}")
            
            return self._finalize_metrics("success")
            
        except Exception as e:
            logger.exception(f"Pipeline failed: {e}")
            self.metrics["errors"].append(str(e))
            return self._finalize_metrics("failed")
    
    def _extract(self) -> list[dict]:
        """Extract data from GFN API."""
        from gfn_pipeline.pipeline_async import ExtractionConfig, extract_all_parallel
        
        api_key = os.getenv("GFN_API_KEY")
        if not api_key:
            raise ValueError("GFN_API_KEY environment variable required")
        
        config = ExtractionConfig(api_key=api_key)
        return asyncio.run(extract_all_parallel(config, self.start_year, self.end_year))
    
    def _store_raw(self, data: list[dict]) -> str:
        """Store raw data to S3 or local filesystem."""
        timestamp = datetime.now(timezone.utc)
        filename = f"carbon_footprint_{timestamp.strftime('%Y%m%d_%H%M%S')}.json"
        
        if self.use_localstack or os.getenv("AWS_ENDPOINT_URL"):
            # Store to S3 (LocalStack or real AWS)
            import boto3
            from botocore.config import Config
            
            s3_config = {
                "region_name": os.getenv("AWS_REGION", "us-east-1"),
                "config": Config(signature_version="s3v4"),
            }
            
            endpoint_url = os.getenv("AWS_ENDPOINT_URL")
            if endpoint_url:
                s3_config["endpoint_url"] = endpoint_url
                s3_config["aws_access_key_id"] = os.getenv("AWS_ACCESS_KEY_ID", "test")
                s3_config["aws_secret_access_key"] = os.getenv("AWS_SECRET_ACCESS_KEY", "test")
            
            s3 = boto3.client("s3", **s3_config)
            bucket = os.getenv("S3_BUCKET", "gfn-data-lake")
            key = f"raw/carbon_footprint/{timestamp.strftime('%Y/%m/%d')}/{filename}"
            
            s3.put_object(
                Bucket=bucket,
                Key=key,
                Body=json.dumps(data).encode(),
                ContentType="application/json",
            )
            return f"s3://{bucket}/{key}"
        else:
            # Store locally
            data_dir = Path(os.getenv("DATA_DIR", "data")) / "raw"
            data_dir.mkdir(parents=True, exist_ok=True)
            filepath = data_dir / filename
            
            with open(filepath, "w") as f:
                json.dump(data, f)
            return str(filepath)
    
    def _transform(self, data: list[dict]) -> list[dict]:
        """Transform and validate data."""
        transformed = []
        seen = set()
        
        for record in data:
            # Validate required fields
            if not record.get("country_code") or not record.get("year"):
                continue
            
            # Deduplicate
            key = (record["country_code"], record["year"])
            if key in seen:
                continue
            seen.add(key)
            
            # Enrich
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
        
        return transformed
    
    def _store_processed(self, data: list[dict]) -> str:
        """Store processed data."""
        timestamp = datetime.now(timezone.utc)
        filename = f"carbon_footprint_{timestamp.strftime('%Y%m%d_%H%M%S')}_processed.json"
        
        if self.use_localstack or os.getenv("AWS_ENDPOINT_URL"):
            import boto3
            from botocore.config import Config
            
            s3_config = {
                "region_name": os.getenv("AWS_REGION", "us-east-1"),
                "config": Config(signature_version="s3v4"),
            }
            
            endpoint_url = os.getenv("AWS_ENDPOINT_URL")
            if endpoint_url:
                s3_config["endpoint_url"] = endpoint_url
                s3_config["aws_access_key_id"] = os.getenv("AWS_ACCESS_KEY_ID", "test")
                s3_config["aws_secret_access_key"] = os.getenv("AWS_SECRET_ACCESS_KEY", "test")
            
            s3 = boto3.client("s3", **s3_config)
            bucket = os.getenv("S3_BUCKET", "gfn-data-lake")
            key = f"processed/carbon_footprint/{timestamp.strftime('%Y/%m/%d')}/{filename}"
            
            s3.put_object(
                Bucket=bucket,
                Key=key,
                Body=json.dumps(data).encode(),
                ContentType="application/json",
            )
            return f"s3://{bucket}/{key}"
        else:
            data_dir = Path(os.getenv("DATA_DIR", "data")) / "processed"
            data_dir.mkdir(parents=True, exist_ok=True)
            filepath = data_dir / filename
            
            with open(filepath, "w") as f:
                json.dump(data, f)
            return str(filepath)
    
    def _load(self, data: list[dict]) -> dict:
        """Load data to destination(s)."""
        results = {"destinations": {}, "total_loaded": 0}
        
        destinations = (
            ["duckdb", "snowflake"] if self.destination == "both"
            else [self.destination]
        )
        
        for dest in destinations:
            try:
                if dest == "duckdb":
                    count = self._load_to_duckdb(data)
                elif dest == "snowflake":
                    count = self._load_to_snowflake(data)
                else:
                    raise ValueError(f"Unknown destination: {dest}")
                
                results["destinations"][dest] = {"status": "success", "count": count}
                results["total_loaded"] += count
                
            except Exception as e:
                logger.error(f"Failed to load to {dest}: {e}")
                results["destinations"][dest] = {"status": "failed", "error": str(e)}
                self.metrics["errors"].append(f"{dest}: {e}")
        
        return results
    
    def _load_to_duckdb(self, data: list[dict]) -> int:
        """Load data to DuckDB."""
        import duckdb
        
        db_path = os.getenv("DUCKDB_PATH", "gfn.duckdb")
        logger.info(f"Loading to DuckDB: {db_path}")
        
        conn = duckdb.connect(db_path)
        
        # Create table if not exists
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
                loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (country_code, year)
            )
        """)
        
        # Insert/update data
        if data:
            conn.execute("""
                INSERT OR REPLACE INTO carbon_footprint 
                SELECT 
                    UNNEST($1) as country_code,
                    UNNEST($2) as country_name,
                    UNNEST($3) as iso_alpha2,
                    UNNEST($4) as year,
                    UNNEST($5) as carbon_footprint_gha,
                    UNNEST($6) as total_footprint_gha,
                    UNNEST($7) as carbon_pct_of_total,
                    UNNEST($8) as score,
                    UNNEST($9)::TIMESTAMP as extracted_at,
                    UNNEST($10)::TIMESTAMP as transformed_at,
                    CURRENT_TIMESTAMP as loaded_at
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
    
    def _load_to_snowflake(self, data: list[dict]) -> int:
        """Load data to Snowflake."""
        import snowflake.connector
        
        account = os.getenv("SNOWFLAKE_ACCOUNT")
        if not account:
            raise ValueError("SNOWFLAKE_ACCOUNT environment variable required")
        
        logger.info(f"Loading to Snowflake: {account}")
        
        conn = snowflake.connector.connect(
            account=account,
            user=os.getenv("SNOWFLAKE_USER"),
            password=os.getenv("SNOWFLAKE_PASSWORD"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
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
        
        # Use MERGE for upsert
        loaded = 0
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
            loaded += 1
        
        conn.commit()
        cursor.close()
        conn.close()
        
        return loaded
    
    def _finalize_metrics(self, status: str) -> dict:
        """Finalize and return metrics."""
        self.metrics["end_time"] = datetime.now(timezone.utc).isoformat()
        self.metrics["status"] = status
        self.metrics["duration_seconds"] = (
            datetime.fromisoformat(self.metrics["end_time"].replace("Z", "+00:00")) -
            datetime.fromisoformat(self.metrics["start_time"].replace("Z", "+00:00"))
        ).total_seconds()
        
        logger.info(f"Pipeline completed: status={status}, duration={self.metrics['duration_seconds']:.1f}s")
        return self.metrics


# =============================================================================
# CLI
# =============================================================================

def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="GFN Carbon Footprint Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run with DuckDB (local testing)
  python -m gfn_pipeline.main --destination duckdb
  
  # Run with Snowflake (production)
  python -m gfn_pipeline.main --destination snowflake
  
  # Run with both destinations
  python -m gfn_pipeline.main --destination both
  
  # Specify year range
  python -m gfn_pipeline.main --start-year 2020 --end-year 2024
        """,
    )
    parser.add_argument(
        "--destination", "-d",
        choices=["duckdb", "snowflake", "both"],
        default=os.getenv("PIPELINE_DESTINATION", "duckdb"),
        help="Target destination(s)",
    )
    parser.add_argument("--start-year", type=int, default=2010)
    parser.add_argument("--end-year", type=int, default=2024)
    parser.add_argument(
        "--no-localstack",
        action="store_true",
        help="Don't use LocalStack for S3 storage",
    )
    args = parser.parse_args()
    
    # Create logs directory if needed
    log_dir = Path(os.getenv("LOG_DIR", "logs"))
    log_dir.mkdir(parents=True, exist_ok=True)
    
    runner = PipelineRunner(
        destination=args.destination,
        start_year=args.start_year,
        end_year=args.end_year,
        use_localstack=not args.no_localstack,
    )
    
    result = runner.run()
    
    # Print summary
    print("\n" + "=" * 60)
    print("PIPELINE SUMMARY")
    print("=" * 60)
    print(json.dumps(result, indent=2))
    
    # Exit with appropriate code
    sys.exit(0 if result["status"] == "success" else 1)


if __name__ == "__main__":
    main()
