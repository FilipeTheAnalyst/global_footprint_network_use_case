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
        include_all_types: bool = True,
    ):
        self.destination = destination
        self.start_year = start_year
        self.end_year = end_year
        self.use_localstack = use_localstack
        self.include_all_types = include_all_types
        self.metrics = {
            "start_time": None,
            "end_time": None,
            "countries_extracted": 0,
            "records_extracted": 0,
            "records_transformed": 0,
            "records_loaded": 0,
            "record_types": [],
            "errors": [],
        }
    
    def run(self) -> dict:
        """Execute the full pipeline."""
        self.metrics["start_time"] = datetime.now(timezone.utc).isoformat()
        logger.info(f"Starting pipeline: destination={self.destination}, years={self.start_year}-{self.end_year}, all_types={self.include_all_types}")
        
        try:
            # Step 1: Extract
            extracted_data = self._extract()
            self.metrics["countries_extracted"] = len(extracted_data.get("countries", []))
            self.metrics["records_extracted"] = len(extracted_data.get("footprint_data", []))
            self.metrics["record_types"] = list(set(r["record_type"] for r in extracted_data.get("footprint_data", [])))
            logger.info(f"Extracted {self.metrics['countries_extracted']} countries, {self.metrics['records_extracted']} records")
            
            if not extracted_data.get("footprint_data"):
                logger.warning("No data extracted, skipping transform and load")
                return self._finalize_metrics("no_data")
            
            # Step 2: Store raw data (S3 or local)
            raw_path = self._store_raw(extracted_data)
            logger.info(f"Stored raw data: {raw_path}")
            
            # Step 3: Transform
            transformed_data = self._transform(extracted_data)
            self.metrics["records_transformed"] = (
                len(transformed_data.get("countries", [])) +
                len(transformed_data.get("footprint_data", []))
            )
            logger.info(f"Transformed {self.metrics['records_transformed']} total records")
            
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
    
    def _extract(self) -> dict[str, list[dict]]:
        """Extract data from GFN API."""
        from gfn_pipeline.pipeline_async import ExtractionConfig, extract_all_data, ALL_RECORD_TYPES
        
        api_key = os.getenv("GFN_API_KEY")
        if not api_key:
            raise ValueError("GFN_API_KEY environment variable required")
        
        config = ExtractionConfig(api_key=api_key)
        
        # Determine record types to extract
        if self.include_all_types:
            record_types = list(ALL_RECORD_TYPES.keys())
        else:
            record_types = ["EFCtot"]
        
        return asyncio.run(extract_all_data(config, self.start_year, self.end_year, record_types))
    
    def _store_raw(self, data: dict[str, list[dict]]) -> str:
        """Store raw data to S3 or local filesystem."""
        timestamp = datetime.now(timezone.utc)
        filename = f"gfn_data_{timestamp.strftime('%Y%m%d_%H%M%S')}.json"
        
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
            key = f"raw/gfn/{timestamp.strftime('%Y/%m/%d')}/{filename}"
            
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
    
    def _transform(self, data: dict[str, list[dict]]) -> dict[str, list[dict]]:
        """Transform and validate data."""
        transformed_at = datetime.now(timezone.utc).isoformat()
        
        # Transform countries (minimal transformation)
        countries = []
        seen_countries = set()
        for c in data.get("countries", []):
            if not c.get("country_code"):
                continue
            if c["country_code"] in seen_countries:
                continue
            seen_countries.add(c["country_code"])
            countries.append({
                **c,
                "transformed_at": transformed_at,
            })
        
        # Transform footprint data
        footprint_data = []
        seen_records = set()
        for r in data.get("footprint_data", []):
            # Validate required fields
            if not r.get("country_code") or not r.get("year") or not r.get("record_type"):
                continue
            
            # Deduplicate
            key = (r["country_code"], r["year"], r["record_type"])
            if key in seen_records:
                continue
            seen_records.add(key)
            
            footprint_data.append({
                **r,
                "transformed_at": transformed_at,
            })
        
        return {
            "countries": countries,
            "footprint_data": footprint_data,
        }
    
    def _store_processed(self, data: dict[str, list[dict]]) -> str:
        """Store processed data."""
        timestamp = datetime.now(timezone.utc)
        filename = f"gfn_data_{timestamp.strftime('%Y%m%d_%H%M%S')}_processed.json"
        
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
            key = f"processed/gfn/{timestamp.strftime('%Y/%m/%d')}/{filename}"
            
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
    
    def _load(self, data: dict[str, list[dict]]) -> dict:
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
    
    def _load_to_duckdb(self, data: dict[str, list[dict]]) -> int:
        """Load data to DuckDB."""
        import duckdb
        
        db_path = os.getenv("DUCKDB_PATH", "gfn.duckdb")
        logger.info(f"Loading to DuckDB: {db_path}")
        
        conn = duckdb.connect(db_path)
        total_loaded = 0
        
        # Create and load countries table
        conn.execute("""
            CREATE TABLE IF NOT EXISTS countries (
                country_code INTEGER PRIMARY KEY,
                country_name VARCHAR NOT NULL,
                iso_alpha2 VARCHAR,
                version VARCHAR,
                extracted_at TIMESTAMP,
                transformed_at TIMESTAMP,
                loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        countries = data.get("countries", [])
        if countries:
            conn.execute("DELETE FROM countries")
            conn.executemany("""
                INSERT INTO countries (country_code, country_name, iso_alpha2, version, extracted_at, transformed_at)
                VALUES (?, ?, ?, ?, ?, ?)
            """, [
                (c["country_code"], c["country_name"], c.get("iso_alpha2"), c.get("version"),
                 c.get("extracted_at"), c.get("transformed_at"))
                for c in countries
            ])
            total_loaded += len(countries)
        
        # Create and load footprint_data table
        conn.execute("""
            CREATE TABLE IF NOT EXISTS footprint_data (
                country_code INTEGER NOT NULL,
                country_name VARCHAR NOT NULL,
                iso_alpha2 VARCHAR,
                year INTEGER NOT NULL,
                record_type VARCHAR NOT NULL,
                record_type_description VARCHAR,
                value DOUBLE,
                carbon DOUBLE,
                score VARCHAR,
                extracted_at TIMESTAMP,
                transformed_at TIMESTAMP,
                loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (country_code, year, record_type)
            )
        """)
        
        footprint_data = data.get("footprint_data", [])
        if footprint_data:
            # Use INSERT OR REPLACE for upsert
            conn.executemany("""
                INSERT OR REPLACE INTO footprint_data 
                (country_code, country_name, iso_alpha2, year, record_type, record_type_description, value, carbon, score, extracted_at, transformed_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, [
                (r["country_code"], r["country_name"], r.get("iso_alpha2"), r["year"],
                 r["record_type"], r.get("record_type_description"), r.get("value"),
                 r.get("carbon"), r.get("score"), r.get("extracted_at"), r.get("transformed_at"))
                for r in footprint_data
            ])
            total_loaded += len(footprint_data)
        
        # Create useful views
        conn.execute("""
            CREATE OR REPLACE VIEW ecological_footprint AS
            SELECT * FROM footprint_data
            WHERE record_type LIKE 'EFC%' AND record_type NOT LIKE '%PerCap'
        """)
        
        conn.execute("""
            CREATE OR REPLACE VIEW biocapacity AS
            SELECT * FROM footprint_data
            WHERE record_type LIKE 'BioCap%' AND record_type NOT LIKE '%PerCap'
        """)
        
        conn.execute("""
            CREATE OR REPLACE VIEW per_capita_metrics AS
            SELECT * FROM footprint_data
            WHERE record_type LIKE '%PerCap'
        """)
        
        conn.execute("""
            CREATE OR REPLACE VIEW ecological_deficit AS
            SELECT * FROM footprint_data
            WHERE record_type LIKE 'EFCdef%'
        """)
        
        conn.close()
        return total_loaded
    
    def _load_to_snowflake(self, data: dict[str, list[dict]]) -> int:
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
        total_loaded = 0
        
        # Create and load countries table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS COUNTRIES (
                country_code INTEGER PRIMARY KEY,
                country_name VARCHAR NOT NULL,
                iso_alpha2 VARCHAR,
                version VARCHAR,
                extracted_at TIMESTAMP_TZ,
                transformed_at TIMESTAMP_TZ,
                loaded_at TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
            )
        """)
        
        countries = data.get("countries", [])
        if countries:
            cursor.execute("DELETE FROM COUNTRIES")
            for c in countries:
                cursor.execute("""
                    INSERT INTO COUNTRIES (country_code, country_name, iso_alpha2, version, extracted_at, transformed_at)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (c["country_code"], c["country_name"], c.get("iso_alpha2"), c.get("version"),
                      c.get("extracted_at"), c.get("transformed_at")))
            total_loaded += len(countries)
        
        # Create and load footprint_data table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS FOOTPRINT_DATA (
                country_code INTEGER NOT NULL,
                country_name VARCHAR NOT NULL,
                iso_alpha2 VARCHAR,
                year INTEGER NOT NULL,
                record_type VARCHAR NOT NULL,
                record_type_description VARCHAR,
                value DOUBLE,
                carbon DOUBLE,
                score VARCHAR,
                extracted_at TIMESTAMP_TZ,
                transformed_at TIMESTAMP_TZ,
                loaded_at TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),
                PRIMARY KEY (country_code, year, record_type)
            )
        """)
        
        footprint_data = data.get("footprint_data", [])
        for r in footprint_data:
            cursor.execute("""
                MERGE INTO FOOTPRINT_DATA t
                USING (SELECT %s as country_code, %s as year, %s as record_type) s
                ON t.country_code = s.country_code AND t.year = s.year AND t.record_type = s.record_type
                WHEN MATCHED THEN UPDATE SET
                    country_name = %s,
                    iso_alpha2 = %s,
                    record_type_description = %s,
                    value = %s,
                    carbon = %s,
                    score = %s,
                    extracted_at = %s,
                    transformed_at = %s,
                    loaded_at = CURRENT_TIMESTAMP()
                WHEN NOT MATCHED THEN INSERT (
                    country_code, country_name, iso_alpha2, year, record_type, 
                    record_type_description, value, carbon, score, extracted_at, transformed_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                r["country_code"], r["year"], r["record_type"],
                r["country_name"], r.get("iso_alpha2"), r.get("record_type_description"),
                r.get("value"), r.get("carbon"), r.get("score"),
                r.get("extracted_at"), r.get("transformed_at"),
                r["country_code"], r["country_name"], r.get("iso_alpha2"), r["year"],
                r["record_type"], r.get("record_type_description"), r.get("value"),
                r.get("carbon"), r.get("score"), r.get("extracted_at"), r.get("transformed_at"),
            ))
            total_loaded += 1
        
        conn.commit()
        cursor.close()
        conn.close()
        
        return total_loaded
    
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
        description="GFN Data Pipeline - Comprehensive Extraction",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run with DuckDB - all data types (default)
  python -m gfn_pipeline.main --destination duckdb
  
  # Run with Snowflake (production)
  python -m gfn_pipeline.main --destination snowflake
  
  # Run with both destinations
  python -m gfn_pipeline.main --destination both
  
  # Specify year range
  python -m gfn_pipeline.main --start-year 2020 --end-year 2024
  
  # Legacy mode (only EFCtot)
  python -m gfn_pipeline.main --legacy
  
  # Local disk storage (no LocalStack)
  python -m gfn_pipeline.main --no-localstack
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
        help="Don't use LocalStack for S3 storage (use local disk)",
    )
    parser.add_argument(
        "--legacy",
        action="store_true",
        help="Only extract EFCtot (legacy mode)",
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
        include_all_types=not args.legacy,
    )
    
    result = runner.run()
    
    # Print summary
    print("\n" + "=" * 70)
    print("PIPELINE SUMMARY")
    print("=" * 70)
    print(json.dumps(result, indent=2))
    
    # Exit with appropriate code
    sys.exit(0 if result["status"] == "success" else 1)


if __name__ == "__main__":
    main()
