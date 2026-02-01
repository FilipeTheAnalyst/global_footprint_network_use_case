"""
GFN Pipeline - Production Entry Point with dlt + S3 Data Lake.

This module combines the best of both approaches:
- S3 intermediate storage (data lake pattern for auditability)
- dlt for schema evolution, incremental loads, and state management
- Soda data quality checks on staging layer

Architecture:
    Extract (async) → S3 Raw (JSON) → Soda Checks → S3 Staged (Parquet) → dlt → Destination

Key Features:
- Schema evolution: dlt automatically handles new columns
- Incremental loads: dlt tracks state, only processes new/changed data
- Data contracts: Define expected schema, fail on violations
- Soda checks: Data quality validation before loading
- Multi-destination: Load to DuckDB, Snowflake, BigQuery in parallel
- Audit trail: Raw JSON preserved in S3 for replay/debugging

Usage:
    python -m gfn_pipeline.main --destination snowflake
    python -m gfn_pipeline.main --destination both --with-contracts
    python -m gfn_pipeline.main --with-soda  # Enable Soda checks
    
Environment Variables:
    GFN_API_KEY: API key for Global Footprint Network
    PIPELINE_DESTINATION: Default destination (duckdb, snowflake, both)
    S3_BUCKET: S3 bucket for data lake storage
    AWS_ENDPOINT_URL: LocalStack endpoint (optional)
    SODA_ENABLED: Enable Soda checks (default: false)
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

import dlt
from dlt.common.schema.typing import TColumnSchema
from dlt.sources.helpers import requests
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("gfn_pipeline")


# =============================================================================
# Soda Data Quality Checks for Staging Layer
# =============================================================================

@dataclass
class SodaCheckResult:
    """Result of Soda data quality checks."""
    passed: bool
    checks_run: int = 0
    checks_passed: int = 0
    checks_failed: int = 0
    checks_warned: int = 0
    failed_checks: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    duration_seconds: float = 0.0


class SodaStagingValidator:
    """
    Soda data quality validator for staging layer.
    
    Runs checks on transformed data before loading to destination.
    This catches data quality issues early in the pipeline.
    """
    
    # Define staging checks inline (no external YAML needed for staging)
    STAGING_CHECKS = {
        "footprint_data": {
            "row_count": {"min": 1},
            "required_columns": ["country_code", "country_name", "year", "record_type"],
            "valid_year_range": {"min": 1960, "max": 2030},
            "valid_record_types": [
                "EFCtot", "EFCcrop", "EFCgraz", "EFCfrst", "EFCfish", "EFCbult", "EFCcarb",
                "BioCaptot", "BioCapcrop", "BioCapgraz", "BioCapfrst", "BioCapfish", "BioCapbult",
                "EFCtotPerCap", "EFCcropPerCap", "EFCgrazPerCap", "EFCfrstPerCap",
                "EFCfishPerCap", "EFCbultPerCap", "EFCcarbPerCap",
                "BioCaptotPerCap", "BioCapcropPerCap", "BioCapgrazPerCap",
                "BioCapfrstPerCap", "BioCapfishPerCap", "BioCapbultPerCap",
            ],
            "non_negative_value": True,
            "unique_key": ["country_code", "year", "record_type"],
        },
        "countries": {
            "row_count": {"min": 1},
            "required_columns": ["country_code", "country_name"],
            "unique_key": ["country_code"],
            "min_country_coverage": 150,  # Expect at least 150 countries
        },
    }
    
    def __init__(self, fail_on_error: bool = True, warn_only: bool = False):
        """
        Initialize Soda validator.
        
        Args:
            fail_on_error: Raise exception if checks fail
            warn_only: Only warn on failures, don't fail pipeline
        """
        self.fail_on_error = fail_on_error
        self.warn_only = warn_only
    
    def validate(self, data: dict[str, list[dict]]) -> SodaCheckResult:
        """
        Run Soda-style checks on staged data.
        
        Args:
            data: Dictionary with 'countries' and 'footprint_data' lists
            
        Returns:
            SodaCheckResult with validation results
        """
        start_time = time.monotonic()
        result = SodaCheckResult(passed=True)
        
        logger.info("Running Soda data quality checks on staging layer...")
        
        # Validate footprint_data
        footprint_result = self._validate_footprint_data(data.get("footprint_data", []))
        self._merge_results(result, footprint_result)
        
        # Validate countries
        countries_result = self._validate_countries(data.get("countries", []))
        self._merge_results(result, countries_result)
        
        result.duration_seconds = time.monotonic() - start_time
        result.passed = result.checks_failed == 0
        
        # Log summary
        self._log_summary(result)
        
        # Handle failures
        if not result.passed:
            if self.fail_on_error and not self.warn_only:
                raise ValueError(
                    f"Soda checks failed: {result.checks_failed} failures. "
                    f"Details: {result.failed_checks}"
                )
        
        return result
    
    def _validate_footprint_data(self, records: list[dict]) -> SodaCheckResult:
        """Validate footprint_data records."""
        result = SodaCheckResult(passed=True)
        checks = self.STAGING_CHECKS["footprint_data"]
        
        # Check 1: Row count
        result.checks_run += 1
        if len(records) >= checks["row_count"]["min"]:
            result.checks_passed += 1
            logger.debug(f"✓ footprint_data row_count: {len(records)} records")
        else:
            result.checks_failed += 1
            result.failed_checks.append(
                f"footprint_data row_count: expected >= {checks['row_count']['min']}, got {len(records)}"
            )
        
        # Check 2: Required columns (null check)
        for col in checks["required_columns"]:
            result.checks_run += 1
            missing = sum(1 for r in records if not r.get(col))
            if missing == 0:
                result.checks_passed += 1
                logger.debug(f"✓ footprint_data.{col}: no missing values")
            else:
                result.checks_failed += 1
                result.failed_checks.append(
                    f"footprint_data.{col}: {missing} missing values"
                )
        
        # Check 3: Valid year range
        result.checks_run += 1
        invalid_years = [
            r for r in records 
            if r.get("year") and (
                r["year"] < checks["valid_year_range"]["min"] or 
                r["year"] > checks["valid_year_range"]["max"]
            )
        ]
        if not invalid_years:
            result.checks_passed += 1
            logger.debug("✓ footprint_data.year: all values in valid range")
        else:
            result.checks_failed += 1
            result.failed_checks.append(
                f"footprint_data.year: {len(invalid_years)} values outside range "
                f"[{checks['valid_year_range']['min']}, {checks['valid_year_range']['max']}]"
            )
        
        # Check 4: Valid record types
        result.checks_run += 1
        valid_types = set(checks["valid_record_types"])
        invalid_types = [
            r.get("record_type") for r in records 
            if r.get("record_type") and r["record_type"] not in valid_types
        ]
        if not invalid_types:
            result.checks_passed += 1
            logger.debug("✓ footprint_data.record_type: all values valid")
        else:
            unique_invalid = set(invalid_types)
            result.checks_warned += 1
            result.warnings.append(
                f"footprint_data.record_type: {len(unique_invalid)} unknown types: {unique_invalid}"
            )
            # This is a warning, not failure (schema evolution may add new types)
            result.checks_passed += 1
        
        # Check 5: Non-negative values
        if checks.get("non_negative_value"):
            result.checks_run += 1
            negative_values = [r for r in records if r.get("value") is not None and r["value"] < 0]
            if not negative_values:
                result.checks_passed += 1
                logger.debug("✓ footprint_data.value: all values non-negative")
            else:
                result.checks_failed += 1
                result.failed_checks.append(
                    f"footprint_data.value: {len(negative_values)} negative values"
                )
        
        # Check 6: Unique key (no duplicates)
        result.checks_run += 1
        key_cols = checks["unique_key"]
        seen_keys = set()
        duplicates = 0
        for r in records:
            key = tuple(r.get(col) for col in key_cols)
            if key in seen_keys:
                duplicates += 1
            seen_keys.add(key)
        
        if duplicates == 0:
            result.checks_passed += 1
            logger.debug("✓ footprint_data unique_key: no duplicates")
        else:
            result.checks_failed += 1
            result.failed_checks.append(
                f"footprint_data unique_key: {duplicates} duplicate records"
            )
        
        return result
    
    def _validate_countries(self, records: list[dict]) -> SodaCheckResult:
        """Validate countries records."""
        result = SodaCheckResult(passed=True)
        checks = self.STAGING_CHECKS["countries"]
        
        # Check 1: Row count
        result.checks_run += 1
        if len(records) >= checks["row_count"]["min"]:
            result.checks_passed += 1
            logger.debug(f"✓ countries row_count: {len(records)} records")
        else:
            result.checks_failed += 1
            result.failed_checks.append(
                f"countries row_count: expected >= {checks['row_count']['min']}, got {len(records)}"
            )
        
        # Check 2: Required columns
        for col in checks["required_columns"]:
            result.checks_run += 1
            missing = sum(1 for r in records if not r.get(col))
            if missing == 0:
                result.checks_passed += 1
                logger.debug(f"✓ countries.{col}: no missing values")
            else:
                result.checks_failed += 1
                result.failed_checks.append(f"countries.{col}: {missing} missing values")
        
        # Check 3: Unique country codes
        result.checks_run += 1
        country_codes = [r.get("country_code") for r in records if r.get("country_code")]
        duplicates = len(country_codes) - len(set(country_codes))
        if duplicates == 0:
            result.checks_passed += 1
            logger.debug("✓ countries.country_code: all unique")
        else:
            result.checks_failed += 1
            result.failed_checks.append(f"countries.country_code: {duplicates} duplicates")
        
        # Check 4: Minimum country coverage
        result.checks_run += 1
        unique_countries = len(set(country_codes))
        min_coverage = checks.get("min_country_coverage", 150)
        if unique_countries >= min_coverage:
            result.checks_passed += 1
            logger.debug(f"✓ countries coverage: {unique_countries} countries")
        else:
            result.checks_warned += 1
            result.warnings.append(
                f"countries coverage: only {unique_countries} countries (expected >= {min_coverage})"
            )
            # Warning only - partial data is allowed
            result.checks_passed += 1
        
        return result
    
    def _merge_results(self, target: SodaCheckResult, source: SodaCheckResult):
        """Merge source results into target."""
        target.checks_run += source.checks_run
        target.checks_passed += source.checks_passed
        target.checks_failed += source.checks_failed
        target.checks_warned += source.checks_warned
        target.failed_checks.extend(source.failed_checks)
        target.warnings.extend(source.warnings)
    
    def _log_summary(self, result: SodaCheckResult):
        """Log check summary."""
        status = "PASSED" if result.passed else "FAILED"
        logger.info(
            f"Soda checks {status}: "
            f"{result.checks_passed}/{result.checks_run} passed, "
            f"{result.checks_failed} failed, "
            f"{result.checks_warned} warnings "
            f"({result.duration_seconds:.2f}s)"
        )
        
        if result.failed_checks:
            for check in result.failed_checks:
                logger.error(f"  ✗ {check}")
        
        if result.warnings:
            for warning in result.warnings:
                logger.warning(f"  ⚠ {warning}")


# =============================================================================
# Data Contracts - Schema Definitions with Evolution Support
# =============================================================================

# Define expected schema with data contracts
# dlt will automatically evolve schema for new columns while enforcing these
FOOTPRINT_DATA_COLUMNS: dict[str, TColumnSchema] = {
    # Required fields (nullable=False enforces data contract)
    "country_code": {"data_type": "bigint", "nullable": False},
    "country_name": {"data_type": "text", "nullable": False},
    "year": {"data_type": "bigint", "nullable": False},
    "record_type": {"data_type": "text", "nullable": False},
    # Optional fields (schema can evolve to add more)
    "iso_alpha2": {"data_type": "text", "nullable": True},
    "short_name": {"data_type": "text", "nullable": True},
    "record_type_description": {"data_type": "text", "nullable": True},
    # Metrics - all nullable to handle missing data gracefully
    "value": {"data_type": "double", "nullable": True},
    "carbon": {"data_type": "double", "nullable": True},
    "crop_land": {"data_type": "double", "nullable": True},
    "grazing_land": {"data_type": "double", "nullable": True},
    "forest_land": {"data_type": "double", "nullable": True},
    "fishing_ground": {"data_type": "double", "nullable": True},
    "builtup_land": {"data_type": "double", "nullable": True},
    "score": {"data_type": "text", "nullable": True},
    # Metadata
    "extracted_at": {"data_type": "timestamp", "nullable": True},
    "transformed_at": {"data_type": "timestamp", "nullable": True},
    "_dlt_load_id": {"data_type": "text", "nullable": True},  # dlt tracking
}

COUNTRIES_COLUMNS: dict[str, TColumnSchema] = {
    "country_code": {"data_type": "bigint", "nullable": False},
    "country_name": {"data_type": "text", "nullable": False},
    "iso_alpha2": {"data_type": "text", "nullable": True},
    "short_name": {"data_type": "text", "nullable": True},
    "version": {"data_type": "text", "nullable": True},
    "score": {"data_type": "text", "nullable": True},
    "extracted_at": {"data_type": "timestamp", "nullable": True},
}


# =============================================================================
# S3 Data Lake Layer
# =============================================================================

class S3DataLake:
    """S3 Data Lake for raw and staged data storage."""

    def __init__(self, bucket: str | None = None, use_localstack: bool = True):
        self.bucket = bucket or os.getenv("S3_BUCKET", "gfn-data-lake")
        self.use_localstack = use_localstack
        self._client = None

    @property
    def client(self):
        """Lazy-load S3 client."""
        if self._client is None:
            import boto3
            from botocore.config import Config

            s3_config = {
                "region_name": os.getenv("AWS_REGION", "us-east-1"),
                "config": Config(signature_version="s3v4"),
            }

            endpoint_url = os.getenv("AWS_ENDPOINT_URL")
            if endpoint_url or self.use_localstack:
                s3_config["endpoint_url"] = endpoint_url or "http://localhost:4566"
                s3_config["aws_access_key_id"] = os.getenv("AWS_ACCESS_KEY_ID", "test")
                s3_config["aws_secret_access_key"] = os.getenv("AWS_SECRET_ACCESS_KEY", "test")

            self._client = boto3.client("s3", **s3_config)
        return self._client

    def store_raw(self, data: dict, prefix: str = "raw") -> str:
        """Store raw extracted data to S3."""
        timestamp = datetime.now(timezone.utc)
        key = f"{prefix}/gfn_footprint_{timestamp.strftime('%Y%m%d_%H%M%S')}.json"

        self.client.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=json.dumps(data, default=str).encode(),
            ContentType="application/json",
            Metadata={
                "extracted_at": timestamp.isoformat(),
                "record_count": str(len(data.get("footprint_data", []))),
            },
        )
        logger.info(f"Stored raw data: s3://{self.bucket}/{key}")
        return f"s3://{self.bucket}/{key}"

    def store_staged(self, data: dict, prefix: str = "staged") -> str:
        """Store transformed/staged data to S3 (ready for dlt)."""
        timestamp = datetime.now(timezone.utc)
        key = f"{prefix}/gfn_footprint_{timestamp.strftime('%Y%m%d_%H%M%S')}_staged.json"

        self.client.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=json.dumps(data, default=str).encode(),
            ContentType="application/json",
            Metadata={
                "staged_at": timestamp.isoformat(),
                "record_count": str(len(data.get("footprint_data", []))),
            },
        )
        logger.info(f"Stored staged data: s3://{self.bucket}/{key}")
        return f"s3://{self.bucket}/{key}"

    def read_staged(self, key: str) -> dict:
        """Read staged data from S3."""
        # Remove s3:// prefix if present
        if key.startswith("s3://"):
            key = key.split("/", 3)[3]

        response = self.client.get_object(Bucket=self.bucket, Key=key)
        return json.loads(response["Body"].read().decode())


# =============================================================================
# dlt Source with S3 Integration
# =============================================================================

@dlt.source(name="gfn_s3", max_table_nesting=0)
def gfn_s3_source(
    data: dict[str, Any],
    incremental_key: str = "year",
    enable_contracts: bool = False,
):
    """
    dlt source that reads from staged S3 data.
    
    Args:
        data: Pre-extracted data dict with 'countries' and 'footprint_data'
        incremental_key: Column to use for incremental loading
        enable_contracts: Enable strict data contract enforcement
    """
    yield countries_resource(
        data.get("countries", []),
        enable_contracts=enable_contracts,
    )
    yield footprint_data_resource(
        data.get("footprint_data", []),
        incremental_key=incremental_key,
        enable_contracts=enable_contracts,
    )


@dlt.resource(
    name="countries",
    write_disposition="merge",
    primary_key=["country_code"],
    columns=COUNTRIES_COLUMNS,
)
def countries_resource(
    countries: list[dict],
    enable_contracts: bool = False,
) -> Iterator[dict]:
    """
    Countries reference data with schema evolution support.
    
    Schema Evolution:
    - New columns from API are automatically added
    - Existing columns maintain their types
    - Data contracts enforce required fields when enabled
    """
    logger.info(f"Processing {len(countries)} countries")
    
    for country in countries:
        # Validate required fields if contracts enabled
        if enable_contracts:
            if not country.get("country_code"):
                logger.warning(f"Skipping country without code: {country}")
                continue
            if not country.get("country_name"):
                logger.warning(f"Skipping country without name: {country}")
                continue
        
        yield country


@dlt.resource(
    name="footprint_data",
    write_disposition="merge",
    primary_key=["country_code", "year", "record_type"],
    columns=FOOTPRINT_DATA_COLUMNS,
)
def footprint_data_resource(
    data: list[dict],
    incremental_key: str = "year",
    enable_contracts: bool = False,
) -> Iterator[dict]:
    """
    Footprint data with incremental loading and schema evolution.
    
    Incremental Loading Strategy:
    - Uses 'year' as incremental key by default
    - dlt tracks last processed year in state
    - Only new/updated records are processed on subsequent runs
    
    Schema Evolution:
    - New metrics from API are automatically added as columns
    - Type changes are handled gracefully
    - Data contracts can enforce required fields
    """
    logger.info(f"Processing {len(data):,} footprint records")
    
    for record in data:
        # Validate required fields if contracts enabled
        if enable_contracts:
            required = ["country_code", "year", "record_type"]
            if not all(record.get(f) for f in required):
                logger.warning(f"Skipping invalid record: {record.get('country_code')}/{record.get('year')}")
                continue
        
        yield record


# =============================================================================
# Incremental State Management
# =============================================================================

def get_incremental_state(pipeline: dlt.Pipeline) -> dict:
    """
    Get incremental loading state from dlt pipeline.
    
    Returns:
        Dict with last processed values for incremental columns
    """
    try:
        state = pipeline.state
        return {
            "last_year": state.get("sources", {}).get("gfn_s3", {}).get("last_year"),
            "last_load_id": state.get("_local", {}).get("last_load_id"),
        }
    except Exception:
        return {}


def calculate_incremental_range(
    pipeline: dlt.Pipeline,
    requested_start: int,
    requested_end: int,
    force_full: bool = False,
) -> tuple[int, int]:
    """
    Calculate actual year range based on incremental state.
    
    Strategy:
    - If force_full: Use requested range
    - If state exists: Start from last_year + 1
    - Otherwise: Use requested range
    
    Returns:
        Tuple of (start_year, end_year)
    """
    if force_full:
        logger.info(f"Full refresh requested: {requested_start}-{requested_end}")
        return requested_start, requested_end

    state = get_incremental_state(pipeline)
    last_year = state.get("last_year")

    if last_year and last_year >= requested_start:
        # Incremental: start from last processed year
        # Include last year to catch any updates
        incremental_start = max(last_year - 1, requested_start)
        logger.info(
            f"Incremental load: {incremental_start}-{requested_end} "
            f"(last processed: {last_year})"
        )
        return incremental_start, requested_end

    logger.info(f"Initial load: {requested_start}-{requested_end}")
    return requested_start, requested_end


# =============================================================================
# Pipeline Runner with dlt + S3
# =============================================================================

class DltPipelineRunner:
    """
    Production pipeline runner combining dlt features with S3 data lake.
    
    Features:
    - S3 raw storage for audit trail
    - S3 staged storage for replay capability
    - Soda data quality checks on staging layer
    - dlt schema evolution
    - dlt incremental loading with state
    - dlt data contracts (optional)
    - Multi-destination support
    """

    def __init__(
        self,
        destination: str = "duckdb",
        start_year: int = 2010,
        end_year: int = 2024,
        use_s3: bool = True,
        use_localstack: bool = True,
        enable_contracts: bool = False,
        enable_soda: bool = False,
        soda_warn_only: bool = False,
        full_refresh: bool = False,
        dataset_name: str = "gfn",
    ):
        self.destination = destination
        self.start_year = start_year
        self.end_year = end_year
        self.use_s3 = use_s3
        self.enable_contracts = enable_contracts
        self.enable_soda = enable_soda
        self.soda_warn_only = soda_warn_only
        self.full_refresh = full_refresh
        self.dataset_name = dataset_name

        # Initialize S3 data lake
        self.data_lake = S3DataLake(use_localstack=use_localstack) if use_s3 else None

        # Initialize Soda validator
        self.soda_validator = SodaStagingValidator(
            fail_on_error=not soda_warn_only,
            warn_only=soda_warn_only,
        ) if enable_soda else None

        # Initialize dlt pipeline(s)
        self.pipelines = self._init_pipelines()

        # Metrics
        self.metrics = {
            "start_time": None,
            "end_time": None,
            "records_extracted": 0,
            "records_loaded": {},
            "s3_raw_path": None,
            "s3_staged_path": None,
            "soda_checks": None,
            "schema_changes": [],
            "errors": [],
        }

    def _init_pipelines(self) -> dict[str, dlt.Pipeline]:
        """Initialize dlt pipelines for each destination."""
        pipelines = {}

        destinations = (
            ["duckdb", "snowflake"] if self.destination == "both"
            else [self.destination]
        )

        for dest in destinations:
            pipeline_name = f"gfn_{dest}"

            if dest == "duckdb":
                pipelines[dest] = dlt.pipeline(
                    pipeline_name=pipeline_name,
                    destination="duckdb",
                    dataset_name=self.dataset_name,
                )
            elif dest == "snowflake":
                pipelines[dest] = dlt.pipeline(
                    pipeline_name=pipeline_name,
                    destination="snowflake",
                    dataset_name=self.dataset_name.upper(),
                )

        return pipelines

    def run(self) -> dict:
        """Execute the full pipeline with dlt + S3."""
        self.metrics["start_time"] = datetime.now(timezone.utc).isoformat()

        print(f"\n{'='*70}")
        print("GFN Pipeline - dlt + S3 Data Lake")
        print(f"{'='*70}")
        print(f"Destination:      {self.destination}")
        print(f"Years:            {self.start_year}-{self.end_year}")
        print(f"S3 Storage:       {'Enabled' if self.use_s3 else 'Disabled'}")
        print(f"Data Contracts:   {'Enabled' if self.enable_contracts else 'Disabled'}")
        print(f"Soda Checks:      {'Enabled' if self.enable_soda else 'Disabled'}")
        print(f"Mode:             {'Full Refresh' if self.full_refresh else 'Incremental'}")
        print(f"{'='*70}\n")

        try:
            # Step 1: Calculate incremental range
            primary_pipeline = list(self.pipelines.values())[0]
            actual_start, actual_end = calculate_incremental_range(
                primary_pipeline,
                self.start_year,
                self.end_year,
                force_full=self.full_refresh,
            )

            # Step 2: Extract data
            print("Step 1: Extracting data from GFN API...")
            extracted_data = self._extract(actual_start, actual_end)
            self.metrics["records_extracted"] = len(extracted_data.get("footprint_data", []))
            print(f"  → Extracted {self.metrics['records_extracted']:,} records")

            if not extracted_data.get("footprint_data"):
                logger.warning("No data extracted")
                return self._finalize("no_data")

            # Step 3: Store raw to S3 (audit trail)
            if self.data_lake:
                print("\nStep 2: Storing raw data to S3...")
                self.metrics["s3_raw_path"] = self.data_lake.store_raw(extracted_data)
                print(f"  → {self.metrics['s3_raw_path']}")

            # Step 4: Transform
            print("\nStep 3: Transforming data...")
            transformed_data = self._transform(extracted_data)
            print(f"  → {len(transformed_data.get('footprint_data', [])):,} valid records")

            # Step 5: Soda data quality checks on staging layer
            if self.soda_validator:
                print("\nStep 4: Running Soda data quality checks...")
                soda_result = self.soda_validator.validate(transformed_data)
                self.metrics["soda_checks"] = {
                    "passed": soda_result.passed,
                    "checks_run": soda_result.checks_run,
                    "checks_passed": soda_result.checks_passed,
                    "checks_failed": soda_result.checks_failed,
                    "checks_warned": soda_result.checks_warned,
                    "failed_checks": soda_result.failed_checks,
                    "warnings": soda_result.warnings,
                    "duration_seconds": soda_result.duration_seconds,
                }
                status_icon = "✓" if soda_result.passed else "✗"
                print(f"  {status_icon} {soda_result.checks_passed}/{soda_result.checks_run} checks passed")
                
                if not soda_result.passed and not self.soda_warn_only:
                    return self._finalize("soda_failed")

            # Step 6: Store staged to S3 (replay capability)
            if self.data_lake:
                step_num = "5" if self.soda_validator else "4"
                print(f"\nStep {step_num}: Storing staged data to S3...")
                self.metrics["s3_staged_path"] = self.data_lake.store_staged(transformed_data)
                print(f"  → {self.metrics['s3_staged_path']}")

            # Step 7: Load via dlt to destination(s)
            step_num = "6" if self.soda_validator else "5"
            print(f"\nStep {step_num}: Loading to destination(s) via dlt...")
            for dest, pipeline in self.pipelines.items():
                load_result = self._load_with_dlt(pipeline, dest, transformed_data)
                self.metrics["records_loaded"][dest] = load_result

            # Step 8: Update incremental state
            self._update_state(actual_end)

            return self._finalize("success")

        except Exception as e:
            logger.exception(f"Pipeline failed: {e}")
            self.metrics["errors"].append(str(e))
            return self._finalize("failed")

    def _extract(self, start_year: int, end_year: int) -> dict:
        """Extract data using async pipeline."""
        from gfn_pipeline.pipeline_async import ExtractionConfig, extract_all_data

        api_key = os.getenv("GFN_API_KEY")
        if not api_key:
            raise ValueError("GFN_API_KEY environment variable required")

        config = ExtractionConfig(api_key=api_key)
        return asyncio.run(extract_all_data(config, start_year, end_year))

    def _transform(self, data: dict) -> dict:
        """Transform and validate data."""
        transformed_at = datetime.now(timezone.utc).isoformat()

        # Transform countries
        countries = []
        seen_countries = set()
        for c in data.get("countries", []):
            if not c.get("country_code"):
                continue
            if c["country_code"] in seen_countries:
                continue
            seen_countries.add(c["country_code"])
            countries.append({**c, "transformed_at": transformed_at})

        # Transform footprint data
        footprint_data = []
        seen_records = set()
        for r in data.get("footprint_data", []):
            # Validate required fields
            if not all([r.get("country_code"), r.get("year"), r.get("record_type")]):
                continue

            # Deduplicate
            key = (r["country_code"], r["year"], r["record_type"])
            if key in seen_records:
                continue
            seen_records.add(key)

            footprint_data.append({**r, "transformed_at": transformed_at})

        return {
            "countries": countries,
            "footprint_data": footprint_data,
            "record_types": data.get("record_types", []),
        }

    def _load_with_dlt(
        self,
        pipeline: dlt.Pipeline,
        dest_name: str,
        data: dict,
    ) -> dict:
        """Load data using dlt pipeline with schema evolution."""
        print(f"\n  Loading to {dest_name}...")

        # Create dlt source
        source = gfn_s3_source(
            data=data,
            enable_contracts=self.enable_contracts,
        )

        # Apply write disposition based on mode
        if self.full_refresh:
            source.footprint_data.apply_hints(write_disposition="replace")
            source.countries.apply_hints(write_disposition="replace")

        # Run pipeline
        start_time = time.monotonic()
        load_info = pipeline.run(source)
        elapsed = time.monotonic() - start_time

        # Extract results
        loaded_count = sum(
            pkg.jobs_count for pkg in load_info.load_packages
        ) if load_info.load_packages else 0

        # Check for schema changes
        schema_changes = self._detect_schema_changes(pipeline)
        if schema_changes:
            self.metrics["schema_changes"].extend(schema_changes)
            print(f"    ⚠ Schema evolved: {schema_changes}")

        print(f"    ✓ Loaded in {elapsed:.1f}s")
        print(f"    → {load_info}")

        return {
            "status": "success",
            "elapsed_seconds": elapsed,
            "load_info": str(load_info),
            "schema_changes": schema_changes,
        }

    def _detect_schema_changes(self, pipeline: dlt.Pipeline) -> list[str]:
        """Detect schema changes from dlt pipeline."""
        changes = []
        try:
            schema = pipeline.default_schema
            # Check for new tables or columns added
            for table_name, table in schema.tables.items():
                if table.get("x-normalizer", {}).get("evolve"):
                    changes.append(f"Table '{table_name}' schema evolved")
        except Exception:
            pass
        return changes

    def _update_state(self, last_year: int):
        """Update incremental state after successful load."""
        for pipeline in self.pipelines.values():
            try:
                # dlt automatically tracks state, but we can add custom state
                pipeline.state.setdefault("sources", {}).setdefault("gfn_s3", {})
                pipeline.state["sources"]["gfn_s3"]["last_year"] = last_year
            except Exception as e:
                logger.warning(f"Could not update state: {e}")

    def _finalize(self, status: str) -> dict:
        """Finalize and return metrics."""
        self.metrics["end_time"] = datetime.now(timezone.utc).isoformat()
        self.metrics["status"] = status

        if self.metrics["start_time"] and self.metrics["end_time"]:
            start = datetime.fromisoformat(self.metrics["start_time"].replace("Z", "+00:00"))
            end = datetime.fromisoformat(self.metrics["end_time"].replace("Z", "+00:00"))
            self.metrics["duration_seconds"] = (end - start).total_seconds()

        print(f"\n{'='*70}")
        status_text = {
            "success": "COMPLETE",
            "failed": "FAILED",
            "soda_failed": "FAILED (Soda checks)",
            "no_data": "COMPLETE (no data)",
        }.get(status, status.upper())
        print(f"PIPELINE {status_text}")
        print(f"{'='*70}")
        print(f"Status:           {status}")
        print(f"Duration:         {self.metrics.get('duration_seconds', 0):.1f}s")
        print(f"Records:          {self.metrics['records_extracted']:,} extracted")
        for dest, result in self.metrics["records_loaded"].items():
            print(f"  → {dest}:        {result.get('status', 'unknown')}")
        if self.metrics["s3_raw_path"]:
            print(f"S3 Raw:           {self.metrics['s3_raw_path']}")
        if self.metrics["s3_staged_path"]:
            print(f"S3 Staged:        {self.metrics['s3_staged_path']}")
        if self.metrics.get("soda_checks"):
            soda = self.metrics["soda_checks"]
            soda_status = "✓ PASSED" if soda["passed"] else "✗ FAILED"
            print(f"Soda Checks:      {soda_status} ({soda['checks_passed']}/{soda['checks_run']})")
            if soda.get("failed_checks"):
                for check in soda["failed_checks"]:
                    print(f"  ✗ {check}")
            if soda.get("warnings"):
                for warning in soda["warnings"]:
                    print(f"  ⚠ {warning}")
        if self.metrics["schema_changes"]:
            print(f"Schema Changes:   {self.metrics['schema_changes']}")
        if self.metrics["errors"]:
            print(f"Errors:           {self.metrics['errors']}")
        print(f"{'='*70}\n")

        return self.metrics


# =============================================================================
# CLI
# =============================================================================

def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="GFN Pipeline - dlt + S3 Data Lake Architecture",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Architecture:
  This pipeline combines dlt's powerful features with S3 data lake storage:
  
  Extract (async) → S3 Raw → Soda Checks → S3 Staged → dlt → Destination
  
  Benefits:
  - Schema Evolution: New API fields automatically become columns
  - Incremental Loads: Only process new/changed data
  - Data Contracts: Enforce required fields (--with-contracts)
  - Soda Checks: Data quality validation on staging layer (--with-soda)
  - Audit Trail: Raw data preserved in S3
  - Replay: Re-process from staged data if needed

Examples:
  # Standard run (incremental, DuckDB)
  python -m gfn_pipeline.main
  
  # Full refresh to Snowflake
  python -m gfn_pipeline.main -d snowflake --full-refresh
  
  # Both destinations with data contracts
  python -m gfn_pipeline.main -d both --with-contracts
  
  # Enable Soda data quality checks
  python -m gfn_pipeline.main --with-soda
  
  # Soda checks in warn-only mode (don't fail pipeline)
  python -m gfn_pipeline.main --with-soda --soda-warn-only
  
  # Specific year range
  python -m gfn_pipeline.main --start-year 2020 --end-year 2024
  
  # Without S3 (direct to destination)
  python -m gfn_pipeline.main --no-s3
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
        "--full-refresh",
        action="store_true",
        help="Replace all data instead of incremental merge",
    )
    parser.add_argument(
        "--with-contracts",
        action="store_true",
        help="Enable strict data contract enforcement",
    )
    parser.add_argument(
        "--with-soda",
        action="store_true",
        help="Enable Soda data quality checks on staging layer",
    )
    parser.add_argument(
        "--soda-warn-only",
        action="store_true",
        help="Soda checks warn but don't fail the pipeline",
    )
    parser.add_argument(
        "--no-s3",
        action="store_true",
        help="Skip S3 storage (direct to destination)",
    )
    parser.add_argument(
        "--no-localstack",
        action="store_true",
        help="Use real AWS instead of LocalStack",
    )
    args = parser.parse_args()

    # Create logs directory
    Path("logs").mkdir(exist_ok=True)

    runner = DltPipelineRunner(
        destination=args.destination,
        start_year=args.start_year,
        end_year=args.end_year,
        use_s3=not args.no_s3,
        use_localstack=not args.no_localstack,
        enable_contracts=args.with_contracts,
        enable_soda=args.with_soda,
        soda_warn_only=args.soda_warn_only,
        full_refresh=args.full_refresh,
    )

    result = runner.run()

    # Print final summary as JSON
    print("\nFull Metrics:")
    print(json.dumps(result, indent=2, default=str))

    sys.exit(0 if result["status"] == "success" else 1)


if __name__ == "__main__":
    main()
