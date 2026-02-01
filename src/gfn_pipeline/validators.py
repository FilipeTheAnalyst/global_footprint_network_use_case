"""
Data Quality Validators for GFN Pipeline.

This module provides Soda-style data quality validation for the staging layer.
Checks are defined in soda/staging_checks.yml and executed before loading to destination.

Architecture:
    Extract → S3 Raw → [Staging Validator] → S3 Staged → dlt → Destination
                              ↑
                    Validates data quality here

Usage:
    from gfn_pipeline.validators import SodaStagingValidator
    
    validator = SodaStagingValidator()
    result = validator.validate(transformed_data)
    
    if not result.passed:
        raise ValueError(f"Data quality checks failed: {result.failed_checks}")
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

logger = logging.getLogger("gfn_pipeline.validators")


# =============================================================================
# Data Classes
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
    
    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "passed": self.passed,
            "checks_run": self.checks_run,
            "checks_passed": self.checks_passed,
            "checks_failed": self.checks_failed,
            "checks_warned": self.checks_warned,
            "failed_checks": self.failed_checks,
            "warnings": self.warnings,
            "duration_seconds": self.duration_seconds,
        }


# =============================================================================
# Staging Validator
# =============================================================================

class SodaStagingValidator:
    """
    Soda-style data quality validator for staging layer.
    
    Reads check definitions from soda/staging_checks.yml and validates
    transformed data before loading to destination.
    
    This catches data quality issues early in the pipeline, before they
    reach the warehouse.
    
    Attributes:
        checks: Dictionary of check definitions loaded from YAML
        fail_on_error: If True, raise exception on check failure
        warn_only: If True, only warn on failures (don't fail pipeline)
    """
    
    DEFAULT_CHECKS_PATH = Path(__file__).parent.parent.parent / "soda" / "staging_checks.yml"
    
    def __init__(
        self,
        checks_path: Path | str | None = None,
        fail_on_error: bool = True,
        warn_only: bool = False,
    ):
        """
        Initialize Soda validator.
        
        Args:
            checks_path: Path to staging_checks.yml (default: soda/staging_checks.yml)
            fail_on_error: Raise exception if checks fail
            warn_only: Only warn on failures, don't fail pipeline
        """
        self.checks_path = Path(checks_path) if checks_path else self.DEFAULT_CHECKS_PATH
        self.fail_on_error = fail_on_error
        self.warn_only = warn_only
        self.checks = self._load_checks()
    
    def _load_checks(self) -> dict[str, Any]:
        """Load check definitions from YAML file."""
        if not self.checks_path.exists():
            logger.warning(f"Checks file not found: {self.checks_path}, using defaults")
            return self._default_checks()
        
        with open(self.checks_path) as f:
            checks = yaml.safe_load(f)
        
        logger.debug(f"Loaded checks from {self.checks_path}")
        return checks
    
    def _default_checks(self) -> dict[str, Any]:
        """Return default checks if YAML file not found."""
        return {
            "footprint_data": {
                "row_count": {"min": 1},
                "required_columns": ["country_code", "country_name", "year", "record_type"],
                "valid_year_range": {"min": 1960, "max": 2030},
                "non_negative_value": True,
                "unique_key": ["country_code", "year", "record_type"],
            },
            "countries": {
                "row_count": {"min": 1},
                "required_columns": ["country_code", "country_name"],
                "unique_key": ["country_code"],
            },
        }
    
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
        if "footprint_data" in self.checks:
            footprint_result = self._validate_footprint_data(
                data.get("footprint_data", [])
            )
            self._merge_results(result, footprint_result)
        
        # Validate countries
        if "countries" in self.checks:
            countries_result = self._validate_countries(
                data.get("countries", [])
            )
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
        """Validate footprint_data records against check definitions."""
        result = SodaCheckResult(passed=True)
        checks = self.checks.get("footprint_data", {})
        
        # Check 1: Row count
        if "row_count" in checks:
            result.checks_run += 1
            min_rows = checks["row_count"].get("min", 1)
            if len(records) >= min_rows:
                result.checks_passed += 1
                logger.debug(f"✓ footprint_data.row_count: {len(records)} records")
            else:
                result.checks_failed += 1
                result.failed_checks.append(
                    f"footprint_data.row_count: expected >= {min_rows}, got {len(records)}"
                )
        
        # Check 2: Required columns (null check)
        for col in checks.get("required_columns", []):
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
        if "valid_year_range" in checks:
            result.checks_run += 1
            year_range = checks["valid_year_range"]
            min_year = year_range.get("min", 1960)
            max_year = year_range.get("max", 2030)
            invalid_years = [
                r for r in records 
                if r.get("year") and (r["year"] < min_year or r["year"] > max_year)
            ]
            if not invalid_years:
                result.checks_passed += 1
                logger.debug("✓ footprint_data.year: all values in valid range")
            else:
                result.checks_failed += 1
                result.failed_checks.append(
                    f"footprint_data.year: {len(invalid_years)} values outside range [{min_year}, {max_year}]"
                )
        
        # Check 4: Valid record types
        if "valid_record_types" in checks:
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
                # Warning only - schema evolution may add new types
                result.checks_passed += 1
        
        # Check 5: Non-negative values
        if checks.get("non_negative_value"):
            result.checks_run += 1
            negative_values = [
                r for r in records 
                if r.get("value") is not None and r["value"] < 0
            ]
            if not negative_values:
                result.checks_passed += 1
                logger.debug("✓ footprint_data.value: all values non-negative")
            else:
                result.checks_failed += 1
                result.failed_checks.append(
                    f"footprint_data.value: {len(negative_values)} negative values"
                )
        
        # Check 6: Unique key (no duplicates)
        if "unique_key" in checks:
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
                logger.debug("✓ footprint_data.unique_key: no duplicates")
            else:
                result.checks_failed += 1
                result.failed_checks.append(
                    f"footprint_data.unique_key: {duplicates} duplicate records"
                )
        
        return result
    
    def _validate_countries(self, records: list[dict]) -> SodaCheckResult:
        """Validate countries records against check definitions."""
        result = SodaCheckResult(passed=True)
        checks = self.checks.get("countries", {})
        
        # Check 1: Row count
        if "row_count" in checks:
            result.checks_run += 1
            min_rows = checks["row_count"].get("min", 1)
            if len(records) >= min_rows:
                result.checks_passed += 1
                logger.debug(f"✓ countries.row_count: {len(records)} records")
            else:
                result.checks_failed += 1
                result.failed_checks.append(
                    f"countries.row_count: expected >= {min_rows}, got {len(records)}"
                )
        
        # Check 2: Required columns
        for col in checks.get("required_columns", []):
            result.checks_run += 1
            missing = sum(1 for r in records if not r.get(col))
            if missing == 0:
                result.checks_passed += 1
                logger.debug(f"✓ countries.{col}: no missing values")
            else:
                result.checks_failed += 1
                result.failed_checks.append(
                    f"countries.{col}: {missing} missing values"
                )
        
        # Check 3: Unique country codes
        if "unique_key" in checks:
            result.checks_run += 1
            country_codes = [r.get("country_code") for r in records if r.get("country_code")]
            duplicates = len(country_codes) - len(set(country_codes))
            if duplicates == 0:
                result.checks_passed += 1
                logger.debug("✓ countries.country_code: all unique")
            else:
                result.checks_failed += 1
                result.failed_checks.append(
                    f"countries.country_code: {duplicates} duplicates"
                )
        
        # Check 4: Minimum country coverage
        if "min_country_coverage" in checks:
            result.checks_run += 1
            country_codes = [r.get("country_code") for r in records if r.get("country_code")]
            unique_countries = len(set(country_codes))
            min_coverage = checks["min_country_coverage"]
            if unique_countries >= min_coverage:
                result.checks_passed += 1
                logger.debug(f"✓ countries.coverage: {unique_countries} countries")
            else:
                result.checks_warned += 1
                result.warnings.append(
                    f"countries.coverage: only {unique_countries} countries (expected >= {min_coverage})"
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
