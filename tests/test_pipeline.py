"""
Tests for GFN Pipeline - Covers both dlt+DuckDB and AWS Lambda approaches.

Test Categories:
- Unit tests: Test core logic with mocked dependencies
- Integration tests: Require LocalStack running (marked with @pytest.mark.integration)
"""
import json
import os
import pytest
from datetime import datetime, timezone
from unittest.mock import patch, MagicMock, AsyncMock


# ============================================================================
# Unit Tests - Soda Staging Validators (validators.py)
# ============================================================================

class TestSodaStagingValidator:
    """Tests for Soda staging layer validation."""

    def test_validator_loads_checks_from_yaml(self):
        """Test that validator loads checks from YAML file."""
        from gfn_pipeline.validators import SodaStagingValidator
        
        validator = SodaStagingValidator()
        
        assert "footprint_data" in validator.checks
        assert "countries" in validator.checks
        assert "required_columns" in validator.checks["footprint_data"]

    def test_validator_validates_footprint_data(self):
        """Test footprint_data validation passes with valid data."""
        from gfn_pipeline.validators import SodaStagingValidator
        
        validator = SodaStagingValidator(fail_on_error=False)
        
        data = {
            "footprint_data": [
                {"country_code": 1, "country_name": "Test", "year": 2020, "record_type": "EFConsTotGHA", "value": 100.0},
                {"country_code": 2, "country_name": "Test2", "year": 2020, "record_type": "BiocapTotGHA", "value": 200.0},
            ],
            "countries": [
                {"country_code": 1, "country_name": "Test"},
                {"country_code": 2, "country_name": "Test2"},
            ],
        }
        
        result = validator.validate(data)
        
        assert result.passed
        assert result.checks_failed == 0

    def test_validator_fails_on_missing_required_columns(self):
        """Test validation fails when required columns are missing."""
        from gfn_pipeline.validators import SodaStagingValidator
        
        validator = SodaStagingValidator(fail_on_error=False)
        
        data = {
            "footprint_data": [
                {"country_code": 1, "year": 2020, "record_type": "EF"},  # Missing country_name
            ],
            "countries": [],
        }
        
        result = validator.validate(data)
        
        assert not result.passed
        assert result.checks_failed > 0
        assert any("country_name" in check for check in result.failed_checks)

    def test_validator_fails_on_invalid_year_range(self):
        """Test validation fails when year is outside valid range."""
        from gfn_pipeline.validators import SodaStagingValidator
        
        validator = SodaStagingValidator(fail_on_error=False)
        
        data = {
            "footprint_data": [
                {"country_code": 1, "country_name": "Test", "year": 1800, "record_type": "EF"},  # Invalid year
            ],
            "countries": [],
        }
        
        result = validator.validate(data)
        
        assert not result.passed
        assert any("year" in check for check in result.failed_checks)

    def test_validator_warns_on_unknown_record_types(self):
        """Test validation warns (not fails) on unknown record types."""
        from gfn_pipeline.validators import SodaStagingValidator
        
        validator = SodaStagingValidator(fail_on_error=False)
        
        data = {
            "footprint_data": [
                {"country_code": 1, "country_name": "Test", "year": 2020, "record_type": "UnknownType", "value": 100.0},
            ],
            "countries": [
                {"country_code": 1, "country_name": "Test"},  # Need at least one country
            ],
        }
        
        result = validator.validate(data)
        
        # Should pass (warning only) but have warnings
        assert result.passed
        assert result.checks_warned > 0
        assert any("unknown types" in warning for warning in result.warnings)

    def test_validator_fails_on_duplicates(self):
        """Test validation fails on duplicate records."""
        from gfn_pipeline.validators import SodaStagingValidator
        
        validator = SodaStagingValidator(fail_on_error=False)
        
        data = {
            "footprint_data": [
                {"country_code": 1, "country_name": "Test", "year": 2020, "record_type": "EFConsTotGHA", "value": 100.0},
                {"country_code": 1, "country_name": "Test", "year": 2020, "record_type": "EFConsTotGHA", "value": 200.0},  # Duplicate
            ],
            "countries": [],
        }
        
        result = validator.validate(data)
        
        assert not result.passed
        assert any("duplicate" in check.lower() for check in result.failed_checks)

    def test_validator_result_to_dict(self):
        """Test SodaCheckResult can be serialized to dict."""
        from gfn_pipeline.validators import SodaCheckResult
        
        result = SodaCheckResult(
            passed=True,
            checks_run=10,
            checks_passed=9,
            checks_failed=1,
            failed_checks=["test failure"],
        )
        
        result_dict = result.to_dict()
        
        assert result_dict["passed"] is True
        assert result_dict["checks_run"] == 10
        assert result_dict["failed_checks"] == ["test failure"]


# ============================================================================
# Unit Tests - dlt+DuckDB Pipeline (pipeline_async.py)
# ============================================================================

class TestDynamicRecordTypeDiscovery:
    """Tests for dynamic record type discovery from API."""

    @pytest.mark.asyncio
    async def test_discover_record_types_from_sample_year(self):
        """Test discovering record types from sample year data."""
        from gfn_pipeline.pipeline_async import discover_record_types_from_sample_year

        # Mock API response
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(return_value=[
            {"record": "EFConsTotGHA", "countryCode": 1, "year": 2020},
            {"record": "BiocapTotGHA", "countryCode": 1, "year": 2020},
            {"record": "EFConsTotGHA", "countryCode": 2, "year": 2020},  # Duplicate type
        ])

        mock_session = MagicMock()
        mock_session.get = MagicMock(return_value=AsyncMock(__aenter__=AsyncMock(return_value=mock_response)))

        mock_auth = MagicMock()

        result = await discover_record_types_from_sample_year(
            mock_session, mock_auth, "https://api.test.com", 2020
        )

        assert "EFConsTotGHA" in result
        assert "BiocapTotGHA" in result
        assert len(result) == 2  # Deduplicated

    @pytest.mark.asyncio
    async def test_fetch_record_types_from_api(self):
        """Test fetching record types from /types endpoint."""
        from gfn_pipeline.pipeline_async import fetch_record_types_from_api

        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(return_value=[
            {"code": "EF", "name": "Ecological Footprint", "record": "EFConsTotGHA"},
            {"code": "BC", "name": "Biocapacity", "record": "BiocapTotGHA"},
        ])

        mock_session = MagicMock()
        mock_session.get = MagicMock(return_value=AsyncMock(__aenter__=AsyncMock(return_value=mock_response)))

        mock_auth = MagicMock()

        result = await fetch_record_types_from_api(
            mock_session, mock_auth, "https://api.test.com"
        )

        assert "EF" in result
        assert result["EF"]["name"] == "Ecological Footprint"
        assert result["EF"]["record"] == "EFConsTotGHA"

    def test_get_record_types_sync_caches_result(self):
        """Test that record types are cached after first fetch."""
        from gfn_pipeline.pipeline_async import (
            get_record_types_sync,
            clear_record_types_cache
        )

        # Clear cache first
        clear_record_types_cache()

        # Mock the async fetch
        with patch('gfn_pipeline.pipeline_async.asyncio.run') as mock_run:
            mock_run.return_value = {"EFConsTotGHA": "Ecological Footprint"}

            result1 = get_record_types_sync("test_api_key")
            result2 = get_record_types_sync("test_api_key")

            # Should only call asyncio.run once due to caching
            assert mock_run.call_count == 1
            assert result1 == result2


class TestExtractionConfig:
    """Tests for extraction configuration."""

    def test_config_requires_api_key(self):
        """Test that config raises error without API key."""
        from gfn_pipeline.pipeline_async import ExtractionConfig

        with patch.dict(os.environ, {"GFN_API_KEY": ""}, clear=True):
            with pytest.raises(ValueError, match="API key required"):
                ExtractionConfig(api_key=None)

    def test_config_uses_env_var(self):
        """Test that config reads API key from environment."""
        from gfn_pipeline.pipeline_async import ExtractionConfig

        with patch.dict(os.environ, {"GFN_API_KEY": "test_key_123"}):
            config = ExtractionConfig()
            assert config.api_key == "test_key_123"

    def test_config_defaults(self):
        """Test configuration default values."""
        from gfn_pipeline.pipeline_async import ExtractionConfig

        with patch.dict(os.environ, {"GFN_API_KEY": "test_key"}):
            config = ExtractionConfig()

            assert config.max_concurrent_requests == 5
            assert config.requests_per_second == 2.0
            assert config.request_timeout == 60
            assert config.parallel_year_batches == 3


class TestTokenBucketRateLimiter:
    """Tests for rate limiter."""

    @pytest.mark.asyncio
    async def test_rate_limiter_allows_burst(self):
        """Test that rate limiter allows burst requests."""
        from gfn_pipeline.pipeline_async import TokenBucketRateLimiter

        limiter = TokenBucketRateLimiter(rate=1.0, burst=3)

        # Should allow 3 immediate requests (burst)
        import time
        start = time.monotonic()

        await limiter.acquire()
        await limiter.acquire()
        await limiter.acquire()

        elapsed = time.monotonic() - start
        assert elapsed < 0.5  # Should be nearly instant

    @pytest.mark.asyncio
    async def test_rate_limiter_throttles_after_burst(self):
        """Test that rate limiter throttles after burst exhausted."""
        from gfn_pipeline.pipeline_async import TokenBucketRateLimiter

        limiter = TokenBucketRateLimiter(rate=10.0, burst=1)  # Fast rate for testing

        import time
        start = time.monotonic()

        await limiter.acquire()  # Uses burst
        await limiter.acquire()  # Should wait

        elapsed = time.monotonic() - start
        assert elapsed >= 0.05  # Should have waited ~0.1s


# ============================================================================
# Unit Tests - Lambda Handlers (lambda_handlers.py)
# ============================================================================

class TestLambdaExtractHandler:
    """Tests for Lambda extract handler."""

    def test_extract_handler_returns_step_functions_format(self):
        """Test extract handler returns Step Functions compatible output."""
        from infrastructure.lambda_handlers import handler_extract

        with patch('infrastructure.lambda_handlers._extract_bulk') as mock_extract:
            mock_extract.return_value = {
                "countries": [{"country_code": 1}],
                "footprint_data": [
                    {"country_code": 1, "year": 2024, "record_type": "EFConsTotGHA"}
                ],
                "record_types": ["EFConsTotGHA"],
                "metadata": {"total_records": 1}
            }

            with patch('infrastructure.lambda_handlers.get_s3_client') as mock_s3:
                mock_s3.return_value = MagicMock()

                with patch('infrastructure.lambda_handlers.get_sqs_client') as mock_sqs:
                    mock_sqs.return_value = MagicMock()
                    mock_sqs.return_value.get_queue_url.side_effect = Exception("Queue not found")

                    with patch.dict(os.environ, {"GFN_API_KEY": "test_key"}):
                        result = handler_extract({"start_year": 2024, "end_year": 2024})

        assert "status" in result
        assert "records_count" in result
        assert "s3_key" in result
        assert "s3_bucket" in result

    def test_extract_handler_handles_no_data(self):
        """Test extract handler handles empty API response."""
        from infrastructure.lambda_handlers import handler_extract

        with patch('infrastructure.lambda_handlers._extract_bulk') as mock_extract:
            mock_extract.return_value = {
                "countries": [],
                "footprint_data": [],
                "record_types": [],
                "metadata": {"total_records": 0}
            }

            with patch.dict(os.environ, {"GFN_API_KEY": "test_key"}):
                result = handler_extract({"start_year": 2024, "end_year": 2024})

        assert result["status"] == "no_data"
        assert result["records_count"] == 0


class TestLambdaTransformHandler:
    """Tests for Lambda transform handler."""

    def test_transform_validates_required_fields(self):
        """Test transform handler validates required fields."""
        from infrastructure.lambda_handlers import handler_transform

        raw_data = {
            "footprint_data": [
                {"country_code": 1, "year": 2024, "record_type": "EF"},
                {"country_code": None, "year": 2024, "record_type": "EF"},  # Invalid
                {"country_code": 2, "year": None, "record_type": "EF"},  # Invalid
            ],
            "countries": [],
            "record_types": []
        }

        with patch('infrastructure.lambda_handlers.get_s3_client') as mock_s3:
            mock_s3_instance = MagicMock()
            mock_s3_instance.get_object.return_value = {
                "Body": MagicMock(read=lambda: json.dumps(raw_data).encode())
            }
            mock_s3.return_value = mock_s3_instance

            with patch('infrastructure.lambda_handlers.get_sqs_client') as mock_sqs:
                mock_sqs.return_value = MagicMock()
                mock_sqs.return_value.get_queue_url.side_effect = Exception("Queue not found")

                result = handler_transform({
                    "s3_bucket": "test-bucket",
                    "s3_key": "raw/test/data.json"
                })

        assert result["status"] == "success"
        assert result["records_count"] == 1  # Only valid record

    def test_transform_deduplicates_records(self):
        """Test transform handler removes duplicates."""
        from infrastructure.lambda_handlers import handler_transform

        raw_data = {
            "footprint_data": [
                {"country_code": 1, "year": 2024, "record_type": "EF", "value": 100},
                {"country_code": 1, "year": 2024, "record_type": "EF", "value": 200},  # Duplicate
                {"country_code": 1, "year": 2023, "record_type": "EF", "value": 150},  # Different year
            ],
            "countries": [],
            "record_types": []
        }

        with patch('infrastructure.lambda_handlers.get_s3_client') as mock_s3:
            mock_s3_instance = MagicMock()
            mock_s3_instance.get_object.return_value = {
                "Body": MagicMock(read=lambda: json.dumps(raw_data).encode())
            }
            mock_s3.return_value = mock_s3_instance

            with patch('infrastructure.lambda_handlers.get_sqs_client') as mock_sqs:
                mock_sqs.return_value = MagicMock()
                mock_sqs.return_value.get_queue_url.side_effect = Exception("Queue not found")

                result = handler_transform({
                    "s3_bucket": "test-bucket",
                    "s3_key": "raw/test/data.json"
                })

        assert result["records_count"] == 2  # First record + different year

    def test_transform_calculates_carbon_percentage(self):
        """Test transform handler calculates carbon percentage."""
        from infrastructure.lambda_handlers import handler_transform

        raw_data = {
            "footprint_data": [
                {"country_code": 1, "year": 2024, "record_type": "EF", "carbon": 100, "value": 400},
            ],
            "countries": [],
            "record_types": []
        }

        transformed_data = None

        with patch('infrastructure.lambda_handlers.get_s3_client') as mock_s3:
            mock_s3_instance = MagicMock()
            mock_s3_instance.get_object.return_value = {
                "Body": MagicMock(read=lambda: json.dumps(raw_data).encode())
            }

            def capture_put(Bucket, Key, Body, **kwargs):
                nonlocal transformed_data
                transformed_data = json.loads(Body.decode())

            mock_s3_instance.put_object = capture_put
            mock_s3.return_value = mock_s3_instance

            with patch('infrastructure.lambda_handlers.get_sqs_client') as mock_sqs:
                mock_sqs.return_value = MagicMock()
                mock_sqs.return_value.get_queue_url.side_effect = Exception("Queue not found")

                handler_transform({
                    "s3_bucket": "test-bucket",
                    "s3_key": "raw/test/data.json"
                })

        assert transformed_data is not None
        assert transformed_data["footprint_data"][0]["carbon_pct_of_total"] == 25.0

    def test_transform_handles_sqs_event_wrapper(self):
        """Test transform handler handles SQS event format."""
        from infrastructure.lambda_handlers import handler_transform

        raw_data = {
            "footprint_data": [{"country_code": 1, "year": 2024, "record_type": "EF"}],
            "countries": [],
            "record_types": []
        }

        sqs_event = {
            "Records": [{
                "body": json.dumps({
                    "s3_bucket": "test-bucket",
                    "s3_key": "raw/test/data.json"
                })
            }]
        }

        with patch('infrastructure.lambda_handlers.get_s3_client') as mock_s3:
            mock_s3_instance = MagicMock()
            mock_s3_instance.get_object.return_value = {
                "Body": MagicMock(read=lambda: json.dumps(raw_data).encode())
            }
            mock_s3.return_value = mock_s3_instance

            with patch('infrastructure.lambda_handlers.get_sqs_client') as mock_sqs:
                mock_sqs.return_value = MagicMock()
                mock_sqs.return_value.get_queue_url.side_effect = Exception("Queue not found")

                result = handler_transform(sqs_event)

        assert result["status"] == "success"


class TestLambdaLoadHandler:
    """Tests for Lambda load handler."""

    def test_load_handler_returns_step_functions_format(self):
        """Test load handler returns Step Functions compatible output."""
        from infrastructure.lambda_handlers import handler_load

        processed_data = {
            "footprint_data": [
                {"country_code": 1, "year": 2024, "record_type": "EF"}
            ]
        }

        with patch('infrastructure.lambda_handlers.get_s3_client') as mock_s3:
            mock_s3_instance = MagicMock()
            mock_s3_instance.get_object.return_value = {
                "Body": MagicMock(read=lambda: json.dumps(processed_data).encode())
            }
            mock_s3.return_value = mock_s3_instance

            with patch('infrastructure.lambda_handlers._load_to_duckdb_bulk') as mock_load:
                mock_load.return_value = 1

                with patch.dict(os.environ, {"SNOWFLAKE_ACCOUNT": ""}, clear=False):
                    result = handler_load({
                        "s3_bucket": "test-bucket",
                        "s3_key": "transformed/test_data.json"
                    })

        assert result["status"] == "success"
        assert result["records_loaded"] == 1
        assert result["destination"] == "duckdb"

    def test_load_handler_chooses_snowflake_when_configured(self):
        """Test load handler uses Snowflake when account is configured."""
        from infrastructure.lambda_handlers import handler_load

        processed_data = {
            "footprint_data": [{"country_code": 1, "year": 2024, "record_type": "EF"}]
        }

        with patch('infrastructure.lambda_handlers.get_s3_client') as mock_s3:
            mock_s3_instance = MagicMock()
            mock_s3_instance.get_object.return_value = {
                "Body": MagicMock(read=lambda: json.dumps(processed_data).encode())
            }
            mock_s3.return_value = mock_s3_instance

            with patch('infrastructure.lambda_handlers._load_to_snowflake_bulk') as mock_load:
                mock_load.return_value = 1

                with patch.dict(os.environ, {"SNOWFLAKE_ACCOUNT": "test_account"}):
                    result = handler_load({
                        "s3_bucket": "test-bucket",
                        "s3_key": "transformed/test_data.json"
                    })

        assert result["destination"] == "snowflake"


# ============================================================================
# Unit Tests - Legacy PipelineRunner (main.py)
# ============================================================================

class TestPipelineRunnerTransform:
    """Tests for DltPipelineRunner transformation logic."""

    def test_transform_validates_required_fields(self):
        """Test that transform validates required fields."""
        from gfn_pipeline.main import DltPipelineRunner

        runner = DltPipelineRunner(use_s3=False)

        # New format: dict with countries and footprint_data
        data = {
            "countries": [],
            "footprint_data": [
                {"country_code": 1, "year": 2024, "record_type": "EF", "country_name": "Test"},
                {"country_code": None, "year": 2024, "record_type": "EF", "country_name": "Invalid"},
                {"country_code": 2, "year": None, "record_type": "EF", "country_name": "Invalid"},
                {"country_code": 3, "year": 2024, "record_type": "EF", "country_name": "Valid"},
            ]
        }

        result = runner._transform(data)

        assert len(result["footprint_data"]) == 2
        assert result["footprint_data"][0]["country_code"] == 1
        assert result["footprint_data"][1]["country_code"] == 3

    def test_transform_deduplicates(self):
        """Test that transform removes duplicates."""
        from gfn_pipeline.main import DltPipelineRunner

        runner = DltPipelineRunner(use_s3=False)

        data = {
            "countries": [],
            "footprint_data": [
                {"country_code": 1, "year": 2024, "record_type": "EF", "country_name": "First"},
                {"country_code": 1, "year": 2024, "record_type": "EF", "country_name": "Duplicate"},
                {"country_code": 1, "year": 2023, "record_type": "EF", "country_name": "Different Year"},
            ]
        }

        result = runner._transform(data)

        assert len(result["footprint_data"]) == 2
        assert result["footprint_data"][0]["country_name"] == "First"

    def test_transform_validates_record_type(self):
        """Test that transform requires record_type field."""
        from gfn_pipeline.main import DltPipelineRunner

        runner = DltPipelineRunner(use_s3=False)

        data = {
            "countries": [],
            "footprint_data": [
                {"country_code": 1, "year": 2024, "record_type": "EF"},  # Valid
                {"country_code": 2, "year": 2024},  # Missing record_type
            ]
        }

        result = runner._transform(data)

        assert len(result["footprint_data"]) == 1
        assert result["footprint_data"][0]["country_code"] == 1


class TestDuckDBLoad:
    """Tests for DuckDB loading via dlt.
    
    Note: These tests verify that dlt correctly loads data to DuckDB.
    The DltPipelineRunner uses dlt for all loading operations.
    """

    @pytest.fixture
    def temp_duckdb(self, tmp_path):
        """Create a temporary DuckDB database."""
        db_path = tmp_path / "test.duckdb"
        return str(db_path)

    def test_dlt_load_creates_table(self, temp_duckdb):
        """Test that dlt load creates table if not exists."""
        import duckdb
        import dlt
        from gfn_pipeline.main import gfn_s3_source

        # Create a dlt pipeline pointing to temp DuckDB
        pipeline = dlt.pipeline(
            pipeline_name="test_pipeline",
            destination=dlt.destinations.duckdb(temp_duckdb),
            dataset_name="test_gfn",
        )

        # Test data
        data = {
            "countries": [
                {
                    "country_code": 1,
                    "country_name": "Test Country",
                    "iso_alpha2": "TC",
                    "version": "2024",
                    "extracted_at": datetime.now(timezone.utc).isoformat(),
                    "transformed_at": datetime.now(timezone.utc).isoformat(),
                }
            ],
            "footprint_data": [
                {
                    "country_code": 1,
                    "country_name": "Test Country",
                    "iso_alpha2": "TC",
                    "year": 2024,
                    "record_type": "EFConsTotGHA",
                    "record_type_description": "Ecological Footprint",
                    "value": 400.0,
                    "carbon": 100.0,
                    "score": "A",
                    "extracted_at": datetime.now(timezone.utc).isoformat(),
                    "transformed_at": datetime.now(timezone.utc).isoformat(),
                }
            ],
            "record_types": [],
        }

        # Create source and run pipeline
        source = gfn_s3_source(data=data, enable_contracts=False)
        load_info = pipeline.run(source)

        # Verify data was loaded
        conn = duckdb.connect(temp_duckdb)
        result = conn.execute("SELECT * FROM test_gfn.footprint_data").fetchall()
        conn.close()

        assert len(result) == 1
        assert result[0][0] == 1  # country_code

    def test_dlt_load_upserts(self, temp_duckdb):
        """Test that dlt load performs merge on duplicate keys."""
        import duckdb
        import dlt
        from gfn_pipeline.main import gfn_s3_source

        # Create a dlt pipeline
        pipeline = dlt.pipeline(
            pipeline_name="test_pipeline_upsert",
            destination=dlt.destinations.duckdb(temp_duckdb),
            dataset_name="test_gfn",
        )

        # First load
        data1 = {
            "countries": [],
            "footprint_data": [
                {
                    "country_code": 1,
                    "country_name": "Original Name",
                    "iso_alpha2": "TC",
                    "year": 2024,
                    "record_type": "EFConsTotGHA",
                    "value": 100.0,
                    "carbon": 25.0,
                    "score": "A",
                    "extracted_at": datetime.now(timezone.utc).isoformat(),
                    "transformed_at": datetime.now(timezone.utc).isoformat(),
                }
            ],
            "record_types": [],
        }
        source1 = gfn_s3_source(data=data1, enable_contracts=False)
        pipeline.run(source1)

        # Second load with updated values
        data2 = {
            "countries": [],
            "footprint_data": [
                {
                    "country_code": 1,
                    "country_name": "Updated Name",
                    "iso_alpha2": "TC",
                    "year": 2024,
                    "record_type": "EFConsTotGHA",
                    "value": 200.0,
                    "carbon": 50.0,
                    "score": "B",
                    "extracted_at": datetime.now(timezone.utc).isoformat(),
                    "transformed_at": datetime.now(timezone.utc).isoformat(),
                }
            ],
            "record_types": [],
        }
        source2 = gfn_s3_source(data=data2, enable_contracts=False)
        pipeline.run(source2)

        # Verify upsert worked
        conn = duckdb.connect(temp_duckdb)
        result = conn.execute("SELECT country_name, value FROM test_gfn.footprint_data").fetchall()
        conn.close()

        assert len(result) == 1
        assert result[0][0] == "Updated Name"
        assert result[0][1] == 200.0


# ============================================================================
# Integration Tests (require LocalStack)
# ============================================================================

@pytest.mark.integration
class TestLocalStackIntegration:
    """Integration tests requiring LocalStack."""

    @pytest.fixture(autouse=True)
    def check_localstack(self):
        """Skip if LocalStack is not running."""
        import requests
        try:
            response = requests.get("http://localhost:4566/_localstack/health", timeout=2)
            if response.status_code != 200:
                pytest.skip("LocalStack not healthy")
        except requests.exceptions.ConnectionError:
            pytest.skip("LocalStack not running")

    def test_s3_bucket_exists(self):
        """Test S3 bucket is created."""
        import boto3

        s3 = boto3.client(
            "s3",
            endpoint_url="http://localhost:4566",
            aws_access_key_id="test",
            aws_secret_access_key="test",
            region_name="us-east-1",
        )

        buckets = s3.list_buckets()["Buckets"]
        bucket_names = [b["Name"] for b in buckets]

        assert "gfn-data-lake" in bucket_names

    def test_sqs_queues_exist(self):
        """Test SQS queues are created."""
        import boto3

        sqs = boto3.client(
            "sqs",
            endpoint_url="http://localhost:4566",
            aws_access_key_id="test",
            aws_secret_access_key="test",
            region_name="us-east-1",
        )

        queues = sqs.list_queues()
        queue_urls = queues.get("QueueUrls", [])

        expected_queues = ["gfn-extract-queue", "gfn-transform-queue", "gfn-load-queue"]
        for queue in expected_queues:
            assert any(queue in url for url in queue_urls), f"Queue {queue} not found"

    def test_lambda_functions_exist(self):
        """Test Lambda functions are created."""
        import boto3

        lambda_client = boto3.client(
            "lambda",
            endpoint_url="http://localhost:4566",
            aws_access_key_id="test",
            aws_secret_access_key="test",
            region_name="us-east-1",
        )

        functions = lambda_client.list_functions()["Functions"]
        function_names = [f["FunctionName"] for f in functions]

        expected_functions = ["gfn-extract", "gfn-transform", "gfn-load"]
        for func in expected_functions:
            assert func in function_names, f"Lambda {func} not found"

    def test_step_functions_state_machine_exists(self):
        """Test Step Functions state machine is created."""
        import boto3

        sfn = boto3.client(
            "stepfunctions",
            endpoint_url="http://localhost:4566",
            aws_access_key_id="test",
            aws_secret_access_key="test",
            region_name="us-east-1",
        )

        state_machines = sfn.list_state_machines()["stateMachines"]
        sm_names = [sm["name"] for sm in state_machines]

        assert "gfn-pipeline-orchestrator" in sm_names


@pytest.mark.integration
class TestEndToEndExtraction:
    """End-to-end extraction tests (require API key and LocalStack)."""

    @pytest.fixture(autouse=True)
    def check_prerequisites(self):
        """Skip if prerequisites not met."""
        import requests

        # Check LocalStack
        try:
            response = requests.get("http://localhost:4566/_localstack/health", timeout=2)
            if response.status_code != 200:
                pytest.skip("LocalStack not healthy")
        except requests.exceptions.ConnectionError:
            pytest.skip("LocalStack not running")

        # Check API key
        if not os.getenv("GFN_API_KEY"):
            pytest.skip("GFN_API_KEY not set")

    @pytest.fixture(autouse=True)
    def force_duckdb_destination(self, monkeypatch):
        """Force DuckDB destination by unsetting Snowflake env vars for tests."""
        monkeypatch.delenv("SNOWFLAKE_ACCOUNT", raising=False)
        monkeypatch.delenv("AWS_LAMBDA_FUNCTION_NAME", raising=False)

    def test_extract_single_year(self):
        """Test extracting a single year of data."""
        from infrastructure.lambda_handlers import handler_extract

        result = handler_extract({
            "start_year": 2023,
            "end_year": 2023,
        })

        assert result["status"] == "success"
        assert result["records_count"] > 0
        assert result["s3_key"] is not None

    def test_full_etl_pipeline(self):
        """Test full ETL pipeline through all stages."""
        from infrastructure.lambda_handlers import (
            handler_extract,
            handler_transform,
            handler_load,
        )

        # Extract
        extract_result = handler_extract({
            "start_year": 2023,
            "end_year": 2023,
        })
        assert extract_result["status"] == "success"

        # Transform
        transform_result = handler_transform({
            "s3_bucket": extract_result["s3_bucket"],
            "s3_key": extract_result["s3_key"],
        })
        assert transform_result["status"] == "success"

        # Load
        load_result = handler_load({
            "s3_bucket": transform_result["s3_bucket"],
            "s3_key": transform_result["s3_key"],
        })
        assert load_result["status"] == "success"
        assert load_result["records_loaded"] > 0
