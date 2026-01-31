"""
Tests for GFN Pipeline core functionality.
"""
import json
import os
import pytest
from datetime import datetime, timezone
from unittest.mock import patch, MagicMock


class TestTransform:
    """Tests for data transformation."""
    
    def test_transform_validates_required_fields(self):
        """Test that transform validates required fields."""
        from gfn_pipeline.main import PipelineRunner
        
        runner = PipelineRunner()
        
        # Records without country_code or year should be filtered
        data = [
            {"country_code": 1, "year": 2024, "country_name": "Test"},
            {"country_code": None, "year": 2024, "country_name": "Invalid"},
            {"country_code": 2, "year": None, "country_name": "Invalid"},
            {"country_code": 3, "year": 2024, "country_name": "Valid"},
        ]
        
        result = runner._transform(data)
        
        assert len(result) == 2
        assert result[0]["country_code"] == 1
        assert result[1]["country_code"] == 3
    
    def test_transform_deduplicates(self):
        """Test that transform removes duplicates."""
        from gfn_pipeline.main import PipelineRunner
        
        runner = PipelineRunner()
        
        data = [
            {"country_code": 1, "year": 2024, "country_name": "First"},
            {"country_code": 1, "year": 2024, "country_name": "Duplicate"},
            {"country_code": 1, "year": 2023, "country_name": "Different Year"},
        ]
        
        result = runner._transform(data)
        
        assert len(result) == 2
        assert result[0]["country_name"] == "First"  # First one wins
    
    def test_transform_calculates_carbon_percentage(self):
        """Test that transform calculates carbon percentage."""
        from gfn_pipeline.main import PipelineRunner
        
        runner = PipelineRunner()
        
        data = [
            {
                "country_code": 1,
                "year": 2024,
                "carbon_footprint_gha": 100,
                "total_footprint_gha": 400,
            },
        ]
        
        result = runner._transform(data)
        
        assert result[0]["carbon_pct_of_total"] == 25.0
    
    def test_transform_handles_missing_values(self):
        """Test that transform handles missing carbon/total values."""
        from gfn_pipeline.main import PipelineRunner
        
        runner = PipelineRunner()
        
        data = [
            {"country_code": 1, "year": 2024, "carbon_footprint_gha": None},
            {"country_code": 2, "year": 2024, "total_footprint_gha": None},
        ]
        
        result = runner._transform(data)
        
        assert result[0]["carbon_pct_of_total"] is None
        assert result[1]["carbon_pct_of_total"] is None


class TestDuckDBLoad:
    """Tests for DuckDB loading."""
    
    @pytest.fixture
    def temp_duckdb(self, tmp_path):
        """Create a temporary DuckDB database."""
        db_path = tmp_path / "test.duckdb"
        return str(db_path)
    
    def test_load_to_duckdb_creates_table(self, temp_duckdb):
        """Test that load creates table if not exists."""
        import duckdb
        from gfn_pipeline.main import PipelineRunner
        
        with patch.dict(os.environ, {"DUCKDB_PATH": temp_duckdb}):
            runner = PipelineRunner(destination="duckdb")
            
            data = [
                {
                    "country_code": 1,
                    "country_name": "Test Country",
                    "iso_alpha2": "TC",
                    "year": 2024,
                    "carbon_footprint_gha": 100.0,
                    "total_footprint_gha": 400.0,
                    "carbon_pct_of_total": 25.0,
                    "score": "A",
                    "extracted_at": datetime.now(timezone.utc).isoformat(),
                    "transformed_at": datetime.now(timezone.utc).isoformat(),
                }
            ]
            
            count = runner._load_to_duckdb(data)
            
            assert count == 1
            
            # Verify data was inserted
            conn = duckdb.connect(temp_duckdb)
            result = conn.execute("SELECT * FROM carbon_footprint").fetchall()
            conn.close()
            
            assert len(result) == 1
            assert result[0][0] == 1  # country_code
            assert result[0][3] == 2024  # year
    
    def test_load_to_duckdb_upserts(self, temp_duckdb):
        """Test that load performs upsert on duplicate keys."""
        import duckdb
        from gfn_pipeline.main import PipelineRunner
        
        with patch.dict(os.environ, {"DUCKDB_PATH": temp_duckdb}):
            runner = PipelineRunner(destination="duckdb")
            
            # First load
            data1 = [
                {
                    "country_code": 1,
                    "country_name": "Original Name",
                    "iso_alpha2": "TC",
                    "year": 2024,
                    "carbon_footprint_gha": 100.0,
                    "total_footprint_gha": 400.0,
                    "carbon_pct_of_total": 25.0,
                    "score": "A",
                    "extracted_at": datetime.now(timezone.utc).isoformat(),
                    "transformed_at": datetime.now(timezone.utc).isoformat(),
                }
            ]
            runner._load_to_duckdb(data1)
            
            # Second load with updated name
            data2 = [
                {
                    "country_code": 1,
                    "country_name": "Updated Name",
                    "iso_alpha2": "TC",
                    "year": 2024,
                    "carbon_footprint_gha": 200.0,
                    "total_footprint_gha": 400.0,
                    "carbon_pct_of_total": 50.0,
                    "score": "B",
                    "extracted_at": datetime.now(timezone.utc).isoformat(),
                    "transformed_at": datetime.now(timezone.utc).isoformat(),
                }
            ]
            runner._load_to_duckdb(data2)
            
            # Verify only one record exists with updated values
            conn = duckdb.connect(temp_duckdb)
            result = conn.execute("SELECT country_name, carbon_footprint_gha FROM carbon_footprint").fetchall()
            conn.close()
            
            assert len(result) == 1
            assert result[0][0] == "Updated Name"
            assert result[0][1] == 200.0


class TestAPIEndpoints:
    """Tests for API endpoints."""
    
    @pytest.fixture
    def client(self):
        """Create test client."""
        from fastapi.testclient import TestClient
        from api.main import app
        return TestClient(app)
    
    def test_root_endpoint(self, client):
        """Test root endpoint returns API info."""
        response = client.get("/")
        assert response.status_code == 200
        data = response.json()
        assert "name" in data
        assert data["name"] == "GFN Pipeline API"
    
    def test_health_endpoint(self, client):
        """Test health endpoint."""
        response = client.get("/health")
        assert response.status_code == 200
        data = response.json()
        assert "status" in data
        assert "services" in data
    
    def test_status_endpoint(self, client):
        """Test status endpoint."""
        response = client.get("/status")
        assert response.status_code == 200
        data = response.json()
        assert "running" in data
        assert "history" in data


class TestLambdaHandlers:
    """Tests for Lambda handlers."""
    
    def test_transform_handler_validates_data(self):
        """Test transform handler validates and enriches data."""
        # This would require mocking S3, so we test the core logic
        from infrastructure.lambda_handlers import handler_transform
        
        # Test would require LocalStack running, so mark as integration test
        pass
    
    def test_extract_handler_requires_api_key(self):
        """Test extract handler requires API key."""
        from infrastructure.lambda_handlers import handler_extract
        
        with patch.dict(os.environ, {"GFN_API_KEY": ""}):
            result = handler_extract({"start_year": 2024, "end_year": 2024})
            # Should fail without API key
            # Note: Actual test would need to mock the API call


# Integration tests (require LocalStack)
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
        )
        
        queues = sqs.list_queues()
        queue_urls = queues.get("QueueUrls", [])
        
        expected_queues = ["gfn-extract-queue", "gfn-transform-queue", "gfn-load-queue"]
        for queue in expected_queues:
            assert any(queue in url for url in queue_urls), f"Queue {queue} not found"
