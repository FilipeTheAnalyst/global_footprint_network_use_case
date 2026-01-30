#!/bin/bash
# LocalStack setup for GFN Pipeline
# Docs: https://docs.localstack.cloud/aws/

set -e

echo "=============================================="
echo "LocalStack Setup for GFN Pipeline"
echo "=============================================="

# Check if LocalStack is installed
if ! command -v localstack &> /dev/null; then
    echo "Installing LocalStack..."
    pip install localstack
fi

# Check if Docker is running
if ! docker info &> /dev/null; then
    echo "ERROR: Docker is not running. Please start Docker first."
    exit 1
fi

# Start LocalStack (if not already running)
if ! curl -s http://localhost:4566/_localstack/health &> /dev/null; then
    echo "Starting LocalStack..."
    localstack start -d
    
    # Wait for LocalStack to be ready
    echo "Waiting for LocalStack to be ready..."
    for i in {1..30}; do
        if curl -s http://localhost:4566/_localstack/health | grep -q '"s3": "running"'; then
            echo "LocalStack is ready!"
            break
        fi
        sleep 1
    done
else
    echo "LocalStack is already running"
fi

# Create S3 bucket for the pipeline
echo ""
echo "Creating S3 bucket..."
aws --endpoint-url=http://localhost:4566 s3 mb s3://gfn-data-lake 2>/dev/null || echo "Bucket already exists"

# List buckets to verify
echo ""
echo "Available buckets:"
aws --endpoint-url=http://localhost:4566 s3 ls

echo ""
echo "=============================================="
echo "LocalStack is ready!"
echo "=============================================="
echo ""
echo "S3 Endpoint: http://localhost:4566"
echo "Bucket: gfn-data-lake"
echo ""
echo "Run the pipeline with:"
echo "  uv run python -m gfn_pipeline.pipeline_parallel --use-localstack"
echo ""
echo "View S3 contents:"
echo "  aws --endpoint-url=http://localhost:4566 s3 ls s3://gfn-data-lake/ --recursive"
