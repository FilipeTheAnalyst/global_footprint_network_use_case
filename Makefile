# =============================================================================
# GFN Pipeline - Makefile
# =============================================================================
# 
# Comprehensive build and deployment commands for the GFN data pipeline.
# Supports LocalStack (AWS simulation), DuckDB (local), and Snowflake (production).
#
# Pipeline Architecture (dlt + S3 Data Lake):
#   Extract (async) → S3 Raw → Soda Checks → S3 Staged → dlt → Destination
#
# S3 Structure:
#   s3://gfn-data-lake/
#   ├── raw/                    # Raw extracted data (immutable audit trail)
#   │   └── gfn_footprint_{YYYYMMDD_HHMMSS}.json
#   ├── staged/                 # Validated data ready for dlt
#   │   └── gfn_footprint_{YYYYMMDD_HHMMSS}_staged.json
#   └── transformed/            # Legacy: for Snowpipe
#       └── gfn_footprint_{YYYYMMDD_HHMMSS}_transformed.json
#
# Quick Start:
#   make setup              # Start LocalStack + setup AWS resources
#   make run                # Run pipeline (DuckDB) with S3 + dlt
#   make run-snowflake      # Run pipeline (Snowflake)
#   make docker-up          # Start all Docker services
# =============================================================================

.PHONY: help setup clean test run docker-up docker-down

# Default target
help:
	@echo "╔══════════════════════════════════════════════════════════════════════════════╗"
	@echo "║                        GFN Pipeline - Available Commands                      ║"
	@echo "╠══════════════════════════════════════════════════════════════════════════════╣"
	@echo "║                                                                               ║"
	@echo "║  DOCKER (Recommended for reproducibility)                                     ║"
	@echo "║  ──────────────────────────────────────────────────────────────────────────── ║"
	@echo "║    make docker-up              Start LocalStack                               ║"
	@echo "║    make docker-up-all          Start all services                             ║"
	@echo "║    make docker-pipeline        Run pipeline in Docker container               ║"
	@echo "║    make docker-down            Stop all services                              ║"
	@echo "║                                                                               ║"
	@echo "║  PIPELINE (dlt + S3 Data Lake)                                                ║"
	@echo "║  ──────────────────────────────────────────────────────────────────────────── ║"
	@echo "║    make run                    Run pipeline (DuckDB + S3 + dlt)               ║"
	@echo "║    make run-snowflake          Run pipeline (Snowflake destination)           ║"
	@echo "║    make run-both               Run pipeline (DuckDB + Snowflake)              ║"
	@echo "║    make run-soda               Run pipeline with Soda checks                  ║"
	@echo "║    make run-contracts          Run pipeline with data contracts               ║"
	@echo "║    make run-production         Production: Snowflake + Soda + contracts       ║"
	@echo "║    make run-full-refresh       Full refresh (replace all data)                ║"
	@echo "║                                                                               ║"
	@echo "║  DIRECT EXTRACTION (no S3)                                                    ║"
	@echo "║  ──────────────────────────────────────────────────────────────────────────── ║"
	@echo "║    make run-direct             Run dlt pipeline directly (no S3)              ║"
	@echo "║    make run-direct-snowflake   Run dlt pipeline to Snowflake (no S3)          ║"
	@echo "║                                                                               ║"
	@echo "║  BACKFILL (Idempotent - safe to re-run)                                       ║"
	@echo "║  ──────────────────────────────────────────────────────────────────────────── ║"
	@echo "║    make backfill-full          Backfill all years (1961-2024)                 ║"
	@echo "║    make backfill-recent        Backfill recent years (2020-2024)              ║"
	@echo "║    make backfill YEARS=2010-2015  Custom year range backfill                  ║"
	@echo "║                                                                               ║"
	@echo "║  LAMBDA (LocalStack)                                                          ║"
	@echo "║  ──────────────────────────────────────────────────────────────────────────── ║"
	@echo "║    make lambda-list            List deployed Lambda functions                 ║"
	@echo "║    make lambda-invoke-extract  Invoke extract Lambda                          ║"
	@echo "║    make lambda-invoke-pipeline Run full Lambda pipeline (E→T→L)               ║"
	@echo "║    make lambda-update          Update Lambda code                             ║"
	@echo "║                                                                               ║"
	@echo "║  SNOWFLAKE                                                                    ║"
	@echo "║  ──────────────────────────────────────────────────────────────────────────── ║"
	@echo "║    make snowflake-setup        Interactive Snowflake setup                    ║"
	@echo "║    make snowflake-verify       Verify Snowflake connection                    ║"
	@echo "║    make load-to-snowflake      Load LocalStack data to Snowflake              ║"
	@echo "║                                                                               ║"
	@echo "║  DATA QUALITY                                                                 ║"
	@echo "║  ──────────────────────────────────────────────────────────────────────────── ║"
	@echo "║    make soda-check             Run Soda data quality checks                   ║"
	@echo "║                                                                               ║"
	@echo "║  DEVELOPMENT                                                                  ║"
	@echo "║  ──────────────────────────────────────────────────────────────────────────── ║"
	@echo "║    make test                   Run tests                                      ║"
	@echo "║    make clean                  Clean generated files                          ║"
	@echo "║    make logs                   View pipeline logs                             ║"
	@echo "║                                                                               ║"
	@echo "╚══════════════════════════════════════════════════════════════════════════════╝"

# =============================================================================
# DOCKER COMMANDS
# =============================================================================

docker-up:
	@echo "Starting LocalStack..."
	docker compose up -d localstack
	@echo "Waiting for LocalStack to be ready..."
	@sleep 5
	@curl -s http://localhost:4566/_localstack/health | python -m json.tool || true
	@echo "\n✓ LocalStack is ready at http://localhost:4566"

docker-up-all:
	@echo "Starting all services..."
	docker compose --profile pipeline --profile ui up -d

docker-pipeline:
	@echo "Running pipeline in Docker..."
	docker compose --profile pipeline run --rm pipeline

docker-pipeline-snowflake:
	@echo "Running pipeline in Docker (Snowflake destination)..."
	docker compose --profile pipeline run --rm -e PIPELINE_DESTINATION=snowflake pipeline

docker-pipeline-both:
	@echo "Running pipeline in Docker (DuckDB + Snowflake)..."
	docker compose --profile pipeline run --rm -e PIPELINE_DESTINATION=both pipeline

docker-down:
	docker compose --profile monitoring --profile pipeline --profile ui down

docker-logs:
	docker compose logs -f

docker-build:
	docker compose build

# =============================================================================
# LOCAL DEVELOPMENT
# =============================================================================

install:
	@echo "Installing dependencies..."
	uv sync
	@echo "✓ Dependencies installed"

localstack-up: docker-up

localstack-down:
	docker compose down localstack

setup: docker-up
	@echo "Setting up AWS resources in LocalStack..."
	uv run python -m infrastructure.setup_localstack
	@echo "\n✓ Setup complete!"

# =============================================================================
# PIPELINE EXECUTION (dlt + S3 Data Lake)
# =============================================================================

# Main pipeline command - uses dlt + S3 architecture
run:
	@echo "Running pipeline (DuckDB + S3 + dlt)..."
	uv run python -m gfn_pipeline.main --destination duckdb

run-snowflake:
	@echo "Running pipeline (Snowflake destination)..."
	uv run python -m gfn_pipeline.main --destination snowflake

run-both:
	@echo "Running pipeline (DuckDB + Snowflake)..."
	uv run python -m gfn_pipeline.main --destination both

run-soda:
	@echo "Running pipeline with Soda data quality checks..."
	uv run python -m gfn_pipeline.main --with-soda

run-soda-warn:
	@echo "Running pipeline with Soda checks (warn only)..."
	uv run python -m gfn_pipeline.main --with-soda --soda-warn-only

run-contracts:
	@echo "Running pipeline with data contracts..."
	uv run python -m gfn_pipeline.main --with-contracts

run-full-refresh:
	@echo "Running pipeline (full refresh)..."
	uv run python -m gfn_pipeline.main --full-refresh

run-production:
	@echo "Running pipeline (production: Snowflake + Soda + contracts)..."
	uv run python -m gfn_pipeline.main --destination snowflake --with-soda --with-contracts

run-local:
	@echo "Running pipeline without S3 storage..."
	uv run python -m gfn_pipeline.main --destination duckdb --no-s3

# =============================================================================
# DIRECT EXTRACTION (no S3 - uses pipeline_async.py directly)
# =============================================================================

run-direct:
	@echo "Running dlt pipeline directly (no S3)..."
	uv run python -m gfn_pipeline.pipeline_async

run-direct-snowflake:
	@echo "Running dlt pipeline to Snowflake (no S3)..."
	uv run python -m gfn_pipeline.pipeline_async --destination snowflake

run-direct-full-refresh:
	@echo "Running dlt pipeline with full refresh (no S3)..."
	uv run python -m gfn_pipeline.pipeline_async --full-refresh

# =============================================================================
# BACKFILL COMMANDS (Idempotent - safe to re-run)
# =============================================================================

# Full backfill: 1961-2024 (all available years)
backfill-full:
	@echo "Running full backfill (1961-2024)..."
	@echo "This is idempotent - existing records will be updated, not duplicated."
	uv run python -m gfn_pipeline.main --start-year 1961 --end-year 2024 --full-refresh

# Recent backfill: 2020-2024
backfill-recent:
	@echo "Running recent backfill (2020-2024)..."
	uv run python -m gfn_pipeline.main --start-year 2020 --end-year 2024

# Custom backfill: make backfill YEARS=2010-2015
backfill:
	@if [ -z "$(YEARS)" ]; then \
		echo "Usage: make backfill YEARS=2010-2015"; \
		exit 1; \
	fi
	@START_YEAR=$$(echo $(YEARS) | cut -d'-' -f1); \
	END_YEAR=$$(echo $(YEARS) | cut -d'-' -f2); \
	echo "Running backfill ($$START_YEAR-$$END_YEAR)..."; \
	uv run python -m gfn_pipeline.main --start-year $$START_YEAR --end-year $$END_YEAR

# Direct backfill (no S3)
backfill-direct-full:
	@echo "Running full direct backfill (1961-2024, no S3)..."
	uv run python -m gfn_pipeline.pipeline_async --start-year 1961 --end-year 2024

backfill-direct-recent:
	@echo "Running recent direct backfill (2020-2024, no S3)..."
	uv run python -m gfn_pipeline.pipeline_async --start-year 2020 --end-year 2024

# Lambda-based backfill (LocalStack)
lambda-backfill-full:
	@echo "Running full Lambda backfill (1961-2024)..."
	uv run python -m infrastructure.lambda_handlers extract --start-year 1961 --end-year 2024

lambda-backfill-recent:
	@echo "Running recent Lambda backfill (2020-2024)..."
	uv run python -m infrastructure.lambda_handlers extract --start-year 2020 --end-year 2024

# =============================================================================
# LAMBDA COMMANDS (LocalStack)
# =============================================================================

lambda-list:
	uv run awslocal lambda list-functions --query 'Functions[*].[FunctionName,Handler,Runtime]' --output table

lambda-invoke-extract:
	@echo "Invoking extract Lambda..."
	uv run awslocal lambda invoke --function-name gfn-extract \
		--payload '{"start_year": 2023, "end_year": 2024}' /tmp/extract_output.json
	@cat /tmp/extract_output.json | python -m json.tool

lambda-invoke-transform:
	@echo "Invoking transform Lambda..."
	@if [ -z "$(S3_KEY)" ]; then \
		echo "Error: S3_KEY required. Usage: make lambda-invoke-transform S3_KEY=raw/gfn_footprint_...json"; \
		exit 1; \
	fi
	uv run awslocal lambda invoke --function-name gfn-transform \
		--payload '{"s3_key": "$(S3_KEY)"}' /tmp/transform_output.json
	@cat /tmp/transform_output.json | python -m json.tool

lambda-invoke-load:
	@echo "Invoking load Lambda..."
	@if [ -z "$(S3_KEY)" ]; then \
		echo "Error: S3_KEY required. Usage: make lambda-invoke-load S3_KEY=transformed/gfn_footprint_...json"; \
		exit 1; \
	fi
	uv run awslocal lambda invoke --function-name gfn-load \
		--payload '{"s3_key": "$(S3_KEY)"}' /tmp/load_output.json
	@cat /tmp/load_output.json | python -m json.tool

lambda-invoke-pipeline:
	@echo "Running full Lambda pipeline (Extract → Transform → Load)..."
	@uv run python -c "\
import subprocess, json, sys; \
print('Step 1: Extract...'); \
r1 = subprocess.run(['uv', 'run', 'awslocal', 'lambda', 'invoke', '--function-name', 'gfn-extract', '--payload', '{\"start_year\": 2023, \"end_year\": 2024}', '/tmp/e.json'], capture_output=True); \
e = json.load(open('/tmp/e.json')); \
print(f'  → Extracted {e.get(\"records_count\", 0)} records to {e.get(\"s3_key\", \"N/A\")}'); \
if not e.get('s3_key'): print('  ✗ Extract failed'); sys.exit(1); \
print('Step 2: Transform...'); \
r2 = subprocess.run(['uv', 'run', 'awslocal', 'lambda', 'invoke', '--function-name', 'gfn-transform', '--payload', json.dumps({'s3_key': e['s3_key']}), '/tmp/t.json'], capture_output=True); \
t = json.load(open('/tmp/t.json')); \
print(f'  → Transformed {t.get(\"records_count\", 0)} records to {t.get(\"s3_key\", \"N/A\")}'); \
if not t.get('s3_key'): print('  ✗ Transform failed'); sys.exit(1); \
print('Step 3: Load...'); \
r3 = subprocess.run(['uv', 'run', 'awslocal', 'lambda', 'invoke', '--function-name', 'gfn-load', '--payload', json.dumps({'s3_key': t['s3_key']}), '/tmp/l.json'], capture_output=True); \
l = json.load(open('/tmp/l.json')); \
print(f'  → Loaded {l.get(\"records_loaded\", 0)} records to {l.get(\"destination\", \"N/A\")}'); \
print('\\n✓ Pipeline complete!'); \
"

lambda-update:
	@echo "Updating Lambda functions with latest code..."
	uv run python -c "\
from infrastructure.setup_localstack import create_lambda_package, get_client, LAMBDA_FUNCTIONS; \
import shutil, os; \
zip_path = create_lambda_package(include_dependencies=True); \
lc = get_client('lambda'); \
zc = open(zip_path, 'rb').read(); \
[print(f'✓ Updated {fn}') or lc.update_function_code(FunctionName=fn, ZipFile=zc) for fn in LAMBDA_FUNCTIONS]; \
shutil.rmtree(os.path.dirname(zip_path), ignore_errors=True); \
"

# =============================================================================
# SNOWFLAKE
# =============================================================================

snowflake-setup:
	@echo "Starting Snowflake production setup..."
	uv run python -m infrastructure.setup_snowflake_production

snowflake-setup-localstack:
	@echo "Setting up AWS resources in LocalStack for Snowflake integration..."
	uv run python -m infrastructure.setup_snowflake_production --step aws --use-localstack

snowflake-setup-aws:
	@echo "Setting up AWS infrastructure for Snowflake..."
	uv run python -m infrastructure.setup_snowflake_production --step aws

snowflake-setup-sf:
	@echo "Setting up Snowflake objects..."
	uv run python -m infrastructure.setup_snowflake_production --step snowflake

snowflake-verify:
	@echo "Verifying Snowflake setup..."
	uv run python -m infrastructure.setup_snowflake_production --step verify

load-to-snowflake:
	@echo "Loading data from LocalStack to Snowflake..."
	uv run python -m infrastructure.load_to_snowflake

load-to-snowflake-all:
	@echo "Loading ALL data from LocalStack to Snowflake..."
	uv run python -m infrastructure.load_to_snowflake --all

snowpipe-check:
	@echo "Checking Snowpipe status..."
	@uv run python -c "\
import snowflake.connector, os, json; \
conn = snowflake.connector.connect( \
    account=os.getenv('SNOWFLAKE_ACCOUNT'), \
    user=os.getenv('SNOWFLAKE_USER'), \
    password=os.getenv('SNOWFLAKE_PASSWORD'), \
    warehouse=os.getenv('SNOWFLAKE_WAREHOUSE', 'COMPUTE_WH')); \
cur = conn.cursor(); \
cur.execute(\"SELECT SYSTEM\$$PIPE_STATUS('GFN.RAW.GFN_FOOTPRINT_PIPE')\"); \
print(json.dumps(json.loads(cur.fetchone()[0]), indent=2)); \
conn.close()"

# =============================================================================
# DATA QUALITY (Soda)
# =============================================================================

soda-check:
	@echo "Running Soda data quality checks..."
	uv run soda scan -d snowflake -c soda/configuration.yml soda/checks.yml
	@echo "\n✓ Soda checks complete"

# =============================================================================
# AWS CLI SHORTCUTS (LocalStack)
# =============================================================================

aws-s3-ls:
	@uv run python -c "import boto3; s3 = boto3.client('s3', endpoint_url='http://localhost:4566', region_name='us-east-1', aws_access_key_id='test', aws_secret_access_key='test'); response = s3.list_objects_v2(Bucket='gfn-data-lake'); print('S3 Contents:'); [print(f\"  {obj['LastModified'].strftime('%Y-%m-%d %H:%M')} {obj['Size']:>10} {obj['Key']}\") for obj in response.get('Contents', [])]"

aws-s3-raw:
	@uv run python -c "import boto3; s3 = boto3.client('s3', endpoint_url='http://localhost:4566', region_name='us-east-1', aws_access_key_id='test', aws_secret_access_key='test'); response = s3.list_objects_v2(Bucket='gfn-data-lake', Prefix='raw/'); print('Raw files:'); [print(f\"  {obj['LastModified'].strftime('%Y-%m-%d %H:%M')} {obj['Size']:>10} {obj['Key']}\") for obj in response.get('Contents', [])]"

aws-s3-staged:
	@uv run python -c "import boto3; s3 = boto3.client('s3', endpoint_url='http://localhost:4566', region_name='us-east-1', aws_access_key_id='test', aws_secret_access_key='test'); response = s3.list_objects_v2(Bucket='gfn-data-lake', Prefix='staged/'); print('Staged files:'); [print(f\"  {obj['LastModified'].strftime('%Y-%m-%d %H:%M')} {obj['Size']:>10} {obj['Key']}\") for obj in response.get('Contents', [])]"

aws-s3-transformed:
	@uv run python -c "import boto3; s3 = boto3.client('s3', endpoint_url='http://localhost:4566', region_name='us-east-1', aws_access_key_id='test', aws_secret_access_key='test'); response = s3.list_objects_v2(Bucket='gfn-data-lake', Prefix='transformed/'); print('Transformed files:'); [print(f\"  {obj['LastModified'].strftime('%Y-%m-%d %H:%M')} {obj['Size']:>10} {obj['Key']}\") for obj in response.get('Contents', [])]"

aws-sqs-ls:
	@uv run python -c "import boto3; sqs = boto3.client('sqs', endpoint_url='http://localhost:4566', region_name='us-east-1', aws_access_key_id='test', aws_secret_access_key='test'); response = sqs.list_queues(); print('SQS Queues:'); [print(f'  {url}') for url in response.get('QueueUrls', [])]"

aws-sqs-dlq:
	@echo "Checking DLQ for failed messages..."
	@uv run python -c "import boto3; sqs = boto3.client('sqs', endpoint_url='http://localhost:4566', region_name='us-east-1', aws_access_key_id='test', aws_secret_access_key='test'); response = sqs.receive_message(QueueUrl='http://localhost:4566/000000000000/gfn-dlq', MaxNumberOfMessages=10); msgs = response.get('Messages', []); print(f'DLQ Messages: {len(msgs)}')"

aws-logs:
	@uv run python -c "import boto3; logs = boto3.client('logs', endpoint_url='http://localhost:4566', region_name='us-east-1', aws_access_key_id='test', aws_secret_access_key='test'); response = logs.describe_log_groups(); print('Log Groups:'); [print(f\"  {g['logGroupName']}\") for g in response.get('logGroups', [])]"

aws-events:
	@uv run python -c "import boto3; events = boto3.client('events', endpoint_url='http://localhost:4566', region_name='us-east-1', aws_access_key_id='test', aws_secret_access_key='test'); response = events.list_rules(); print('EventBridge Rules:'); [print(f\"  {r['Name']}: {r.get('ScheduleExpression', 'N/A')}\") for r in response.get('Rules', [])]"

# =============================================================================
# DUCKDB
# =============================================================================

duckdb-query:
	@echo "Querying DuckDB..."
	uv run python -c "import duckdb; conn = duckdb.connect('gfn_duckdb.duckdb'); print(conn.execute('SELECT COUNT(*) as records, COUNT(DISTINCT country_code) as countries FROM gfn.footprint_data').fetchdf())"

duckdb-summary:
	@uv run python -c "\
import duckdb; \
conn = duckdb.connect('gfn_duckdb.duckdb', read_only=True); \
print('\\n=== DuckDB Summary ==='); \
r = conn.execute('SELECT COUNT(*) as records, COUNT(DISTINCT country_code) as countries, COUNT(DISTINCT year) as years, MIN(year) as min_year, MAX(year) as max_year FROM gfn.footprint_data').fetchone(); \
print(f'Records: {r[0]}'); \
print(f'Countries: {r[1]}'); \
print(f'Years: {r[3]}-{r[4]} ({r[2]} unique)'); \
"

duckdb-top-emitters:
	@uv run python -c "\
import duckdb; \
conn = duckdb.connect('gfn_duckdb.duckdb', read_only=True); \
print(conn.execute('SELECT country_name, year, ROUND(value/1e6, 2) as value_million FROM gfn.footprint_data WHERE year = 2023 AND record_type = \\'EFConsTotGHA\\' ORDER BY value DESC NULLS LAST LIMIT 10').fetchdf().to_string()); \
"

# =============================================================================
# DEVELOPMENT
# =============================================================================

test:
	uv run pytest tests/ -v

test-coverage:
	uv run pytest tests/ -v --cov=src --cov-report=html

lint:
	uv run ruff check src/ infrastructure/

format:
	uv run ruff format src/ infrastructure/

logs:
	@if [ -d "logs" ]; then \
		tail -f logs/pipeline_*.log; \
	else \
		echo "No logs directory found"; \
	fi

clean:
	rm -rf localstack-data/
	rm -rf data/raw/*.json
	rm -rf data/transformed/
	rm -f *.duckdb *.duckdb.wal
	rm -rf __pycache__ **/__pycache__
	rm -rf .pytest_cache
	rm -rf logs/*.log
	rm -rf .ruff_cache
	@echo "✓ Cleaned generated files"

clean-docker:
	docker compose down -v --remove-orphans
	docker system prune -f
	@echo "✓ Cleaned Docker resources"

# =============================================================================
# SYNC LOCALSTACK TO REAL AWS
# =============================================================================

sync-to-aws:
	@echo "Syncing transformed data from LocalStack to real AWS S3..."
	@echo "Downloading from LocalStack..."
	@mkdir -p /tmp/gfn-sync
	@uv run awslocal s3 sync s3://gfn-data-lake/transformed/ /tmp/gfn-sync/transformed/
	@echo "Uploading to real AWS S3..."
	@aws s3 sync /tmp/gfn-sync/transformed/ s3://${S3_BUCKET}/transformed/
	@rm -rf /tmp/gfn-sync
	@echo "✓ Sync complete. Snowpipe should pick up new files."
