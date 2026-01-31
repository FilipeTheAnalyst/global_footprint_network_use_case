# =============================================================================
# GFN Pipeline - Main Dockerfile
# =============================================================================
# Multi-stage build for the GFN data pipeline
# Supports: DuckDB (local), Snowflake (production), LocalStack (AWS simulation)
#
# Build:
#   docker build -t gfn-pipeline .
#
# Run:
#   docker run -e GFN_API_KEY=xxx -e PIPELINE_DESTINATION=duckdb gfn-pipeline
# =============================================================================

# -----------------------------------------------------------------------------
# Stage 1: Builder
# -----------------------------------------------------------------------------
FROM python:3.11-slim AS builder

WORKDIR /build

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install uv for fast dependency management
RUN pip install uv

# Copy dependency files
COPY pyproject.toml uv.lock ./

# Install dependencies
RUN uv pip install --system --no-cache -r pyproject.toml

# -----------------------------------------------------------------------------
# Stage 2: Runtime
# -----------------------------------------------------------------------------
FROM python:3.11-slim AS runtime

WORKDIR /app

# Install runtime dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy installed packages from builder
COPY --from=builder /usr/local/lib/python3.11/site-packages /usr/local/lib/python3.11/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin

# Copy application code
COPY src/ ./src/
COPY infrastructure/ ./infrastructure/

# Create directories
RUN mkdir -p /data /app/logs

# Set Python path
ENV PYTHONPATH=/app/src:/app
ENV PYTHONUNBUFFERED=1

# Default environment variables
ENV PIPELINE_DESTINATION=duckdb
ENV DUCKDB_PATH=/data/gfn.duckdb
ENV LOG_LEVEL=INFO

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import sys; sys.exit(0)"

# Default command: run the pipeline
ENTRYPOINT ["python", "-m", "gfn_pipeline.main"]
CMD ["--destination", "duckdb"]
