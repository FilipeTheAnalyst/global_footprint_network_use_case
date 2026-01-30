"""
GFN Carbon Footprint Pipeline - UNIFIED VERSION.

Combines the best of both approaches:
- dlt: Schema contracts, state management, pluggable destinations
- Async parallelism: Fast extraction with bulk API endpoint

Performance: ~60-80s full extraction with all dlt benefits.
"""
from __future__ import annotations

import asyncio
import os
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Iterator

import aiohttp
import dlt
from dlt.sources.helpers import requests
from dotenv import load_dotenv

load_dotenv()


# ============================================================================
# Configuration
# ============================================================================

@dataclass
class ExtractionConfig:
    """Extraction configuration."""
    api_key: str = field(default_factory=lambda: os.getenv("GFN_API_KEY"))
    api_base_url: str = "https://api.footprintnetwork.org/v1"
    max_concurrent_requests: int = 8
    requests_per_second: float = 5.0
    request_timeout: int = 30
    
    def __post_init__(self):
        if not self.api_key:
            raise ValueError(
                "API key required. Set GFN_API_KEY environment variable."
            )


# ============================================================================
# Async Rate Limiter
# ============================================================================

class TokenBucketRateLimiter:
    """Token bucket for smooth rate limiting."""
    
    def __init__(self, rate: float, burst: int = 10):
        self.rate = rate
        self.burst = burst
        self.tokens = burst
        self.last_update = time.monotonic()
        self._lock = asyncio.Lock()
    
    async def acquire(self):
        async with self._lock:
            now = time.monotonic()
            elapsed = now - self.last_update
            self.tokens = min(self.burst, self.tokens + elapsed * self.rate)
            self.last_update = now
            
            if self.tokens >= 1:
                self.tokens -= 1
                return
            
            wait_time = (1 - self.tokens) / self.rate
            await asyncio.sleep(wait_time)
            self.tokens = 0


# ============================================================================
# Async Extraction (Parallel)
# ============================================================================

async def fetch_country_data(
    session: aiohttp.ClientSession,
    auth: aiohttp.BasicAuth,
    semaphore: asyncio.Semaphore,
    rate_limiter: TokenBucketRateLimiter,
    base_url: str,
    country: dict,
    start_year: int,
    end_year: int,
) -> list[dict]:
    """Fetch all years for a country using bulk endpoint."""
    country_code = country.get("countryCode")
    if not country_code or not str(country_code).isdigit():
        return []
    
    async with semaphore:
        await rate_limiter.acquire()
        
        url = f"{base_url}/data/{country_code}/all/EFCtot"
        
        for attempt in range(3):
            try:
                async with session.get(url, auth=auth) as resp:
                    if resp.status == 429:
                        retry_after = int(resp.headers.get("Retry-After", 3))
                        await asyncio.sleep(retry_after)
                        continue
                    
                    if resp.status != 200:
                        return []
                    
                    data = await resp.json()
                    records = data if isinstance(data, list) else [data]
                    
                    extracted_at = datetime.now(timezone.utc).isoformat()
                    
                    return [
                        {
                            "country_code": r.get("countryCode"),
                            "country_name": r.get("countryName"),
                            "iso_alpha2": r.get("isoa2"),
                            "year": r.get("year"),
                            "carbon_footprint_gha": r.get("carbon"),
                            "total_footprint_gha": r.get("value"),
                            "carbon_pct_of_total": (
                                round(r["carbon"] / r["value"] * 100, 2)
                                if r.get("carbon") and r.get("value") else None
                            ),
                            "score": r.get("score"),
                            "extracted_at": extracted_at,
                        }
                        for r in records
                        if r.get("year") and start_year <= r["year"] <= end_year
                    ]
                    
            except (aiohttp.ClientError, asyncio.TimeoutError):
                if attempt < 2:
                    await asyncio.sleep(2 ** attempt)
                    continue
                return []
        
        return []


async def extract_all_parallel(
    config: ExtractionConfig,
    start_year: int,
    end_year: int,
) -> list[dict]:
    """Extract all data using parallel async requests."""
    auth = aiohttp.BasicAuth("user", config.api_key)
    semaphore = asyncio.Semaphore(config.max_concurrent_requests)
    rate_limiter = TokenBucketRateLimiter(
        rate=config.requests_per_second,
        burst=config.max_concurrent_requests
    )
    
    connector = aiohttp.TCPConnector(limit=20, limit_per_host=10)
    timeout = aiohttp.ClientTimeout(total=config.request_timeout)
    
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        # Fetch countries list
        async with session.get(f"{config.api_base_url}/countries", auth=auth) as resp:
            resp.raise_for_status()
            countries = await resp.json()
        
        print(f"Fetching data for {len(countries)} countries...")
        
        # Create tasks for all countries
        tasks = [
            fetch_country_data(
                session, auth, semaphore, rate_limiter,
                config.api_base_url, country, start_year, end_year
            )
            for country in countries
        ]
        
        # Execute with progress tracking
        all_records = []
        completed = 0
        total = len(tasks)
        start_time = time.monotonic()
        
        for coro in asyncio.as_completed(tasks):
            records = await coro
            all_records.extend(records)
            completed += 1
            
            if completed % 50 == 0 or completed == total:
                elapsed = time.monotonic() - start_time
                rate = completed / elapsed if elapsed > 0 else 0
                print(f"  Progress: {completed}/{total} ({len(all_records)} records) - {rate:.1f} req/s")
        
        return all_records


# ============================================================================
# dlt Source & Resource
# ============================================================================

@dlt.source(name="gfn", max_table_nesting=0)
def gfn_source(
    api_key: str = dlt.secrets.value,
    start_year: int = 2010,
    end_year: int = 2024,
    parallel: bool = True,
):
    """Global Footprint Network data source.
    
    Args:
        api_key: GFN API key
        start_year: First year to extract
        end_year: Last year to extract
        parallel: Use async parallel extraction (recommended)
    """
    yield carbon_footprint_resource(api_key, start_year, end_year, parallel)


@dlt.resource(
    name="carbon_footprint",
    write_disposition="merge",
    primary_key=["country_code", "year"],
    columns={
        "country_code": {"data_type": "bigint", "nullable": False},
        "country_name": {"data_type": "text", "nullable": False},
        "iso_alpha2": {"data_type": "text", "nullable": True},
        "year": {"data_type": "bigint", "nullable": False},
        "carbon_footprint_gha": {"data_type": "double", "nullable": True},
        "total_footprint_gha": {"data_type": "double", "nullable": True},
        "carbon_pct_of_total": {"data_type": "double", "nullable": True},
        "score": {"data_type": "text", "nullable": True},
        "extracted_at": {"data_type": "timestamp", "nullable": True},
    },
)
def carbon_footprint_resource(
    api_key: str,
    start_year: int,
    end_year: int,
    parallel: bool = True,
) -> Iterator[dict]:
    """Extract carbon footprint data from GFN API.
    
    Uses parallel async extraction by default for ~15x speedup.
    Falls back to sequential if parallel=False.
    """
    if parallel:
        # Use async parallel extraction
        config = ExtractionConfig(api_key=api_key)
        records = asyncio.run(extract_all_parallel(config, start_year, end_year))
        print(f"Extracted {len(records)} records")
        yield from records
    else:
        # Sequential fallback (for debugging or rate limit issues)
        yield from _extract_sequential(api_key, start_year, end_year)


def _extract_sequential(api_key: str, start_year: int, end_year: int) -> Iterator[dict]:
    """Sequential extraction fallback."""
    base_url = "https://api.footprintnetwork.org/v1"
    session = requests.Session()
    auth = ("user", api_key)
    
    countries = session.get(f"{base_url}/countries", auth=auth, timeout=30).json()
    print(f"Fetching data for {len(countries)} countries (sequential mode)...")
    
    for country in countries:
        country_code = country.get("countryCode")
        if not country_code or not str(country_code).isdigit():
            continue
        
        time.sleep(0.3)  # Rate limit
        
        try:
            resp = session.get(
                f"{base_url}/data/{country_code}/all/EFCtot",
                auth=auth,
                timeout=30,
            )
            if resp.status_code != 200:
                continue
            
            data = resp.json()
            records = data if isinstance(data, list) else [data]
            extracted_at = datetime.now(timezone.utc).isoformat()
            
            for r in records:
                if r.get("year") and start_year <= r["year"] <= end_year:
                    yield {
                        "country_code": r.get("countryCode"),
                        "country_name": r.get("countryName"),
                        "iso_alpha2": r.get("isoa2"),
                        "year": r.get("year"),
                        "carbon_footprint_gha": r.get("carbon"),
                        "total_footprint_gha": r.get("value"),
                        "carbon_pct_of_total": (
                            round(r["carbon"] / r["value"] * 100, 2)
                            if r.get("carbon") and r.get("value") else None
                        ),
                        "score": r.get("score"),
                        "extracted_at": extracted_at,
                    }
        except Exception as e:
            print(f"  Warning: {country_code}: {e}")
            continue


# ============================================================================
# Pipeline Runner
# ============================================================================

def run_pipeline(
    destination: str = "duckdb",
    start_year: int = 2010,
    end_year: int = 2024,
    api_key: str | None = None,
    parallel: bool = True,
    full_refresh: bool = False,
) -> dlt.Pipeline:
    """Run the GFN pipeline.
    
    Args:
        destination: Target destination ("duckdb", "snowflake", "filesystem")
        start_year: First year to extract
        end_year: Last year to extract
        api_key: GFN API key (or set GFN_API_KEY env var)
        parallel: Use parallel extraction (default: True)
        full_refresh: Replace all data instead of merge
    
    Returns:
        dlt.Pipeline instance with load info
    """
    api_key = api_key or os.getenv("GFN_API_KEY")
    if not api_key:
        raise ValueError("API key required. Set GFN_API_KEY environment variable.")
    
    pipeline = dlt.pipeline(
        pipeline_name="gfn_carbon_footprint",
        destination=destination,
        dataset_name="gfn",
    )
    
    # Override write disposition for full refresh
    source = gfn_source(
        api_key=api_key,
        start_year=start_year,
        end_year=end_year,
        parallel=parallel,
    )
    
    if full_refresh:
        source.carbon_footprint.apply_hints(write_disposition="replace")
    
    print(f"\n{'='*60}")
    print(f"GFN Pipeline - {'Parallel' if parallel else 'Sequential'} Mode")
    print(f"{'='*60}")
    print(f"Destination: {destination}")
    print(f"Years: {start_year}-{end_year}")
    print(f"Mode: {'Full Refresh' if full_refresh else 'Incremental Merge'}")
    print(f"{'='*60}\n")
    
    start_time = time.monotonic()
    load_info = pipeline.run(source)
    elapsed = time.monotonic() - start_time
    
    print(f"\n{'='*60}")
    print(f"COMPLETE in {elapsed:.1f}s")
    print(f"{'='*60}")
    print(load_info)
    print(f"\nPipeline state: {pipeline.pipelines_dir}")
    
    return pipeline


# ============================================================================
# CLI
# ============================================================================

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(
        description="GFN Carbon Footprint Pipeline (Unified)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Fast parallel extraction to DuckDB (default)
  python -m gfn_pipeline.pipeline_unified
  
  # Load to Snowflake
  python -m gfn_pipeline.pipeline_unified --destination snowflake
  
  # Full refresh (replace all data)
  python -m gfn_pipeline.pipeline_unified --full-refresh
  
  # Sequential mode (for debugging)
  python -m gfn_pipeline.pipeline_unified --no-parallel
        """,
    )
    parser.add_argument(
        "--destination", "-d",
        choices=["duckdb", "snowflake", "filesystem", "bigquery"],
        default="duckdb",
        help="Target destination",
    )
    parser.add_argument("--start-year", type=int, default=2010)
    parser.add_argument("--end-year", type=int, default=2024)
    parser.add_argument(
        "--full-refresh",
        action="store_true",
        help="Replace all data instead of merge",
    )
    parser.add_argument(
        "--no-parallel",
        action="store_true",
        help="Use sequential extraction (slower)",
    )
    args = parser.parse_args()
    
    run_pipeline(
        destination=args.destination,
        start_year=args.start_year,
        end_year=args.end_year,
        parallel=not args.no_parallel,
        full_refresh=args.full_refresh,
    )
