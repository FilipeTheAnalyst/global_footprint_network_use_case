"""
GFN Data Pipeline - COMPREHENSIVE EXTRACTION with Dynamic Type Discovery.

Extracts ALL available data from the Global Footprint Network API:
- Countries (reference data)
- Record types (dynamically discovered from API)
- All footprint, biocapacity, and supplementary data

API Documentation: https://data.footprintnetwork.org/#/api

API Endpoints Used:
- GET /countries - List all countries
- GET /types - List available record types  
- GET /data/all/{year} - Get ALL data for ALL countries for a year (MOST EFFICIENT)

Performance: Uses bulk endpoint for maximum efficiency (~1 API call per year).
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
from dotenv import load_dotenv

load_dotenv()


# ============================================================================
# Fallback Record Types (used if API discovery fails)
# ============================================================================

FALLBACK_RECORD_TYPES = {
    # Core footprint/biocapacity types
    "BiocapPerCap": "Biocapacity per person (gha)",
    "BiocapTotGHA": "Total biocapacity (gha)",
    "EFConsPerCap": "Ecological Footprint of consumption per capita (gha)",
    "EFConsTotGHA": "Total Ecological Footprint of consumption (gha)",
    "EFProdPerCap": "Ecological Footprint of production per capita (gha)",
    "EFProdTotGHA": "Total Ecological Footprint of production (gha)",
    "EFExportsPerCap": "Ecological Footprint of exports per capita (gha)",
    "EFExportsTotGHA": "Total Ecological Footprint of exports (gha)",
    "EFImportsPerCap": "Ecological Footprint of imports per capita (gha)",
    "EFImportsTotGHA": "Total Ecological Footprint of imports (gha)",
    # Area types
    "AreaPerCap": "Area per capita (ha)",
    "AreaTotHA": "Total area (hectares)",
    # Supplementary indicators
    "Earths": "Number of Earths required",
    "Population": "Population",
    "HDI": "Human Development Index",
    "GDP-PPP": "GDP at Purchasing Power Parity",
    "GDP-USD": "GDP in USD",
}


# ============================================================================
# Configuration
# ============================================================================

@dataclass
class ExtractionConfig:
    """Extraction configuration."""
    api_key: str = field(default_factory=lambda: os.getenv("GFN_API_KEY"))
    api_base_url: str = "https://api.footprintnetwork.org/v1"
    max_concurrent_requests: int = 5  # Conservative for bulk endpoints
    requests_per_second: float = 2.0  # Rate limit for bulk requests
    request_timeout: int = 60  # Longer timeout for bulk data
    use_dynamic_types: bool = True
    
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
    
    def __init__(self, rate: float, burst: int = 5):
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
# Dynamic Type Discovery
# ============================================================================

async def fetch_record_types_from_api(
    session: aiohttp.ClientSession,
    auth: aiohttp.BasicAuth,
    base_url: str,
) -> dict[str, dict]:
    """
    Fetch available record types from the /types endpoint.
    
    Returns:
        Dictionary mapping type code to metadata
    """
    try:
        async with session.get(f"{base_url}/types", auth=auth) as resp:
            if resp.status != 200:
                return {}
            
            types_data = await resp.json()
            return {
                t["code"]: {
                    "name": t.get("name", ""),
                    "note": t.get("note", ""),
                    "record": t.get("record", ""),
                }
                for t in types_data
                if t.get("code")
            }
    except Exception as e:
        print(f"Warning: Could not fetch types from API: {e}")
        return {}


async def discover_record_types_from_sample_year(
    session: aiohttp.ClientSession,
    auth: aiohttp.BasicAuth,
    base_url: str,
    sample_year: int = 2020,
) -> set[str]:
    """
    Discover all available record types by fetching data for a sample year.
    
    Uses the efficient /data/all/{year} endpoint.
    """
    try:
        url = f"{base_url}/data/all/{sample_year}"
        async with session.get(url, auth=auth) as resp:
            if resp.status != 200:
                return set()
            
            data = await resp.json()
            if not isinstance(data, list):
                return set()
            
            return {r["record"] for r in data if r.get("record")}
    except Exception as e:
        print(f"Warning: Could not discover types from sample year: {e}")
        return set()


async def get_available_record_types(
    session: aiohttp.ClientSession,
    auth: aiohttp.BasicAuth,
    base_url: str,
) -> dict[str, str]:
    """
    Get all available record types, combining API metadata with discovery.
    """
    # Fetch from /types endpoint (has metadata)
    types_metadata = await fetch_record_types_from_api(session, auth, base_url)
    
    # Discover from sample year (comprehensive list)
    discovered_types = await discover_record_types_from_sample_year(session, auth, base_url)
    
    # Build result
    result = {}
    
    for record_type in discovered_types:
        # Check if we have metadata from /types endpoint
        for code, meta in types_metadata.items():
            if meta.get("record") == record_type:
                result[record_type] = meta.get("name", record_type)
                break
        else:
            result[record_type] = FALLBACK_RECORD_TYPES.get(record_type, record_type)
    
    # Fallback if discovery failed
    if not result:
        if types_metadata:
            for code, meta in types_metadata.items():
                record = meta.get("record", code)
                result[record] = meta.get("name", record)
        else:
            print("Warning: Using fallback record types (API discovery failed)")
            result = FALLBACK_RECORD_TYPES.copy()
    
    return result


# Synchronous wrapper with caching
_cached_record_types: dict[str, str] | None = None


def get_record_types_sync(
    api_key: str, 
    base_url: str = "https://api.footprintnetwork.org/v1"
) -> dict[str, str]:
    """Synchronous wrapper to get record types (with caching)."""
    global _cached_record_types
    
    if _cached_record_types is not None:
        return _cached_record_types
    
    async def _fetch():
        auth = aiohttp.BasicAuth("user", api_key)
        connector = aiohttp.TCPConnector(limit=5)
        timeout = aiohttp.ClientTimeout(total=30)
        
        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
            return await get_available_record_types(session, auth, base_url)
    
    try:
        _cached_record_types = asyncio.run(_fetch())
        return _cached_record_types
    except Exception as e:
        print(f"Warning: Could not fetch record types: {e}")
        return FALLBACK_RECORD_TYPES.copy()


def clear_record_types_cache():
    """Clear the cached record types."""
    global _cached_record_types
    _cached_record_types = None


# ============================================================================
# Async Extraction Functions
# ============================================================================

async def fetch_countries(
    session: aiohttp.ClientSession,
    auth: aiohttp.BasicAuth,
    base_url: str,
) -> list[dict]:
    """Fetch all countries from the API."""
    async with session.get(f"{base_url}/countries", auth=auth) as resp:
        resp.raise_for_status()
        countries = await resp.json()
        
        extracted_at = datetime.now(timezone.utc).isoformat()
        return [
            {
                "country_code": c.get("countryCode"),
                "country_name": c.get("countryName"),
                "iso_alpha2": c.get("isoa2"),
                "version": c.get("version"),
                "extracted_at": extracted_at,
            }
            for c in countries
            if c.get("countryCode") is not None and c.get("countryName")
        ]


async def fetch_year_all_data(
    session: aiohttp.ClientSession,
    auth: aiohttp.BasicAuth,
    rate_limiter: TokenBucketRateLimiter,
    base_url: str,
    year: int,
    record_type_descriptions: dict[str, str],
) -> list[dict]:
    """
    Fetch ALL data for ALL countries for a single year.
    
    Uses the highly efficient /data/all/{year} endpoint which returns
    all countries × all record types in a single API call.
    
    This is ~200x more efficient than fetching per-country.
    """
    await rate_limiter.acquire()
    
    url = f"{base_url}/data/all/{year}"
    
    for attempt in range(3):
        try:
            async with session.get(url, auth=auth) as resp:
                if resp.status == 429:
                    retry_after = int(resp.headers.get("Retry-After", 5))
                    print(f"  Rate limited, waiting {retry_after}s...")
                    await asyncio.sleep(retry_after)
                    continue
                
                if resp.status != 200:
                    print(f"  Warning: Year {year} returned status {resp.status}")
                    return []
                
                data = await resp.json()
                records = data if isinstance(data, list) else [data]
                
                extracted_at = datetime.now(timezone.utc).isoformat()
                
                return [
                    {
                        "country_code": r.get("countryCode"),
                        "country_name": r.get("countryName"),
                        "short_name": r.get("shortName"),
                        "iso_alpha2": r.get("isoa2"),
                        "year": r.get("year"),
                        "record_type": r.get("record"),
                        "record_type_description": record_type_descriptions.get(
                            r.get("record"), r.get("record")
                        ),
                        # Land use breakdown (in global hectares or hectares)
                        "crop_land": r.get("cropLand"),
                        "grazing_land": r.get("grazingLand"),
                        "forest_land": r.get("forestLand"),
                        "fishing_ground": r.get("fishingGround"),
                        "builtup_land": r.get("builtupLand"),
                        "carbon": r.get("carbon"),
                        # Aggregate value
                        "value": r.get("value"),
                        "score": r.get("score"),
                        "extracted_at": extracted_at,
                    }
                    for r in records
                    if r.get("year") and r.get("countryCode")
                ]
                
        except asyncio.TimeoutError:
            print(f"  Timeout for year {year}, attempt {attempt + 1}/3")
            if attempt < 2:
                await asyncio.sleep(2 ** attempt)
                continue
            return []
        except aiohttp.ClientError as e:
            print(f"  Error for year {year}: {e}")
            if attempt < 2:
                await asyncio.sleep(2 ** attempt)
                continue
            return []
    
    return []


async def extract_all_data(
    config: ExtractionConfig,
    start_year: int,
    end_year: int,
    record_types: list[str] | None = None,
) -> dict[str, Any]:
    """
    Extract all data from GFN API using the most efficient approach.
    
    Strategy: Use /data/all/{year} endpoint which returns ALL countries
    and ALL record types for a year in a single API call.
    
    For 15 years of data, this requires only ~17 API calls instead of ~3000+.
    
    Args:
        config: Extraction configuration
        start_year: First year to extract
        end_year: Last year to extract  
        record_types: Optional filter for specific record types (None = all)
    
    Returns:
        Dictionary with keys: 'countries', 'footprint_data', 'record_types'
    """
    auth = aiohttp.BasicAuth("user", config.api_key)
    rate_limiter = TokenBucketRateLimiter(
        rate=config.requests_per_second,
        burst=config.max_concurrent_requests
    )
    
    connector = aiohttp.TCPConnector(limit=10, limit_per_host=5)
    timeout = aiohttp.ClientTimeout(total=config.request_timeout)
    
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        # Step 1: Discover available record types
        print("Discovering available record types...")
        if config.use_dynamic_types:
            available_types = await get_available_record_types(session, auth, config.api_base_url)
        else:
            available_types = FALLBACK_RECORD_TYPES.copy()
        
        print(f"  Found {len(available_types)} record types:")
        for rt in sorted(available_types.keys()):
            print(f"    - {rt}: {available_types[rt]}")
        
        # Step 2: Fetch countries (for reference data)
        print("\nFetching countries...")
        countries = await fetch_countries(session, auth, config.api_base_url)
        print(f"  Found {len(countries)} countries")
        
        # Step 3: Fetch data year by year using bulk endpoint
        years = list(range(start_year, end_year + 1))
        total_years = len(years)
        
        print(f"\nFetching data for {total_years} years ({start_year}-{end_year})...")
        print(f"  Using bulk endpoint: /data/all/{{year}}")
        print(f"  Total API calls needed: {total_years}")
        
        all_records = []
        start_time = time.monotonic()
        
        for i, year in enumerate(years, 1):
            records = await fetch_year_all_data(
                session, auth, rate_limiter,
                config.api_base_url,
                year,
                available_types,
            )
            
            # Filter by record_types if specified
            if record_types:
                records = [r for r in records if r["record_type"] in record_types]
            
            all_records.extend(records)
            
            elapsed = time.monotonic() - start_time
            rate = i / elapsed if elapsed > 0 else 0
            print(f"  Year {year}: {len(records):,} records (total: {len(all_records):,}) - {rate:.1f} years/s")
        
        # Collect unique record types found
        found_types = {r["record_type"] for r in all_records if r.get("record_type")}
        
        elapsed = time.monotonic() - start_time
        print(f"\nExtraction complete in {elapsed:.1f}s")
        print(f"  Total records: {len(all_records):,}")
        print(f"  Record types found: {len(found_types)}")
        print(f"  Countries with data: {len(set(r['country_code'] for r in all_records))}")
        
        return {
            "countries": countries,
            "footprint_data": all_records,
            "record_types": [
                {"record_type": rt, "description": available_types.get(rt, rt)}
                for rt in sorted(found_types)
            ],
            "available_types": available_types,
        }


# ============================================================================
# dlt Source & Resources
# ============================================================================

@dlt.source(name="gfn", max_table_nesting=0)
def gfn_source(
    api_key: str = dlt.secrets.value,
    start_year: int = 2010,
    end_year: int = 2024,
    record_types: list[str] | None = None,
    use_dynamic_types: bool = True,
):
    """
    Global Footprint Network data source.
    
    Args:
        api_key: GFN API key
        start_year: First year to extract
        end_year: Last year to extract
        record_types: Optional list of specific record types to extract (None = all)
        use_dynamic_types: Discover types from API (True) or use fallback (False)
    """
    config = ExtractionConfig(api_key=api_key, use_dynamic_types=use_dynamic_types)
    data = asyncio.run(extract_all_data(config, start_year, end_year, record_types))
    
    yield countries_resource(data["countries"])
    yield record_types_resource(data["record_types"])
    yield footprint_data_resource(data["footprint_data"])


@dlt.resource(
    name="countries",
    write_disposition="replace",
    primary_key=["country_code"],
    columns={
        "country_code": {"data_type": "bigint", "nullable": True},
        "country_name": {"data_type": "text", "nullable": True},
        "iso_alpha2": {"data_type": "text", "nullable": True},
        "version": {"data_type": "text", "nullable": True},
        "extracted_at": {"data_type": "timestamp", "nullable": True},
    },
)
def countries_resource(countries: list[dict]) -> Iterator[dict]:
    """Countries reference data."""
    print(f"Loading {len(countries)} countries...")
    yield from countries


@dlt.resource(
    name="record_types",
    write_disposition="replace",
    primary_key=["record_type"],
    columns={
        "record_type": {"data_type": "text", "nullable": False},
        "description": {"data_type": "text", "nullable": True},
    },
)
def record_types_resource(record_types: list[dict]) -> Iterator[dict]:
    """Record types reference data (dynamically discovered)."""
    print(f"Loading {len(record_types)} record types...")
    yield from record_types


@dlt.resource(
    name="footprint_data",
    write_disposition="merge",
    primary_key=["country_code", "year", "record_type"],
    columns={
        "country_code": {"data_type": "bigint", "nullable": False},
        "country_name": {"data_type": "text", "nullable": False},
        "short_name": {"data_type": "text", "nullable": True},
        "iso_alpha2": {"data_type": "text", "nullable": True},
        "year": {"data_type": "bigint", "nullable": False},
        "record_type": {"data_type": "text", "nullable": False},
        "record_type_description": {"data_type": "text", "nullable": True},
        "crop_land": {"data_type": "double", "nullable": True},
        "grazing_land": {"data_type": "double", "nullable": True},
        "forest_land": {"data_type": "double", "nullable": True},
        "fishing_ground": {"data_type": "double", "nullable": True},
        "builtup_land": {"data_type": "double", "nullable": True},
        "carbon": {"data_type": "double", "nullable": True},
        "value": {"data_type": "double", "nullable": True},
        "score": {"data_type": "text", "nullable": True},
        "extracted_at": {"data_type": "timestamp", "nullable": True},
    },
)
def footprint_data_resource(data: list[dict]) -> Iterator[dict]:
    """All footprint and biocapacity data."""
    print(f"Loading {len(data):,} footprint records...")
    yield from data


# ============================================================================
# Pipeline Runner
# ============================================================================

def run_pipeline(
    destination: str = "duckdb",
    start_year: int = 2010,
    end_year: int = 2024,
    api_key: str | None = None,
    record_types: list[str] | None = None,
    use_dynamic_types: bool = True,
    full_refresh: bool = False,
) -> dlt.Pipeline:
    """
    Run the GFN pipeline.
    
    Args:
        destination: Target destination ("duckdb", "snowflake", "filesystem")
        start_year: First year to extract
        end_year: Last year to extract
        api_key: GFN API key (or set GFN_API_KEY env var)
        record_types: List of specific record types (None = all)
        use_dynamic_types: Discover types from API (default: True)
        full_refresh: Replace all data instead of merge
    
    Returns:
        dlt.Pipeline instance with load info
    """
    api_key = api_key or os.getenv("GFN_API_KEY")
    if not api_key:
        raise ValueError("API key required. Set GFN_API_KEY environment variable.")
    
    pipeline = dlt.pipeline(
        pipeline_name="gfn_footprint",
        destination=destination,
        dataset_name="gfn",
    )
    
    source = gfn_source(
        api_key=api_key,
        start_year=start_year,
        end_year=end_year,
        record_types=record_types,
        use_dynamic_types=use_dynamic_types,
    )
    
    if full_refresh:
        source.footprint_data.apply_hints(write_disposition="replace")
        source.countries.apply_hints(write_disposition="replace")
        source.record_types.apply_hints(write_disposition="replace")
    
    years_count = end_year - start_year + 1
    type_filter = f"{len(record_types)} types" if record_types else "all types"
    
    print(f"\n{'='*70}")
    print(f"GFN Pipeline - Bulk Extraction")
    print(f"{'='*70}")
    print(f"Destination:    {destination}")
    print(f"Years:          {start_year}-{end_year} ({years_count} years)")
    print(f"Record Types:   {type_filter} (dynamic discovery: {use_dynamic_types})")
    print(f"Mode:           {'Full Refresh' if full_refresh else 'Incremental Merge'}")
    print(f"API Calls:      ~{years_count + 2} (bulk endpoint)")
    print(f"{'='*70}\n")
    
    start_time = time.monotonic()
    load_info = pipeline.run(source)
    elapsed = time.monotonic() - start_time
    
    print(f"\n{'='*70}")
    print(f"COMPLETE in {elapsed:.1f}s")
    print(f"{'='*70}")
    print(load_info)
    print(f"\nPipeline state: {pipeline.pipelines_dir}")
    
    return pipeline


# ============================================================================
# CLI
# ============================================================================

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(
        description="GFN Data Pipeline - Bulk Extraction with Dynamic Type Discovery",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Extraction Strategy:
  Uses the efficient /data/all/{year} bulk endpoint which returns ALL
  countries and ALL record types for a year in a single API call.
  
  For 15 years of data, this requires only ~17 API calls instead of ~3000+.

Available Record Types (dynamically discovered):
  Footprint (Consumption):  EFConsTotGHA, EFConsPerCap
  Footprint (Production):   EFProdTotGHA, EFProdPerCap  
  Footprint (Trade):        EFExportsTotGHA, EFImportsTotGHA, etc.
  Biocapacity:              BiocapTotGHA, BiocapPerCap
  Area:                     AreaTotHA, AreaPerCap
  Supplementary:            Population, HDI, GDP-USD, GDP-PPP, Earths

Examples:
  # Extract ALL data (recommended)
  python -m gfn_pipeline.pipeline_async
  
  # List available record types
  python -m gfn_pipeline.pipeline_async --list-types
  
  # Extract specific record types only
  python -m gfn_pipeline.pipeline_async --record-types EFConsTotGHA BiocapTotGHA
  
  # Extract specific year range
  python -m gfn_pipeline.pipeline_async --start-year 2015 --end-year 2020
  
  # Load to Snowflake
  python -m gfn_pipeline.pipeline_async --destination snowflake
  
  # Full refresh (replace all data)
  python -m gfn_pipeline.pipeline_async --full-refresh
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
        "--record-types",
        nargs="+",
        help="Specific record types to extract (default: all)",
    )
    parser.add_argument(
        "--no-dynamic-types",
        action="store_true",
        help="Use fallback types instead of API discovery",
    )
    parser.add_argument(
        "--list-types",
        action="store_true",
        help="List available record types and exit",
    )
    args = parser.parse_args()
    
    # Handle --list-types
    if args.list_types:
        api_key = os.getenv("GFN_API_KEY")
        if not api_key:
            print("Error: GFN_API_KEY environment variable required")
            exit(1)
        
        print("Discovering available record types from API...\n")
        types = get_record_types_sync(api_key)
        
        print(f"Found {len(types)} record types:\n")
        for rt in sorted(types.keys()):
            print(f"  {rt:20} - {types[rt]}")
        exit(0)
    
    run_pipeline(
        destination=args.destination,
        start_year=args.start_year,
        end_year=args.end_year,
        record_types=args.record_types,
        use_dynamic_types=not args.no_dynamic_types,
        full_refresh=args.full_refresh,
    )
