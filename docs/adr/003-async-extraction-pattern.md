# ADR-003: Async Extraction Pattern

## Status
Accepted

## Date
2024-01-16

## Context

The GFN API requires multiple calls to fetch all data:
- ~270 countries
- ~17 record types
- ~60 years of data

Sequential extraction would be too slow. We need a concurrent approach.

### Options Considered
1. **Threading** - `concurrent.futures.ThreadPoolExecutor`
2. **Async/Await** - `asyncio` with `aiohttp`
3. **Multiprocessing** - `concurrent.futures.ProcessPoolExecutor`

## Decision

**Use async/await with `aiohttp` and `asyncio.Semaphore` for rate limiting.**

### Rationale

1. **I/O Bound**: API calls are I/O bound, not CPU bound → async is ideal
2. **Rate Limiting**: `asyncio.Semaphore` provides elegant rate limiting
3. **Memory Efficient**: Single thread, no process overhead
4. **Modern Python**: Industry standard for concurrent I/O

### Implementation Pattern

```python
async def extract_with_rate_limit(
    countries: list[int],
    max_concurrent: int = 10
):
    semaphore = asyncio.Semaphore(max_concurrent)
    
    async def fetch_with_limit(country_code: int):
        async with semaphore:
            return await fetch_country_data(country_code)
    
    tasks = [fetch_with_limit(c) for c in countries]
    return await asyncio.gather(*tasks, return_exceptions=True)
```

## Configuration

| Parameter | Value | Rationale |
|-----------|-------|-----------|
| `max_concurrent` | 10 | GFN API rate limit |
| `timeout` | 30s | Prevent hanging requests |
| `retry_attempts` | 3 | Handle transient failures |

## Consequences

### Positive
- 10x faster extraction vs sequential
- Clean rate limiting implementation
- Easy to adjust concurrency
- Good error isolation with `return_exceptions=True`

### Negative
- Async code can be harder to debug
- Stack traces less intuitive
- Requires async-compatible libraries

## Performance Results

| Approach | Time for Full Extract |
|----------|----------------------|
| Sequential | ~45 minutes |
| Async (10 concurrent) | ~4.5 minutes |

**10x improvement achieved.**
