from __future__ import annotations

import time

import requests
from tenacity import (
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)


def _retryable(exc: BaseException) -> bool:
    if isinstance(exc, (requests.Timeout, requests.ConnectionError)):
        return True
    if isinstance(exc, requests.HTTPError) and exc.response is not None:
        # Respect rate limiting and transient gateway errors
        return exc.response.status_code in (429, 500, 502, 503, 504)
    return False


class GFNClient:
    """Thin HTTP client for the Global Footprint Network API.

    Docs: http://data.footprintnetwork.org/#/api

    Auth (per docs): HTTP Basic Auth
      - username: any string
      - password: api_key

    Base URL (per docs): https://api.footprintnetwork.org/v1
    """

    def __init__(self, base_url: str, api_key: str, username: str = "any-user-name", timeout_s: int = 30) -> None:
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.username = username
        self.timeout_s = timeout_s

        self.session = requests.Session()
        self.session.headers.update(
            {
                "Accept": "application/json",
                "User-Agent": "gfn-pipeline/0.1 (+https://api.footprintnetwork.org)",
            }
        )

    @retry(
        reraise=True,
        stop=stop_after_attempt(8),
        wait=wait_exponential(multiplier=1, min=1, max=60),
        retry=retry_if_exception(_retryable),
    )
    def get_json(self, path: str, params: dict | None = None) -> dict | list:
        url = f"{self.base_url}/{path.lstrip('/')}"
        resp = self.session.get(url, params=params or {}, auth=(self.username, self.api_key), timeout=self.timeout_s)

        # Simple, polite rate-limit handling
        if resp.status_code == 429:
            retry_after = resp.headers.get("Retry-After")
            if retry_after:
                try:
                    time.sleep(int(retry_after))
                except ValueError:
                    pass

        resp.raise_for_status()
        return resp.json()
