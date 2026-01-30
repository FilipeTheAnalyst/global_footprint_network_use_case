from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any

import pandas as pd


def _stable_hash(d: dict[str, Any]) -> str:
    payload = json.dumps(d, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def normalize_carbon_footprint(raw: list[dict], extracted_at: datetime | None = None) -> pd.DataFrame:
    """Transform raw API responses into a clean carbon footprint dataframe.

    Extracts the 'carbon' field from the Ecological Footprint response.
    Output schema:
      - country_code: int
      - country_name: str
      - iso_alpha2: str
      - year: int
      - carbon_footprint_gha: float (global hectares)
      - total_footprint_gha: float
      - record_hash: str
      - extracted_at: timestamp
    """
    extracted_at = extracted_at or datetime.now(timezone.utc)

    records = []
    for r in raw:
        records.append(
            {
                "country_code": r.get("countryCode"),
                "country_name": r.get("countryName"),
                "iso_alpha2": r.get("isoa2"),
                "year": r.get("year"),
                "carbon_footprint_gha": r.get("carbon"),
                "total_footprint_gha": r.get("value"),
                "score": r.get("score"),
                "record_hash": _stable_hash(r),
                "extracted_at": extracted_at,
            }
        )

    return pd.DataFrame(records)


def normalize_records(raw: Any, source: str, extracted_at: datetime | None = None) -> pd.DataFrame:
    """Generic normalizer for RAW layer (keeps full record as JSON)."""
    extracted_at = extracted_at or datetime.now(timezone.utc)

    if isinstance(raw, dict):
        for k in ("data", "results", "items"):
            if k in raw and isinstance(raw[k], list):
                raw = raw[k]
                break

    if not isinstance(raw, list):
        raw = [raw]

    df = pd.DataFrame({"record": raw})
    df["source"] = source
    df["extracted_at"] = extracted_at
    df["record_hash"] = df["record"].apply(lambda r: _stable_hash(r if isinstance(r, dict) else {"value": r}))

    # Extract common fields
    def get_field(rec: dict, keys: list[str]):
        for k in keys:
            if k in rec:
                return rec[k]
        return None

    countries, years, values, units, metrics = [], [], [], [], []
    for rec in df["record"].tolist():
        if not isinstance(rec, dict):
            countries.append(None)
            years.append(None)
            values.append(None)
            units.append(None)
            metrics.append(None)
            continue
        countries.append(get_field(rec, ["countryName", "country", "name", "shortName"]))
        years.append(get_field(rec, ["year", "Year"]))
        values.append(get_field(rec, ["carbon", "value", "Value"]))
        units.append("gha")
        metrics.append("carbon_footprint")

    df["country"] = countries
    df["year"] = pd.to_numeric(years, errors="coerce")
    df["value"] = pd.to_numeric(values, errors="coerce")
    df["unit"] = units
    df["metric"] = metrics

    return df
