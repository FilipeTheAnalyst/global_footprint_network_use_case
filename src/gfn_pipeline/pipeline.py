"""
GFN Carbon Footprint Pipeline - LEGACY VERSION (Sequential).

DEPRECATED: Use pipeline_async.py instead for 15x faster extraction.

This file is kept for reference and Snowflake integration code.
For new usage, run:
    python -m gfn_pipeline.pipeline_async --destination snowflake
"""
from __future__ import annotations

import argparse
import time
from datetime import datetime, timezone

import pandas as pd

from gfn_pipeline.client_gfn import GFNClient
from gfn_pipeline.config import Settings
from gfn_pipeline.storage import partition_path, write_parquet_local, write_parquet_s3
from gfn_pipeline.transform import normalize_carbon_footprint


def run(
    start_year: int,
    end_year: int,
    output_mode: str,
    load_to_duckdb: bool,
    load_to_snowflake: bool,
) -> None:
    s = Settings()
    client = GFNClient(
        base_url=s.gfn_api_base_url,
        api_key=s.gfn_api_key,
        username=s.gfn_api_username,
    )

    extracted_at = datetime.now(timezone.utc)

    # EFCtot = Ecological Footprint Total (includes carbon field)
    record_code = "EFCtot"

    # Fetch countries once
    countries = []
    if not s.gfn_mock:
        countries = client.get_json("countries")
        print(f"Fetched {len(countries)} countries")

    all_data: list[pd.DataFrame] = []

    for year in range(start_year, end_year + 1):
        print(f"Processing year {year}...")

        if s.gfn_mock:
            raw_records = [
                {"countryCode": 1, "countryName": "France", "isoa2": "FR", "year": year, "carbon": 123000000, "value": 456000000, "score": "3A"},
                {"countryCode": 2, "countryName": "Germany", "isoa2": "DE", "year": year, "carbon": 234000000, "value": 567000000, "score": "3A"},
            ]
        else:
            raw_records = []
            for i, c in enumerate(countries):
                country_code = c.get("countryCode")
                if country_code is None:
                    continue

                path = f"data/{country_code}/{year}/{record_code}"
                try:
                    raw = client.get_json(path=path)
                    if isinstance(raw, list):
                        raw_records.extend(raw)
                    else:
                        raw_records.append(raw)
                except Exception as e:
                    print(f"  Warning: Failed to fetch {path}: {e}")
                    continue

                # Throttle to avoid 429s
                if i % 10 == 0:
                    time.sleep(0.2)

        df = normalize_carbon_footprint(raw_records, extracted_at=extracted_at)
        all_data.append(df)

        # Write RAW to S3/local as parquet
        rel = partition_path(prefix=s.s3_prefix, dataset="carbon_footprint", year=year, ingest_dt=extracted_at, ext="parquet")

        if output_mode == "local":
            res = write_parquet_local(df, out_dir=s.local_output_dir, relative_path=rel)
            print(f"  Wrote {res.row_count} rows to {res.location}")
        elif output_mode == "s3":
            if not s.s3_bucket:
                raise ValueError("S3_BUCKET env var must be set for output_mode=s3")
            res = write_parquet_s3(df, bucket=s.s3_bucket, key=rel)
            print(f"  Wrote {res.row_count} rows to {res.location}")

    # Combine all years
    if all_data:
        combined_df = pd.concat(all_data, ignore_index=True)
        print(f"\nTotal records: {len(combined_df)}")

        # Load to DuckDB (local testing)
        if load_to_duckdb:
            import duckdb
            from gfn_pipeline.load_duckdb import load_raw

            con = duckdb.connect(s.duckdb_path)
            load_raw(con, combined_df)
            con.close()
            print(f"Loaded {len(combined_df)} rows into DuckDB: {s.duckdb_path}")

        # Load to Snowflake (target DB)
        if load_to_snowflake:
            from gfn_pipeline.load_snowflake import (
                get_snowflake_connection,
                init_snowflake_tables,
                load_to_snowflake as sf_load,
                refresh_mart,
            )

            if not all([s.snowflake_account, s.snowflake_user, s.snowflake_password]):
                raise ValueError("Snowflake credentials not set (SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD)")

            conn = get_snowflake_connection(s)
            try:
                init_snowflake_tables(conn, s.snowflake_database, s.snowflake_schema)
                nrows = sf_load(conn, combined_df, s.snowflake_database, s.snowflake_schema)
                print(f"Loaded {nrows} rows into Snowflake: {s.snowflake_database}.{s.snowflake_schema}.CARBON_FOOTPRINT_RAW")

                refresh_mart(conn, s.snowflake_database)
                print(f"Refreshed MART table: {s.snowflake_database}.MART.CARBON_FOOTPRINT")
            finally:
                conn.close()


def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="GFN Carbon Footprint ingestion pipeline")
    p.add_argument("--start-year", type=int, default=Settings().start_year)
    p.add_argument("--end-year", type=int, default=Settings().end_year)
    p.add_argument("--output-mode", choices=["local", "s3"], default=Settings().output_mode)
    p.add_argument("--load-duckdb", action="store_true", help="Load data into local DuckDB")
    p.add_argument("--load-snowflake", action="store_true", help="Load data into Snowflake")
    return p


def main() -> None:
    args = build_arg_parser().parse_args()
    run(
        start_year=args.start_year,
        end_year=args.end_year,
        output_mode=args.output_mode,
        load_to_duckdb=args.load_duckdb,
        load_to_snowflake=args.load_snowflake,
    )


if __name__ == "__main__":
    main()
