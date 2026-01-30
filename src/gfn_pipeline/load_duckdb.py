from __future__ import annotations

import duckdb
import pandas as pd


DDL = """
CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.carbon_footprint (
    country_code INTEGER,
    country_name VARCHAR,
    iso_alpha2 VARCHAR,
    year INTEGER,
    carbon_footprint_gha DOUBLE,
    total_footprint_gha DOUBLE,
    score VARCHAR,
    record_hash VARCHAR,
    extracted_at TIMESTAMP
);
"""


def init_db(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(DDL)


def load_raw(con: duckdb.DuckDBPyConnection, df: pd.DataFrame) -> None:
    init_db(con)
    con.register("df", df)
    con.execute(
        """
        INSERT INTO raw.carbon_footprint
        SELECT
            country_code,
            country_name,
            iso_alpha2,
            year,
            carbon_footprint_gha,
            total_footprint_gha,
            score,
            record_hash,
            extracted_at
        FROM df
        """
    )
    con.unregister("df")
