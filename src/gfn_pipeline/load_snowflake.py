"""Snowflake loader for GFN carbon footprint data."""
from __future__ import annotations

import pandas as pd


def get_snowflake_connection(settings):
    """Create a Snowflake connection using snowflake-connector-python."""
    import snowflake.connector

    return snowflake.connector.connect(
        account=settings.snowflake_account,
        user=settings.snowflake_user,
        password=settings.snowflake_password,
        warehouse=settings.snowflake_warehouse,
        database=settings.snowflake_database,
        schema=settings.snowflake_schema,
    )


DDL_RAW = """
CREATE TABLE IF NOT EXISTS {database}.{schema}.carbon_footprint_raw (
    country_code INTEGER,
    country_name VARCHAR,
    iso_alpha2 VARCHAR(2),
    year INTEGER,
    carbon_footprint_gha FLOAT,
    total_footprint_gha FLOAT,
    score VARCHAR(10),
    record_hash VARCHAR(64),
    extracted_at TIMESTAMP_NTZ,
    loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
"""

DDL_MART = """
CREATE TABLE IF NOT EXISTS {database}.MART.carbon_footprint (
    country_code INTEGER,
    country_name VARCHAR,
    iso_alpha2 VARCHAR(2),
    year INTEGER,
    carbon_footprint_gha FLOAT,
    total_footprint_gha FLOAT,
    carbon_pct_of_total FLOAT,
    score VARCHAR(10),
    PRIMARY KEY (country_code, year)
);
"""


def init_snowflake_tables(conn, database: str, schema: str) -> None:
    """Create RAW and MART tables if they don't exist."""
    cur = conn.cursor()
    try:
        cur.execute(f"CREATE DATABASE IF NOT EXISTS {database}")
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {database}.{schema}")
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {database}.MART")
        cur.execute(DDL_RAW.format(database=database, schema=schema))
        cur.execute(DDL_MART.format(database=database))
    finally:
        cur.close()


def load_to_snowflake(conn, df: pd.DataFrame, database: str, schema: str) -> int:
    """Load carbon footprint data to Snowflake RAW table using write_pandas."""
    from snowflake.connector.pandas_tools import write_pandas

    # Ensure column order matches table
    cols = [
        "country_code",
        "country_name",
        "iso_alpha2",
        "year",
        "carbon_footprint_gha",
        "total_footprint_gha",
        "score",
        "record_hash",
        "extracted_at",
    ]
    df_out = df[cols].copy()

    success, nchunks, nrows, _ = write_pandas(
        conn,
        df_out,
        table_name="CARBON_FOOTPRINT_RAW",
        database=database,
        schema=schema,
        auto_create_table=False,
    )
    return nrows


def refresh_mart(conn, database: str) -> None:
    """Refresh the MART table with deduplicated, enriched data."""
    cur = conn.cursor()
    try:
        cur.execute(f"""
            MERGE INTO {database}.MART.carbon_footprint AS tgt
            USING (
                SELECT DISTINCT
                    country_code,
                    country_name,
                    iso_alpha2,
                    year,
                    carbon_footprint_gha,
                    total_footprint_gha,
                    CASE WHEN total_footprint_gha > 0 
                         THEN ROUND(carbon_footprint_gha / total_footprint_gha * 100, 2)
                         ELSE NULL END AS carbon_pct_of_total,
                    score
                FROM {database}.RAW.carbon_footprint_raw
                QUALIFY ROW_NUMBER() OVER (PARTITION BY country_code, year ORDER BY extracted_at DESC) = 1
            ) AS src
            ON tgt.country_code = src.country_code AND tgt.year = src.year
            WHEN MATCHED THEN UPDATE SET
                country_name = src.country_name,
                iso_alpha2 = src.iso_alpha2,
                carbon_footprint_gha = src.carbon_footprint_gha,
                total_footprint_gha = src.total_footprint_gha,
                carbon_pct_of_total = src.carbon_pct_of_total,
                score = src.score
            WHEN NOT MATCHED THEN INSERT (
                country_code, country_name, iso_alpha2, year,
                carbon_footprint_gha, total_footprint_gha, carbon_pct_of_total, score
            ) VALUES (
                src.country_code, src.country_name, src.iso_alpha2, src.year,
                src.carbon_footprint_gha, src.total_footprint_gha, src.carbon_pct_of_total, src.score
            );
        """)
    finally:
        cur.close()
