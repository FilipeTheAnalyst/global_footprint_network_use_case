from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime

import boto3
import pandas as pd


@dataclass(frozen=True)
class WriteResult:
    location: str
    row_count: int


def _ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def write_jsonl_local(df: pd.DataFrame, out_dir: str, relative_path: str) -> WriteResult:
    full_dir = os.path.join(out_dir, os.path.dirname(relative_path))
    _ensure_dir(full_dir)
    full_path = os.path.join(out_dir, relative_path)

    with open(full_path, "w", encoding="utf-8") as f:
        for rec in df["record"].tolist():
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")

    return WriteResult(location=os.path.abspath(full_path), row_count=len(df))


def write_parquet_local(df: pd.DataFrame, out_dir: str, relative_path: str) -> WriteResult:
    full_dir = os.path.join(out_dir, os.path.dirname(relative_path))
    _ensure_dir(full_dir)
    full_path = os.path.join(out_dir, relative_path)

    df.to_parquet(full_path, index=False)
    return WriteResult(location=os.path.abspath(full_path), row_count=len(df))


def write_parquet_s3(df: pd.DataFrame, bucket: str, key: str) -> WriteResult:
    # Small pipeline: write to tmp file then upload. For production, stream/multipart.
    import tempfile

    s3 = boto3.client("s3")
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=True) as tmp:
        df.to_parquet(tmp.name, index=False)
        s3.upload_file(tmp.name, bucket, key)

    return WriteResult(location=f"s3://{bucket}/{key}", row_count=len(df))


def partition_path(prefix: str, dataset: str, year: int, ingest_dt: datetime, ext: str) -> str:
    dt = ingest_dt.strftime("%Y-%m-%d")
    return f"{prefix}/bronze/dataset={dataset}/year={year}/ingest_dt={dt}/part-00000.{ext}"
