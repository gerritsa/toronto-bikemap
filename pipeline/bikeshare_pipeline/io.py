from __future__ import annotations

import json
import io
import zipfile
from pathlib import Path

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests


def fetch_json(url: str) -> dict:
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    return response.json()


def download_file(url: str, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    with requests.get(url, stream=True, timeout=120) as response:
        response.raise_for_status()
        with destination.open("wb") as file:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    file.write(chunk)


def read_csv_bytes(payload: bytes) -> pd.DataFrame:
    last_error: Exception | None = None
    for encoding in ("utf-8", "cp1252", "latin-1"):
        try:
            return pd.read_csv(io.BytesIO(payload), dtype=str, encoding=encoding)
        except UnicodeDecodeError as error:
            last_error = error
    if last_error is not None:
        raise last_error
    raise ValueError("Could not decode CSV payload")


def read_trip_zip(path: Path) -> pd.DataFrame:
    with zipfile.ZipFile(path) as archive:
        csv_names = sorted(name for name in archive.namelist() if name.lower().endswith(".csv"))
        if not csv_names:
            raise ValueError(f"Expected at least one CSV in {path}, found none")

        frames: list[pd.DataFrame] = []
        for csv_name in csv_names:
            with archive.open(csv_name) as file:
                frames.append(read_csv_bytes(file.read()))
        return pd.concat(frames, ignore_index=True)


def write_parquet(frame: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pandas(frame, preserve_index=False)
    pq.write_table(table, path, compression="zstd")


def write_partitioned_trips(trips: pd.DataFrame, base_dir: Path) -> list[str]:
    written: list[str] = []
    for service_date, group in trips.groupby("service_date"):
        year, month, day = str(service_date).split("-")
        path = base_dir / f"year={year}" / f"month={month}" / f"day={day}" / "trips.parquet"
        write_parquet(group, path)
        written.append(str(path))
    return written


def write_json(payload: dict, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True))


def upload_directory_to_r2(directory: Path, bucket: str, prefix: str, endpoint_url: str) -> None:
    client = boto3.client("s3", endpoint_url=endpoint_url)
    for path in directory.rglob("*"):
        if not path.is_file():
            continue
        key = f"{prefix.strip('/')}/{path.relative_to(directory).as_posix()}"
        client.upload_file(str(path), bucket, key, ExtraArgs={"CacheControl": "public, max-age=31536000, immutable"})


def upload_file_to_r2(path: Path, bucket: str, key: str, endpoint_url: str, cache_control: str) -> None:
    client = boto3.client("s3", endpoint_url=endpoint_url)
    client.upload_file(str(path), bucket, key, ExtraArgs={"CacheControl": cache_control, "ContentType": "application/json"})
