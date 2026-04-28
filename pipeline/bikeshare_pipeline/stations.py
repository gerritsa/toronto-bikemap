from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd
import requests

from .config import GBFS_STATION_INFORMATION_URL
from .normalize import normalize_station_id


def fetch_gbfs_stations(url: str = GBFS_STATION_INFORMATION_URL) -> pd.DataFrame:
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    payload = response.json()
    rows = []
    for station in payload["data"]["stations"]:
        rows.append(
            {
                "station_id": normalize_station_id(station.get("station_id")),
                "name": station.get("name", ""),
                "lat": float(station["lat"]),
                "lon": float(station["lon"]),
                "capacity": station.get("capacity"),
                "source": "gbfs_current",
            }
        )
    return pd.DataFrame(rows)


def load_manual_overrides(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(columns=["station_id", "name", "lat", "lon", "capacity", "source"])
    payload: list[dict[str, Any]] = json.loads(path.read_text())
    rows = []
    for station in payload:
        rows.append(
            {
                "station_id": normalize_station_id(station.get("station_id")),
                "name": station.get("name", ""),
                "lat": float(station["lat"]),
                "lon": float(station["lon"]),
                "capacity": station.get("capacity"),
                "source": "manual_override",
            }
        )
    return pd.DataFrame(rows)


def build_station_table(trips: pd.DataFrame, gbfs: pd.DataFrame, overrides: pd.DataFrame, missing_trip_threshold: int) -> pd.DataFrame:
    stations = pd.concat([gbfs, overrides], ignore_index=True)
    stations = stations.drop_duplicates("station_id", keep="last")
    known_ids = set(stations["station_id"])

    start_counts = trips["start_station_id"].value_counts()
    end_counts = trips["end_station_id"].value_counts()
    affected_counts = start_counts.add(end_counts, fill_value=0).astype(int)
    missing = affected_counts[[station_id not in known_ids and station_id != "" for station_id in affected_counts.index]]
    blocking = missing[missing > missing_trip_threshold]
    if not blocking.empty:
        formatted = ", ".join(f"{station_id} ({count} trips)" for station_id, count in blocking.items())
        raise ValueError(
            "Missing station coordinates exceed threshold. Add manual overrides for: "
            f"{formatted}"
        )

    return stations[["station_id", "name", "lat", "lon", "capacity", "source"]].sort_values("station_id").reset_index(drop=True)
