from __future__ import annotations

import json
import re
import unicodedata
from pathlib import Path
from typing import Any

import pandas as pd
import requests

from .config import GBFS_STATION_INFORMATION_URL
from .normalize import normalize_station_id


def normalize_station_name(value: Any) -> str:
    return " ".join(str(value or "").strip().split())


def canonical_station_name(value: Any) -> str:
    text = normalize_station_name(value).lower()
    text = unicodedata.normalize("NFKD", text)
    text = "".join(char for char in text if not unicodedata.combining(char))
    text = text.replace("’", "'").replace("`", "'")
    text = re.sub(r"\s*-\s*smart$", "", text)
    text = re.sub(r"\([^)]*\)", " ", text)
    text = text.replace("&", " and ")
    text = text.replace("'", "")
    text = text.replace(".", "")
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return " ".join(text.split())


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
                "source": station.get("source", "manual_override"),
            }
        )
    return pd.DataFrame(rows)


def infer_historical_station_overrides(trips: pd.DataFrame, stations: pd.DataFrame) -> pd.DataFrame:
    observed = pd.concat(
        [
            trips[["start_station_id", "start_station_name"]].rename(
                columns={"start_station_id": "station_id", "start_station_name": "name"}
            ),
            trips[["end_station_id", "end_station_name"]].rename(
                columns={"end_station_id": "station_id", "end_station_name": "name"}
            ),
        ],
        ignore_index=True,
    )
    observed["station_id"] = observed["station_id"].map(normalize_station_id)
    observed["name"] = observed["name"].map(normalize_station_name)
    observed = observed[(observed["station_id"] != "") & (observed["name"] != "")]
    if observed.empty:
        return pd.DataFrame(columns=["station_id", "name", "lat", "lon", "capacity", "source"])

    primary_names = (
        observed.groupby(["station_id", "name"], as_index=False)
        .size()
        .sort_values(["station_id", "size", "name"], ascending=[True, False, True])
        .drop_duplicates("station_id")
    )

    known_ids = set(stations["station_id"])
    station_lookup = stations.copy()
    station_lookup["name_key"] = station_lookup["name"].map(canonical_station_name)
    station_lookup = station_lookup[station_lookup["name_key"] != ""]
    unique_name_matches = station_lookup.groupby("name_key")["station_id"].nunique()
    unique_name_keys = set(unique_name_matches[unique_name_matches == 1].index)
    station_lookup = station_lookup[station_lookup["name_key"].isin(unique_name_keys)].drop_duplicates("name_key")

    inferred_rows = []
    for row in primary_names.itertuples(index=False):
        if row.station_id in known_ids:
            continue
        name_key = canonical_station_name(row.name)
        if not name_key:
            continue
        match = station_lookup[station_lookup["name_key"] == name_key]
        if match.empty:
            continue
        source_station = match.iloc[0]
        inferred_rows.append(
            {
                "station_id": row.station_id,
                "name": row.name,
                "lat": float(source_station["lat"]),
                "lon": float(source_station["lon"]),
                "capacity": source_station["capacity"],
                "source": f"historical_name_match:{source_station['station_id']}",
            }
        )

    return pd.DataFrame(inferred_rows, columns=["station_id", "name", "lat", "lon", "capacity", "source"])


def build_missing_station_candidates(trips: pd.DataFrame, stations: pd.DataFrame, missing_counts: pd.Series) -> list[dict[str, Any]]:
    observed = pd.concat(
        [
            trips[["start_station_id", "start_station_name"]].rename(
                columns={"start_station_id": "station_id", "start_station_name": "name"}
            ),
            trips[["end_station_id", "end_station_name"]].rename(
                columns={"end_station_id": "station_id", "end_station_name": "name"}
            ),
        ],
        ignore_index=True,
    )
    observed["station_id"] = observed["station_id"].map(normalize_station_id)
    observed["name"] = observed["name"].map(normalize_station_name)
    observed = observed[(observed["station_id"] != "") & (observed["name"] != "")]

    station_lookup = stations.copy()
    station_lookup["name_key"] = station_lookup["name"].map(canonical_station_name)
    station_lookup = station_lookup[station_lookup["name_key"] != ""]
    by_name_key = {
        name_key: group[["station_id", "name", "lat", "lon", "capacity", "source"]].to_dict("records")
        for name_key, group in station_lookup.groupby("name_key")
    }

    candidates: list[dict[str, Any]] = []
    for station_id, trip_count in missing_counts.items():
        names = observed[observed["station_id"] == station_id]["name"].value_counts().head(5)
        top_names = [{"name": name, "count": int(count)} for name, count in names.items()]
        primary_name = top_names[0]["name"] if top_names else ""
        matches = by_name_key.get(canonical_station_name(primary_name), []) if primary_name else []
        candidate = {
            "station_id": station_id,
            "trip_count": int(trip_count),
            "primary_name": primary_name,
            "top_names": top_names,
        }
        if len(matches) == 1:
            match = matches[0]
            candidate["suggested_match"] = {
                "station_id": match["station_id"],
                "name": match["name"],
                "lat": float(match["lat"]),
                "lon": float(match["lon"]),
                "capacity": match["capacity"],
                "source": match["source"],
            }
        elif matches:
            candidate["ambiguous_matches"] = matches
        candidates.append(candidate)
    return candidates


def build_station_table(trips: pd.DataFrame, gbfs: pd.DataFrame, overrides: pd.DataFrame, missing_trip_threshold: int) -> pd.DataFrame:
    station_frames = [frame for frame in (gbfs, overrides) if not frame.empty]
    stations = pd.concat(station_frames, ignore_index=True) if station_frames else pd.DataFrame(
        columns=["station_id", "name", "lat", "lon", "capacity", "source"]
    )
    stations = stations.drop_duplicates("station_id", keep="last")
    inferred = infer_historical_station_overrides(trips, stations)
    if not inferred.empty:
        stations = pd.concat([stations, inferred], ignore_index=True).drop_duplicates("station_id", keep="last")
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
