from __future__ import annotations

import hashlib
from typing import Any

import pandas as pd

from .config import BIKE_CATEGORIES


def normalize_station_id(value: Any) -> str:
    if value is None or pd.isna(value):
        return ""
    text = str(value).strip()
    if text.endswith(".0"):
        text = text[:-2]
    return text


def bike_category(model: Any) -> str:
    model_text = str(model).strip()
    if model_text not in BIKE_CATEGORIES:
        raise ValueError(f"Unsupported Bike_Model: {model_text}")
    return BIKE_CATEGORIES[model_text]


def normal_route_id(start_station_id: str, end_station_id: str) -> str:
    return f"od_{start_station_id}_{end_station_id}"


def synthetic_loop_route_id(trip_id: str) -> str:
    digest = hashlib.sha1(str(trip_id).encode("utf-8")).hexdigest()[:16]
    return f"loop_{digest}"


def is_internal_station_name(value: Any) -> bool:
    text = str(value).strip().lower()
    return "test" in text or "warehouse" in text or text == "nyclab"


def normalize_trips(frame: pd.DataFrame) -> pd.DataFrame:
    required = {
        "Trip_Id",
        "Trip_Duration",
        "Start_Station_Id",
        "Start_Time",
        "End_Station_Id",
        "End_Time",
        "User_Type",
        "Bike_Model",
    }
    missing = required - set(frame.columns)
    if missing:
        raise ValueError(f"Trip CSV is missing required columns: {sorted(missing)}")

    trips = pd.DataFrame()
    trips["trip_id"] = frame["Trip_Id"].astype(str)
    trips["start_station_id"] = frame["Start_Station_Id"].map(normalize_station_id)
    trips["end_station_id"] = frame["End_Station_Id"].map(normalize_station_id)
    trips["start_station_name"] = frame.get("Start_Station_Name", trips["start_station_id"]).astype(str)
    trips["end_station_name"] = frame.get("End_Station_Name", trips["end_station_id"]).astype(str)
    trips["start_time"] = pd.to_datetime(frame["Start_Time"], errors="raise")
    duration_seconds = pd.to_numeric(frame["Trip_Duration"], errors="coerce")
    trips["end_time"] = pd.to_datetime(frame["End_Time"], errors="coerce").fillna(
        trips["start_time"] + pd.to_timedelta(duration_seconds, unit="s")
    )
    trips["service_date"] = trips["start_time"].dt.date.astype(str)
    trips["start_seconds"] = (
        trips["start_time"].dt.hour * 3600 + trips["start_time"].dt.minute * 60 + trips["start_time"].dt.second
    ).astype("int32")
    trips["end_seconds"] = (
        trips["end_time"].dt.hour * 3600 + trips["end_time"].dt.minute * 60 + trips["end_time"].dt.second
    ).astype("int32")
    trips["duration_seconds"] = duration_seconds.fillna((trips["end_time"] - trips["start_time"]).dt.total_seconds()).astype(
        "int32"
    )
    trips["user_type"] = frame["User_Type"].astype(str).str.strip()
    trips["bike_model"] = frame["Bike_Model"].astype(str).str.strip()
    trips["bike_category"] = trips["bike_model"].map(bike_category)
    internal_station = trips["start_station_name"].map(is_internal_station_name) | trips["end_station_name"].map(
        is_internal_station_name
    )
    has_station_ids = (trips["start_station_id"] != "") & (trips["end_station_id"] != "")
    trips = trips[~internal_station & has_station_ids].copy()
    trips["is_same_station"] = trips["start_station_id"] == trips["end_station_id"]
    trips["route_id"] = trips.apply(
        lambda row: synthetic_loop_route_id(row["trip_id"])
        if row["is_same_station"]
        else normal_route_id(row["start_station_id"], row["end_station_id"]),
        axis=1,
    )
    return trips
