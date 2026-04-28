from __future__ import annotations

import hashlib
import math
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Iterable

import pandas as pd
import polyline
import requests

from .config import (
    SYNTHETIC_LOOP_MAX_WAYPOINT_METERS,
    SYNTHETIC_LOOP_MIN_WAYPOINT_METERS,
    SYNTHETIC_LOOP_SPEED_KMH,
)
from .normalize import normal_route_id, synthetic_loop_route_id


@dataclass(frozen=True)
class Point:
    lon: float
    lat: float


def haversine_meters(points: list[Point]) -> float:
    total = 0.0
    radius = 6371000
    for first, second in zip(points, points[1:]):
        lat1 = math.radians(first.lat)
        lat2 = math.radians(second.lat)
        dlat = math.radians(second.lat - first.lat)
        dlon = math.radians(second.lon - first.lon)
        a = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
        total += radius * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return total


def encode_points(points: list[Point]) -> str:
    return polyline.encode([(point.lat, point.lon) for point in points], precision=5)


def loop_waypoint_distance_meters(duration_seconds: float) -> float:
    total_distance = max(0.0, duration_seconds) * (SYNTHETIC_LOOP_SPEED_KMH * 1000 / 3600)
    one_way_distance = total_distance / 2
    return min(
        SYNTHETIC_LOOP_MAX_WAYPOINT_METERS,
        max(SYNTHETIC_LOOP_MIN_WAYPOINT_METERS, one_way_distance),
    )


def deterministic_loop_waypoint(trip_id: str, station: Point, duration_seconds: float) -> Point:
    digest = hashlib.sha1(str(trip_id).encode("utf-8")).digest()
    distance_m = loop_waypoint_distance_meters(duration_seconds)
    raw_bearing = int.from_bytes(digest[2:4], "big") / 65535 * 360

    if station.lat < 43.66:
        raw_bearing = 90 + raw_bearing % 120

    bearing = math.radians(raw_bearing)
    radius = 6371000
    lat1 = math.radians(station.lat)
    lon1 = math.radians(station.lon)
    angular_distance = distance_m / radius

    lat2 = math.asin(
        math.sin(lat1) * math.cos(angular_distance)
        + math.cos(lat1) * math.sin(angular_distance) * math.cos(bearing)
    )
    lon2 = lon1 + math.atan2(
        math.sin(bearing) * math.sin(angular_distance) * math.cos(lat1),
        math.cos(angular_distance) - math.sin(lat1) * math.sin(lat2),
    )
    return Point(lon=math.degrees(lon2), lat=math.degrees(lat2))


def osrm_route(osrm_url: str, profile: str, points: Iterable[Point]) -> tuple[str, float, float]:
    coordinates = ";".join(f"{point.lon:.6f},{point.lat:.6f}" for point in points)
    url = f"{osrm_url.rstrip('/')}/route/v1/{profile}/{coordinates}"
    response = requests.get(url, params={"overview": "full", "geometries": "polyline", "steps": "false"}, timeout=60)
    response.raise_for_status()
    payload = response.json()
    if payload.get("code") != "Ok" or not payload.get("routes"):
        raise RuntimeError(f"OSRM did not return a route: {payload.get('code')}")
    route = payload["routes"][0]
    return route["geometry"], float(route["distance"]), float(route["duration"])


def fallback_route(points: list[Point]) -> tuple[str, float, float]:
    distance = haversine_meters(points)
    return encode_points(points), distance, distance / 4.5


def generate_routes(
    trips: pd.DataFrame,
    stations: pd.DataFrame,
    osrm_url: str | None,
    osrm_profile: str,
    allow_fallback_routes: bool,
    route_workers: int = 1,
) -> pd.DataFrame:
    station_points = {
        row.station_id: Point(lon=float(row.lon), lat=float(row.lat))
        for row in stations.itertuples(index=False)
    }
    rows: list[dict[str, object]] = []
    route_tasks: list[tuple[str, str, str, bool, list[Point]]] = []

    normal_pairs = (
        trips[~trips["is_same_station"]][["start_station_id", "end_station_id"]]
        .drop_duplicates()
        .itertuples(index=False)
    )
    for start_station_id, end_station_id in normal_pairs:
        route_id = normal_route_id(start_station_id, end_station_id)
        if start_station_id not in station_points or end_station_id not in station_points:
            rows.append(missing_station_route(route_id, start_station_id, end_station_id))
            continue
        points = [station_points[start_station_id], station_points[end_station_id]]
        route_tasks.append((route_id, start_station_id, end_station_id, False, points))

    same_station_trips = trips[trips["is_same_station"]][
        ["trip_id", "start_station_id", "end_station_id", "duration_seconds"]
    ].itertuples(index=False)
    for trip_id, start_station_id, end_station_id, duration_seconds in same_station_trips:
        route_id = synthetic_loop_route_id(trip_id)
        if start_station_id not in station_points:
            rows.append(missing_station_route(route_id, start_station_id, end_station_id, synthetic=True))
            continue
        station = station_points[start_station_id]
        waypoint = deterministic_loop_waypoint(trip_id, station, float(duration_seconds))
        points = [station, waypoint, station]
        route_tasks.append((route_id, start_station_id, end_station_id, True, points))

    rows.extend(
        build_route_rows(route_tasks, osrm_url, osrm_profile, allow_fallback_routes, max(1, route_workers))
    )

    return pd.DataFrame(rows).drop_duplicates("route_id", keep="first").sort_values("route_id").reset_index(drop=True)


def build_route_rows(
    route_tasks: list[tuple[str, str, str, bool, list[Point]]],
    osrm_url: str | None,
    osrm_profile: str,
    allow_fallback_routes: bool,
    route_workers: int,
) -> list[dict[str, object]]:
    total = len(route_tasks)
    if total == 0:
        return []

    print(f"Generating {total} routes with {route_workers} worker(s)...", flush=True)

    if route_workers == 1:
        rows = []
        for index, task in enumerate(route_tasks, start=1):
            rows.append(route_row(*task, osrm_url, osrm_profile, allow_fallback_routes))
            print_route_progress(index, total)
        return rows

    rows = []
    with ThreadPoolExecutor(max_workers=route_workers) as executor:
        futures = [
            executor.submit(route_row, *task, osrm_url, osrm_profile, allow_fallback_routes)
            for task in route_tasks
        ]
        for index, future in enumerate(as_completed(futures), start=1):
            rows.append(future.result())
            print_route_progress(index, total)
    return rows


def print_route_progress(completed: int, total: int) -> None:
    if completed == total or completed % 1000 == 0:
        print(f"Generated {completed}/{total} routes", flush=True)


def route_row(
    route_id: str,
    start_station_id: str,
    end_station_id: str,
    synthetic: bool,
    points: list[Point],
    osrm_url: str | None,
    osrm_profile: str,
    allow_fallback_routes: bool,
) -> dict[str, object]:
    status = "synthetic_loop" if synthetic else "ok"
    engine = "osrm"
    try:
        if osrm_url:
            encoded, distance, duration = osrm_route(osrm_url, osrm_profile, points)
        elif allow_fallback_routes:
            engine = "fallback"
            encoded, distance, duration = fallback_route(points)
            status = "synthetic_loop" if synthetic else "fallback_straight_line"
        else:
            raise RuntimeError("OSRM URL is required unless fallback routes are allowed")
    except Exception:
        if not allow_fallback_routes:
            raise
        engine = "fallback"
        encoded, distance, duration = fallback_route(points)
        status = "synthetic_loop" if synthetic else "fallback_straight_line"

    return {
        "route_id": route_id,
        "start_station_id": start_station_id,
        "end_station_id": end_station_id,
        "is_synthetic_loop": synthetic,
        "encoded_polyline": encoded,
        "distance_meters": distance,
        "duration_seconds_estimate": duration,
        "routing_engine": engine,
        "routing_profile": osrm_profile,
        "route_status": status,
    }


def missing_station_route(route_id: str, start_station_id: str, end_station_id: str, synthetic: bool = False) -> dict[str, object]:
    return {
        "route_id": route_id,
        "start_station_id": start_station_id,
        "end_station_id": end_station_id,
        "is_synthetic_loop": synthetic,
        "encoded_polyline": "",
        "distance_meters": 0.0,
        "duration_seconds_estimate": 0.0,
        "routing_engine": "none",
        "routing_profile": "bicycle",
        "route_status": "missing_station",
    }
