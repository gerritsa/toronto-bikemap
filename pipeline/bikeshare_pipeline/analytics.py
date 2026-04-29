from __future__ import annotations

import pandas as pd


def _trips_with_routes(trips: pd.DataFrame, routes: pd.DataFrame) -> pd.DataFrame:
    merged = trips.merge(routes[["route_id", "distance_meters"]], on="route_id", how="left")
    merged["distance_meters"] = merged["distance_meters"].fillna(0.0)
    return merged


def build_daily_analytics(trips: pd.DataFrame, routes: pd.DataFrame) -> pd.DataFrame:
    merged = _trips_with_routes(trips, routes)
    daily = (
        merged.groupby(["service_date", "user_type", "bike_category"], as_index=False)
        .agg(
            trip_count=("trip_id", "size"),
            route_count=("route_id", "nunique"),
            distance_meters_sum=("distance_meters", "sum"),
            duration_seconds_sum=("duration_seconds", "sum"),
        )
        .sort_values(["service_date", "user_type", "bike_category"])
        .reset_index(drop=True)
    )
    daily["trip_count"] = daily["trip_count"].astype("int32")
    daily["route_count"] = daily["route_count"].astype("int32")
    daily["duration_seconds_sum"] = daily["duration_seconds_sum"].astype("int64")
    return daily


def build_hourly_analytics(trips: pd.DataFrame) -> pd.DataFrame:
    hourly_source = trips[["service_date", "user_type", "bike_category", "trip_id", "start_seconds"]].copy()
    hourly_source["hour"] = (hourly_source["start_seconds"] // 3600).astype("int8")
    hourly = (
        hourly_source.groupby(["service_date", "hour", "user_type", "bike_category"], as_index=False)
        .agg(trip_count=("trip_id", "size"))
        .sort_values(["service_date", "hour", "user_type", "bike_category"])
        .reset_index(drop=True)
    )
    hourly["trip_count"] = hourly["trip_count"].astype("int32")
    return hourly


def build_routes_daily_analytics(trips: pd.DataFrame, routes: pd.DataFrame) -> pd.DataFrame:
    merged = _trips_with_routes(trips, routes)
    routes_daily = (
        merged.groupby(
            [
                "service_date",
                "user_type",
                "bike_category",
                "route_id",
                "start_station_id",
                "end_station_id",
                "start_station_name",
                "end_station_name",
            ],
            as_index=False,
        )
        .agg(
            trip_count=("trip_id", "size"),
            distance_meters=("distance_meters", "first"),
        )
        .sort_values(["service_date", "trip_count", "route_id"], ascending=[True, False, True])
        .reset_index(drop=True)
    )
    routes_daily["trip_count"] = routes_daily["trip_count"].astype("int32")
    return routes_daily
