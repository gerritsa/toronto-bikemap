import pandas as pd

from bikeshare_pipeline.analytics import build_daily_analytics, build_hourly_analytics, build_routes_daily_analytics


def sample_trips() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "trip_id": "1",
                "route_id": "od_7000_7001",
                "service_date": "2024-01-01",
                "user_type": "Member",
                "bike_category": "Classic",
                "duration_seconds": 600,
                "start_seconds": 8 * 3600,
                "start_station_id": "7000",
                "end_station_id": "7001",
                "start_station_name": "Start A",
                "end_station_name": "End A",
            },
            {
                "trip_id": "2",
                "route_id": "od_7000_7001",
                "service_date": "2024-01-01",
                "user_type": "Member",
                "bike_category": "Classic",
                "duration_seconds": 900,
                "start_seconds": 8 * 3600 + 1800,
                "start_station_id": "7000",
                "end_station_id": "7001",
                "start_station_name": "Start A",
                "end_station_name": "End A",
            },
            {
                "trip_id": "3",
                "route_id": "od_7001_7002",
                "service_date": "2024-01-01",
                "user_type": "Casual",
                "bike_category": "E-bike",
                "duration_seconds": 300,
                "start_seconds": 9 * 3600,
                "start_station_id": "7001",
                "end_station_id": "7002",
                "start_station_name": "Start B",
                "end_station_name": "End B",
            },
        ]
    )


def sample_routes() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"route_id": "od_7000_7001", "distance_meters": 1200.0},
            {"route_id": "od_7001_7002", "distance_meters": 800.0},
        ]
    )


def test_build_daily_analytics_groups_by_date_and_filters():
    daily = build_daily_analytics(sample_trips(), sample_routes())

    member_classic = daily[(daily["user_type"] == "Member") & (daily["bike_category"] == "Classic")].iloc[0]
    assert member_classic["trip_count"] == 2
    assert member_classic["route_count"] == 1
    assert member_classic["distance_meters_sum"] == 2400.0
    assert member_classic["duration_seconds_sum"] == 1500


def test_build_hourly_analytics_groups_by_hour():
    hourly = build_hourly_analytics(sample_trips())

    eight_am = hourly[(hourly["user_type"] == "Member") & (hourly["hour"] == 8)].iloc[0]
    assert eight_am["trip_count"] == 2


def test_build_routes_daily_analytics_preserves_station_labels():
    routes_daily = build_routes_daily_analytics(sample_trips(), sample_routes())

    top_route = routes_daily.iloc[0]
    assert top_route["route_id"] == "od_7000_7001"
    assert top_route["trip_count"] == 2
    assert top_route["start_station_name"] == "Start A"
    assert top_route["end_station_name"] == "End A"
