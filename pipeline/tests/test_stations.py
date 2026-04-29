import pandas as pd

from bikeshare_pipeline.stations import build_missing_station_candidates, build_station_table, infer_historical_station_overrides


def test_infer_historical_station_overrides_matches_by_exact_name():
    trips = pd.DataFrame(
        [
            {
                "start_station_id": "9000",
                "start_station_name": "King St W / Bay St",
                "end_station_id": "7001",
                "end_station_name": "Known End",
            },
            {
                "start_station_id": "9000",
                "start_station_name": "King St W / Bay St",
                "end_station_id": "7001",
                "end_station_name": "Known End",
            },
        ]
    )
    stations = pd.DataFrame(
        [
            {
                "station_id": "7000",
                "name": "King St W / Bay St",
                "lat": 43.1,
                "lon": -79.1,
                "capacity": None,
                "source": "gbfs_current",
            }
        ]
    )

    inferred = infer_historical_station_overrides(trips, stations)

    assert inferred.iloc[0]["station_id"] == "9000"
    assert inferred.iloc[0]["lat"] == 43.1
    assert inferred.iloc[0]["lon"] == -79.1


def test_build_station_table_accepts_name_matched_historical_station():
    trips = pd.DataFrame(
        [
            {
                "start_station_id": "9000",
                "start_station_name": "King St W / Bay St",
                "end_station_id": "7001",
                "end_station_name": "Known End",
            }
        ]
    )
    gbfs = pd.DataFrame(
        [
            {
                "station_id": "7000",
                "name": "King St W / Bay St",
                "lat": 43.1,
                "lon": -79.1,
                "capacity": None,
                "source": "gbfs_current",
            }
        ]
    )
    overrides = pd.DataFrame(columns=["station_id", "name", "lat", "lon", "capacity", "source"])

    stations = build_station_table(trips, gbfs, overrides, missing_trip_threshold=1)

    matched = stations[stations["station_id"] == "9000"].iloc[0]
    assert matched["source"].startswith("historical_name_match:")


def test_infer_historical_station_overrides_matches_canonicalized_name():
    trips = pd.DataFrame(
        [
            {
                "start_station_id": "9001",
                "start_station_name": "St. Joseph St / Bay St - SMART",
                "end_station_id": "7001",
                "end_station_name": "Known End",
            }
        ]
    )
    stations = pd.DataFrame(
        [
            {
                "station_id": "7548_current",
                "name": "St Joseph St / Bay St",
                "lat": 43.2,
                "lon": -79.2,
                "capacity": None,
                "source": "gbfs_current",
            }
        ]
    )

    inferred = infer_historical_station_overrides(trips, stations)

    assert inferred.iloc[0]["station_id"] == "9001"
    assert inferred.iloc[0]["lat"] == 43.2


def test_build_missing_station_candidates_emits_primary_name_and_suggestion():
    trips = pd.DataFrame(
        [
            {
                "start_station_id": "9001",
                "start_station_name": "St. Joseph St / Bay St - SMART",
                "end_station_id": "7001",
                "end_station_name": "Known End",
            }
        ]
    )
    stations = pd.DataFrame(
        [
            {
                "station_id": "7548_current",
                "name": "St Joseph St / Bay St",
                "lat": 43.2,
                "lon": -79.2,
                "capacity": None,
                "source": "gbfs_current",
            }
        ]
    )
    missing_counts = pd.Series({"9001": 42})

    candidates = build_missing_station_candidates(trips, stations, missing_counts)

    assert candidates[0]["primary_name"] == "St. Joseph St / Bay St - SMART"
    assert candidates[0]["suggested_match"]["station_id"] == "7548_current"
