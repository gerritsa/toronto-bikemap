import pandas as pd

from bikeshare_pipeline.normalize import bike_category, normalize_station_id, normalize_trips


def test_normalize_station_id_strips_float_suffix():
    assert normalize_station_id("7037.0") == "7037"
    assert normalize_station_id("7000") == "7000"


def test_bike_category_mapping():
    assert bike_category("ICONIC") == "Classic"
    assert bike_category("EFIT") == "E-bike"
    assert bike_category("ASTRO") == "E-bike"


def test_normalize_trips_handles_commas_and_same_station():
    frame = pd.DataFrame(
        [
            {
                "Trip_Id": "1",
                "Trip_Duration": "120",
                "Start_Station_Id": "7000",
                "Start_Time": "2026-01-01 08:00:00",
                "Start_Station_Name": "Station, With Comma",
                "End_Station_Id": "7000.0",
                "End_Time": "2026-01-01 08:02:00",
                "End_Station_Name": "Station, With Comma",
                "Bike_Id": "1",
                "User_Type": "Casual",
                "Bike_Model": "ASTRO",
            }
        ]
    )
    trips = normalize_trips(frame)
    assert bool(trips.iloc[0]["is_same_station"]) is True
    assert trips.iloc[0]["bike_category"] == "E-bike"
    assert trips.iloc[0]["route_id"].startswith("loop_")


def test_normalize_trips_derives_missing_end_time_from_duration():
    frame = pd.DataFrame(
        [
            {
                "Trip_Id": "1",
                "Trip_Duration": "120",
                "Start_Station_Id": "7000",
                "Start_Time": "2026-01-01 08:00:00",
                "Start_Station_Name": "Start",
                "End_Station_Id": "7001",
                "End_Time": None,
                "End_Station_Name": "End",
                "Bike_Id": "1",
                "User_Type": "Casual",
                "Bike_Model": "ICONIC",
            }
        ]
    )

    trips = normalize_trips(frame)

    assert trips.iloc[0]["end_seconds"] == 8 * 3600 + 2 * 60
    assert trips.iloc[0]["duration_seconds"] == 120


def test_normalize_trips_drops_internal_test_stations():
    frame = pd.DataFrame(
        [
            {
                "Trip_Id": "1",
                "Trip_Duration": "120",
                "Start_Station_Id": "7000",
                "Start_Time": "2026-01-01 08:00:00",
                "Start_Station_Name": "Start",
                "End_Station_Id": "7001",
                "End_Time": "2026-01-01 08:02:00",
                "End_Station_Name": "End",
                "Bike_Id": "1",
                "User_Type": "Casual",
                "Bike_Model": "ICONIC",
            },
            {
                "Trip_Id": "2",
                "Trip_Duration": "0",
                "Start_Station_Id": "8222",
                "Start_Time": "2026-01-01 09:00:00",
                "Start_Station_Name": "NycLab",
                "End_Station_Id": "8222",
                "End_Time": "2026-01-01 09:00:00",
                "End_Station_Name": "NycLab",
                "Bike_Id": "2",
                "User_Type": "Casual",
                "Bike_Model": "ICONIC",
            },
        ]
    )

    trips = normalize_trips(frame)

    assert trips["trip_id"].tolist() == ["1"]


def test_normalize_trips_drops_blank_station_ids():
    frame = pd.DataFrame(
        [
            {
                "Trip_Id": "1",
                "Trip_Duration": "120",
                "Start_Station_Id": "7000",
                "Start_Time": "2026-01-01 08:00:00",
                "Start_Station_Name": "Start",
                "End_Station_Id": None,
                "End_Time": "2026-01-01 08:02:00",
                "End_Station_Name": None,
                "Bike_Id": "1",
                "User_Type": "Casual",
                "Bike_Model": "ICONIC",
            }
        ]
    )

    trips = normalize_trips(frame)

    assert trips.empty
