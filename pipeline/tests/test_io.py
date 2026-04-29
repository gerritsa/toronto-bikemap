from pathlib import Path
import zipfile

from bikeshare_pipeline.io import read_csv_bytes, read_trip_zip


def test_read_trip_zip_concatenates_multiple_csv_files(tmp_path: Path):
    zip_path = tmp_path / "bikeshare-ridership-2025.zip"
    with zipfile.ZipFile(zip_path, "w") as archive:
        archive.writestr(
            "bikeshare_2025_01.csv",
            "Trip_Id,Trip_Duration,Start_Station_Id,Start_Time,End_Station_Id,End_Time,User_Type,Bike_Model\n"
            "1,600,7000,2025-01-01 08:00:00,7001,2025-01-01 08:10:00,Member,ICONIC\n",
        )
        archive.writestr(
            "bikeshare_2025_02.csv",
            "Trip_Id,Trip_Duration,Start_Station_Id,Start_Time,End_Station_Id,End_Time,User_Type,Bike_Model\n"
            "2,900,7001,2025-02-01 09:00:00,7002,2025-02-01 09:15:00,Casual,EFIT\n",
        )

    frame = read_trip_zip(zip_path)

    assert frame["Trip_Id"].tolist() == ["1", "2"]


def test_read_trip_zip_raises_when_no_csv_exists(tmp_path: Path):
    zip_path = tmp_path / "empty.zip"
    with zipfile.ZipFile(zip_path, "w") as archive:
        archive.writestr("notes.txt", "no csv here")

    try:
        read_trip_zip(zip_path)
    except ValueError as error:
        assert "Expected at least one CSV" in str(error)
    else:
        raise AssertionError("Expected read_trip_zip to reject archives without CSV files")


def test_read_csv_bytes_falls_back_to_cp1252():
    payload = "Trip_Id,Bike_Model,Start_Station_Name\n1,ICONIC,King – Bathurst\n".encode("cp1252")
    frame = read_csv_bytes(payload)
    assert frame.iloc[0]["Start_Station_Name"] == "King – Bathurst"
