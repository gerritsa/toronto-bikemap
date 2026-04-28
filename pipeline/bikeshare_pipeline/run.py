from __future__ import annotations

import argparse
import os
import shutil
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests

from .config import CKAN_PACKAGE_URL, DEFAULT_MISSING_STATION_TRIP_THRESHOLD
from .io import (
    download_file,
    fetch_json,
    read_trip_zip,
    upload_directory_to_r2,
    upload_file_to_r2,
    write_json,
    write_parquet,
    write_partitioned_trips,
)
from .normalize import normalize_trips
from .qa import route_qa_summary
from .routing import generate_routes
from .stations import build_station_table, fetch_gbfs_stations, load_manual_overrides


def find_2026_resource(package: dict) -> dict:
    resources = package["result"]["resources"]
    matches = [resource for resource in resources if resource["name"] == "bikeshare-ridership-2026.zip"]
    if not matches:
        raise ValueError("Could not find bikeshare-ridership-2026.zip in CKAN package")
    return matches[0]


def build_manifest(resource: dict, trips: pd.DataFrame, run_id: str, public_base_url: str, has_basemap: bool) -> dict:
    dates = sorted(trips["service_date"].unique().tolist())
    run_base = f"{public_base_url.rstrip('/')}/runs/{run_id}"
    return {
        "runId": run_id,
        "generatedAt": datetime.now(timezone.utc).isoformat(),
        "source": {
            "ckanPackageId": "bike-share-toronto-ridership-data",
            "ridershipResourceName": resource["name"],
            "ridershipUrl": resource["url"],
            "ridershipLastModified": resource.get("last_modified", ""),
            "ridershipSizeBytes": int(resource.get("size") or 0),
            "tripCount": int(len(trips)),
            "dateMin": dates[0] if dates else "",
            "dateMax": dates[-1] if dates else "",
        },
        "assets": {
            "tripsBaseUrl": f"{run_base}/trips",
            "routesUrl": f"{run_base}/routes/routes.parquet",
            "stationsUrl": f"{run_base}/stations/stations.parquet",
            "basemapUrl": f"{run_base}/basemap/toronto.pmtiles" if has_basemap else "",
        },
        "filters": {
            "userTypes": sorted(trips["user_type"].unique().tolist()),
            "bikeModels": sorted(trips["bike_model"].unique().tolist()),
            "bikeCategories": {
                "Classic": ["ICONIC"],
                "E-bike": ["EFIT", "ASTRO"],
            },
        },
        "dates": dates,
    }


def validate_outputs(trips: pd.DataFrame, routes: pd.DataFrame, stations: pd.DataFrame) -> None:
    if trips.empty:
        raise ValueError("No trips were generated")
    if routes.empty:
        raise ValueError("No routes were generated")
    if stations.empty:
        raise ValueError("No stations were generated")
    missing_route_ids = set(trips["route_id"]) - set(routes["route_id"])
    if missing_route_ids:
        raise ValueError(f"Trips reference missing routes: {len(missing_route_ids)} route IDs")


def previous_manifest_is_current(previous_manifest_url: str | None, resource: dict) -> bool:
    if not previous_manifest_url:
        return False
    try:
        response = requests.get(previous_manifest_url, timeout=15)
        if response.status_code == 404:
            return False
        response.raise_for_status()
        previous = response.json()
    except Exception:
        return False

    source = previous.get("source", {})
    return (
        source.get("ridershipUrl") == resource.get("url")
        and source.get("ridershipLastModified") == resource.get("last_modified")
        and int(source.get("ridershipSizeBytes") or 0) == int(resource.get("size") or 0)
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build Toronto Bike Share flow visualizer data artifacts.")
    parser.add_argument("--work-dir", type=Path, default=Path("pipeline/work"))
    parser.add_argument("--output-dir", type=Path, default=Path("pipeline/output"))
    parser.add_argument("--manual-overrides", type=Path, default=Path("pipeline/manual_overrides/stations.json"))
    parser.add_argument("--public-base-url", default=os.environ.get("PUBLIC_DATA_BASE_URL", "https://example-r2-public-host"))
    parser.add_argument("--previous-manifest-url", default=os.environ.get("PREVIOUS_MANIFEST_URL"))
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--osrm-url", default=os.environ.get("OSRM_URL"))
    parser.add_argument("--osrm-profile", default=os.environ.get("OSRM_PROFILE", "bicycle"))
    parser.add_argument("--route-workers", type=int, default=int(os.environ.get("ROUTE_WORKERS", "8")))
    parser.add_argument("--allow-fallback-routes", action="store_true")
    parser.add_argument("--missing-station-trip-threshold", type=int, default=DEFAULT_MISSING_STATION_TRIP_THRESHOLD)
    parser.add_argument("--basemap-path", type=Path, default=Path(os.environ["BASEMAP_PATH"]) if os.environ.get("BASEMAP_PATH") else None)
    parser.add_argument("--publish", action="store_true")
    parser.add_argument("--r2-bucket", default=os.environ.get("R2_BUCKET"))
    parser.add_argument("--r2-endpoint-url", default=os.environ.get("R2_ENDPOINT_URL"))
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    package = fetch_json(CKAN_PACKAGE_URL)
    resource = find_2026_resource(package)
    previous_manifest_url = args.previous_manifest_url
    if not previous_manifest_url and args.public_base_url != "https://example-r2-public-host":
        previous_manifest_url = f"{args.public_base_url.rstrip('/')}/manifest/latest.json"
    if not args.force and previous_manifest_is_current(previous_manifest_url, resource):
        print("Ridership resource is unchanged; no new artifacts published.")
        return

    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    args.work_dir.mkdir(parents=True, exist_ok=True)
    args.output_dir.mkdir(parents=True, exist_ok=True)

    zip_path = args.work_dir / resource["name"]
    download_file(resource["url"], zip_path)

    raw = read_trip_zip(zip_path)
    trips = normalize_trips(raw)
    gbfs = fetch_gbfs_stations()
    overrides = load_manual_overrides(args.manual_overrides)
    stations = build_station_table(trips, gbfs, overrides, args.missing_station_trip_threshold)
    routes = generate_routes(trips, stations, args.osrm_url, args.osrm_profile, args.allow_fallback_routes, args.route_workers)
    validate_outputs(trips, routes, stations)

    run_dir = args.output_dir / "runs" / run_id
    write_partitioned_trips(trips, run_dir / "trips")
    write_parquet(routes, run_dir / "routes" / "routes.parquet")
    write_json(route_qa_summary(routes), run_dir / "routes" / "qa.json")
    write_parquet(stations, run_dir / "stations" / "stations.parquet")
    has_basemap = bool(args.basemap_path and args.basemap_path.exists())
    if has_basemap:
        (run_dir / "basemap").mkdir(parents=True, exist_ok=True)
        shutil.copyfile(args.basemap_path, run_dir / "basemap" / "toronto.pmtiles")
    manifest = build_manifest(resource, trips, run_id, args.public_base_url, has_basemap)
    write_json(manifest, run_dir / "manifest.json")
    latest_path = args.output_dir / "manifest" / "latest.json"
    write_json(manifest, latest_path)

    if args.publish:
        if not args.r2_bucket or not args.r2_endpoint_url:
            raise ValueError("--publish requires R2_BUCKET and R2_ENDPOINT_URL")
        upload_directory_to_r2(run_dir, args.r2_bucket, f"runs/{run_id}", args.r2_endpoint_url)
        upload_file_to_r2(latest_path, args.r2_bucket, "manifest/latest.json", args.r2_endpoint_url, "public, max-age=60")

    print(f"Generated run {run_id}: {len(trips)} trips, {len(routes)} routes, {len(stations)} stations")


if __name__ == "__main__":
    main()
