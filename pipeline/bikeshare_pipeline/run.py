from __future__ import annotations

import argparse
import os
import shutil
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests

from .analytics import build_daily_analytics, build_hourly_analytics
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
from .stations import build_missing_station_candidates, build_station_table, fetch_gbfs_stations, load_manual_overrides


DEFAULT_PUBLISH_YEARS = (2026,)


def find_year_resources(package: dict, years: list[int]) -> list[dict]:
    resources = package["result"]["resources"]
    resources_by_name = {resource["name"]: resource for resource in resources}
    matches: list[dict] = []
    missing: list[str] = []
    for year in years:
        name = f"bikeshare-ridership-{year}.zip"
        resource = resources_by_name.get(name)
        if not resource:
            missing.append(name)
            continue
        matches.append(resource)
    if missing:
        raise ValueError(f"Could not find ridership resources in CKAN package: {missing}")
    return matches


def build_source_metadata(resources: list[dict]) -> dict:
    latest_resource = max(resources, key=lambda resource: resource["name"])
    return {
        "ckanPackageId": "bike-share-toronto-ridership-data",
        "ridershipResourceName": latest_resource["name"],
        "ridershipUrl": latest_resource["url"],
        "ridershipLastModified": latest_resource.get("last_modified", ""),
        "ridershipSizeBytes": int(latest_resource.get("size") or 0),
        "ridershipResources": [
            {
                "year": int(resource["name"].removeprefix("bikeshare-ridership-").removesuffix(".zip")),
                "name": resource["name"],
                "url": resource["url"],
                "lastModified": resource.get("last_modified", ""),
                "sizeBytes": int(resource.get("size") or 0),
            }
            for resource in sorted(resources, key=lambda resource: resource["name"])
        ],
    }


def build_manifest(resources: list[dict], trips: pd.DataFrame, run_id: str, public_base_url: str, has_basemap: bool) -> dict:
    dates = sorted(trips["service_date"].unique().tolist())
    run_base = f"{public_base_url.rstrip('/')}/runs/{run_id}"
    source = build_source_metadata(resources)
    source.update(
        {
            "tripCount": int(len(trips)),
            "dateMin": dates[0] if dates else "",
            "dateMax": dates[-1] if dates else "",
        }
    )
    return {
        "runId": run_id,
        "generatedAt": datetime.now(timezone.utc).isoformat(),
        "source": source,
        "assets": {
            "tripsBaseUrl": f"{run_base}/trips",
            "routesUrl": f"{run_base}/routes/routes.parquet",
            "stationsUrl": f"{run_base}/stations/stations.parquet",
            "basemapUrl": f"{run_base}/basemap/toronto.pmtiles" if has_basemap else "",
        },
        "analytics": {
            "dailyUrl": f"{run_base}/analytics/daily.parquet",
            "hourlyUrl": f"{run_base}/analytics/hourly.parquet",
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


def previous_manifest_is_current(previous_manifest_url: str | None, resources: list[dict]) -> bool:
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
    previous_resources = source.get("ridershipResources")
    current_resources = build_source_metadata(resources)["ridershipResources"]
    if isinstance(previous_resources, list):
        return previous_resources == current_resources
    if len(resources) != 1:
        return False

    resource = resources[0]
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
    parser.add_argument("--years", nargs="+", type=int, default=list(DEFAULT_PUBLISH_YEARS))
    parser.add_argument("--publish", action="store_true")
    parser.add_argument("--r2-bucket", default=os.environ.get("R2_BUCKET"))
    parser.add_argument("--r2-endpoint-url", default=os.environ.get("R2_ENDPOINT_URL"))
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    package = fetch_json(CKAN_PACKAGE_URL)
    years = sorted(set(args.years))
    resources = find_year_resources(package, years)
    previous_manifest_url = args.previous_manifest_url
    if not previous_manifest_url and args.public_base_url != "https://example-r2-public-host":
        previous_manifest_url = f"{args.public_base_url.rstrip('/')}/manifest/latest.json"
    if not args.force and previous_manifest_is_current(previous_manifest_url, resources):
        print("Ridership resource is unchanged; no new artifacts published.")
        return

    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    args.work_dir.mkdir(parents=True, exist_ok=True)
    args.output_dir.mkdir(parents=True, exist_ok=True)

    trip_frames: list[pd.DataFrame] = []
    for resource in resources:
        zip_path = args.work_dir / resource["name"]
        download_file(resource["url"], zip_path)
        raw = read_trip_zip(zip_path)
        trip_frames.append(normalize_trips(raw))
    trips = pd.concat(trip_frames, ignore_index=True)
    gbfs = fetch_gbfs_stations()
    overrides = load_manual_overrides(args.manual_overrides)
    try:
        stations = build_station_table(trips, gbfs, overrides, args.missing_station_trip_threshold)
    except ValueError as error:
        if str(error).startswith("Missing station coordinates exceed threshold."):
            stations_so_far = pd.concat([frame for frame in (gbfs, overrides) if not frame.empty], ignore_index=True)
            known_ids = set(stations_so_far["station_id"])
            start_counts = trips["start_station_id"].value_counts()
            end_counts = trips["end_station_id"].value_counts()
            affected_counts = start_counts.add(end_counts, fill_value=0).astype(int)
            missing = affected_counts[[station_id not in known_ids and station_id != "" for station_id in affected_counts.index]]
            blocking = missing[missing > args.missing_station_trip_threshold]
            candidate_path = args.output_dir / "missing_station_candidates.json"
            write_json({"candidates": build_missing_station_candidates(trips, stations_so_far, blocking)}, candidate_path)
            raise ValueError(f"{error}\nCandidate override file written to: {candidate_path}") from error
        raise
    routes = generate_routes(trips, stations, args.osrm_url, args.osrm_profile, args.allow_fallback_routes, args.route_workers)
    validate_outputs(trips, routes, stations)
    daily_analytics = build_daily_analytics(trips, routes)
    hourly_analytics = build_hourly_analytics(trips)

    run_dir = args.output_dir / "runs" / run_id
    write_partitioned_trips(trips, run_dir / "trips")
    write_parquet(routes, run_dir / "routes" / "routes.parquet")
    write_json(route_qa_summary(routes), run_dir / "routes" / "qa.json")
    write_parquet(stations, run_dir / "stations" / "stations.parquet")
    write_parquet(daily_analytics, run_dir / "analytics" / "daily.parquet")
    write_parquet(hourly_analytics, run_dir / "analytics" / "hourly.parquet")
    has_basemap = bool(args.basemap_path and args.basemap_path.exists())
    if has_basemap:
        (run_dir / "basemap").mkdir(parents=True, exist_ok=True)
        shutil.copyfile(args.basemap_path, run_dir / "basemap" / "toronto.pmtiles")
    manifest = build_manifest(resources, trips, run_id, args.public_base_url, has_basemap)
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
