# Data And Operations

This document is the production runbook for generating, publishing, and deploying Toronto Bike Map data.

## System Overview

The app is static, but the data is generated offline and published to Cloudflare R2.

```text
Toronto Open Data trips
  + Bike Share Toronto GBFS station metadata
  + manual retired station overrides
  + local OSRM bicycle routing over Ontario OSM
    -> pipeline/output/runs/{run_id}
    -> Cloudflare R2
    -> Vercel app fetches manifest/latest.json
```

Important boundaries:

- OSRM is not part of the deployed app. It is a local preprocessing service used to generate route lines.
- R2 stores generated artifacts: manifest, trips, routes, and stations.
- The app loads one daily trip partition at a time, plus shared routes/stations.
- The default basemap is CARTO CDN. PMTiles/Protomaps is optional and not required for production.

## Data Sources

- Historical trips: Toronto Open Data package `bike-share-toronto-ridership-data`.
- Current station metadata: Bike Share Toronto GBFS `station_information`.
- Retired/missing station metadata: `pipeline/manual_overrides/stations.json`.
- Route geometry: local OSRM generated from an Ontario/Toronto OpenStreetMap extract using the bicycle profile.
- Basemap: CARTO CDN by default via MapLibre style JSON.
- Optional basemap: Protomaps PMTiles derived from OpenStreetMap.

Toronto Open Data publishes the year file incrementally. For example, the 2026 file can contain January-March only until later monthly releases are published.

## Cloudflare R2

### Bucket

Use one public R2 bucket for generated data. The current development setup uses:

```text
Bucket: toronto-bikemap
Public development URL: https://pub-67c940875b3145f7bc775de4bd649785.r2.dev
S3 endpoint: https://ca716f6307e90ed6b9aab3380266cb2a.r2.cloudflarestorage.com
```

For long-term production, prefer a custom domain over the `r2.dev` public development URL.

### R2 Credentials

Create an R2 API token with object read/write access for the bucket. The pipeline uses standard S3-compatible environment variables:

```bash
export PUBLIC_DATA_BASE_URL="https://<r2-public-host>"
export R2_BUCKET="toronto-bikemap"
export R2_ACCOUNT_ID="<cloudflare-account-id>"
export R2_ENDPOINT_URL="https://$R2_ACCOUNT_ID.r2.cloudflarestorage.com"
export AWS_ACCESS_KEY_ID="<r2-access-key-id>"
export AWS_SECRET_ACCESS_KEY="<r2-secret-access-key>"
```

Do not commit credentials.

### CORS

R2 must allow browser requests for Parquet files, including range requests:

```json
[
  {
    "AllowedOrigins": ["https://<your-vercel-domain>", "http://localhost:5173"],
    "AllowedMethods": ["GET", "HEAD"],
    "AllowedHeaders": ["Range", "Content-Type"],
    "ExposeHeaders": ["Accept-Ranges", "Content-Length", "Content-Range", "ETag"],
    "MaxAgeSeconds": 3600
  }
]
```

Add the real Vercel domain before production launch.

### R2 Object Layout

Generated data is published with immutable run prefixes:

```text
runs/{run_id}/manifest.json
runs/{run_id}/trips/year=YYYY/month=MM/day=DD/trips.parquet
runs/{run_id}/routes/routes.parquet
runs/{run_id}/routes/qa.json
runs/{run_id}/stations/stations.parquet
manifest/latest.json
```

If PMTiles is used later, the pipeline can also publish:

```text
runs/{run_id}/basemap/toronto.pmtiles
```

`manifest/latest.json` is the only mutable object. Cache it briefly. Run artifacts should be immutable and cacheable for a long time.

## Basemap Strategy

### Default: CARTO CDN

No self-hosted basemap is required for the current production path.

If `manifest.assets.basemapUrl` is empty, the app uses the CARTO dark basemap:

```text
https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json
```

This is similar to the Switch chargermap approach, which uses CARTO's hosted `positron` style.

### Optional: Protomaps PMTiles

Use this only if you want to self-host a Toronto basemap in R2.

```bash
pmtiles extract \
  https://build.protomaps.com/<build>.pmtiles \
  pipeline/work/toronto.pmtiles \
  --bbox=-79.65,43.55,-79.12,43.86

export BASEMAP_PATH="$PWD/pipeline/work/toronto.pmtiles"
```

Then include `--basemap-path "$BASEMAP_PATH"` in the pipeline run.

If `BASEMAP_PATH` is unset or the file does not exist, the manifest publishes `basemapUrl: ""`, and the app uses CARTO.

## OSRM / OpenStreetMap Routing

OSRM generates plausible bicycle route geometry between station pairs. The raw Toronto trip data contains start/end stations and times, but not the exact path ridden.

Pipeline route generation flow:

```text
start station coordinates + end station coordinates
  -> OSRM bicycle route API
  -> encoded polyline, distance, duration estimate
  -> routes.parquet
```

The app later animates trips along the saved route polylines. The deployed app never calls OSRM.

### Prepare OSRM Locally

Start Docker Desktop first.

Download an Ontario OSM extract:

```bash
cd "/Users/abelgerrits/Toronto Bike Map"
mkdir -p pipeline/work/osrm
cd pipeline/work/osrm

curl -L -o ontario-latest.osm.pbf \
  https://download.geofabrik.de/north-america/canada/ontario-latest.osm.pbf
```

On Apple Silicon, force the OSRM image to run as `linux/amd64`:

```bash
docker run --platform linux/amd64 -t -v "$PWD:/data" ghcr.io/project-osrm/osrm-backend \
  osrm-extract -p /opt/bicycle.lua /data/ontario-latest.osm.pbf

docker run --platform linux/amd64 -t -v "$PWD:/data" ghcr.io/project-osrm/osrm-backend \
  osrm-partition /data/ontario-latest.osrm

docker run --platform linux/amd64 -t -v "$PWD:/data" ghcr.io/project-osrm/osrm-backend \
  osrm-customize /data/ontario-latest.osrm
```

Start the local OSRM server. Use port `5001` if `5000` is already taken:

```bash
docker run --platform linux/amd64 -t -i -p 5001:5000 -v "$PWD:/data" ghcr.io/project-osrm/osrm-backend \
  osrm-routed --algorithm mld /data/ontario-latest.osrm
```

Leave that terminal running while the pipeline runs.

Test OSRM:

```bash
curl "http://localhost:5001/route/v1/bicycle/-79.395954,43.639832;-79.3832,43.6532?overview=false"
```

Expected:

```json
{"code":"Ok"}
```

## Manual Station Overrides

Historical trips can reference retired stations missing from current GBFS. Add high-volume missing real stations to:

```text
pipeline/manual_overrides/stations.json
```

Format:

```json
[
  {
    "station_id": "7020",
    "name": "Retired station name",
    "lat": 43.0,
    "lon": -79.0,
    "capacity": null
  }
]
```

Use source-backed coordinates, such as Bike Share Toronto records, OpenStreetMap/Mapcarta, or another reliable map source.

Do not add fake coordinates for internal/test/warehouse stations. The pipeline filters internal/test/warehouse trips and blank station IDs.

## Running The Pipeline

Run without publishing first:

```bash
cd "/Users/abelgerrits/Toronto Bike Map"

PYTHONPATH=pipeline .venv/bin/python -m bikeshare_pipeline.run \
  --osrm-url http://localhost:5001 \
  --osrm-profile bicycle \
  --route-workers 8 \
  --allow-fallback-routes \
  --public-base-url "$PUBLIC_DATA_BASE_URL" \
  --force
```

Notes:

- `--route-workers 8` parallelizes OSRM calls. Increase carefully if OSRM is stable; decrease if Docker/OSRM becomes overloaded.
- `--allow-fallback-routes` allows the run to finish if a route request fails, but QA should still be checked before publishing.
- Omit `--basemap-path` unless you intentionally generated PMTiles.

After the run completes, inspect:

```bash
cat pipeline/output/runs/{run_id}/routes/qa.json
```

Production-quality target:

```json
{
  "fallbackStraightLineCount": 0,
  "missingStationRouteCount": 0
}
```

If `missingStationRouteCount` is nonzero, identify missing station IDs and either add real manual overrides or filter invalid internal/test data.

## Publishing To R2

After QA passes:

```bash
PYTHONPATH=pipeline .venv/bin/python -m bikeshare_pipeline.run \
  --osrm-url http://localhost:5001 \
  --osrm-profile bicycle \
  --route-workers 8 \
  --allow-fallback-routes \
  --public-base-url "$PUBLIC_DATA_BASE_URL" \
  --force \
  --publish
```

The publish step uploads the run directory and updates:

```text
manifest/latest.json
```

Verify:

```bash
curl "$PUBLIC_DATA_BASE_URL/manifest/latest.json"
```

The manifest should include:

```json
{
  "assets": {
    "tripsBaseUrl": "https://<r2-public-host>/runs/{run_id}/trips",
    "routesUrl": "https://<r2-public-host>/runs/{run_id}/routes/routes.parquet",
    "stationsUrl": "https://<r2-public-host>/runs/{run_id}/stations/stations.parquet",
    "basemapUrl": ""
  }
}
```

`basemapUrl: ""` is expected when using CARTO CDN.

## Vercel Deployment

Create a Vercel project for the `app/` directory.

Settings:

```text
Root Directory: app
Install Command: pnpm install --frozen-lockfile
Build Command: pnpm build
Output Directory: dist
```

Environment variable:

```text
VITE_MANIFEST_URL=https://<r2-public-host>/manifest/latest.json
```

After deployment:

- Add the Vercel production domain to the R2 CORS `AllowedOrigins`.
- Open the Vercel app.
- Confirm the date dropdown reflects the published source date range.
- Confirm trips animate.
- Confirm station and trip popups work.

## Monthly Refresh Checklist

Run this when Toronto publishes a new monthly update.

1. Check whether the Toronto Open Data `bikeshare-ridership-2026.zip` resource changed.
2. Start Docker Desktop.
3. Start the existing local OSRM server from `pipeline/work/osrm`.
4. If the OSM extract is old, optionally refresh `ontario-latest.osm.pbf` and rebuild OSRM with `osrm-extract`, `osrm-partition`, and `osrm-customize`.
5. Run the pipeline without `--publish`.
6. If the pipeline reports high-volume missing station IDs, add real coordinates to `pipeline/manual_overrides/stations.json`.
7. Re-run until `routes/qa.json` reports `fallbackStraightLineCount: 0` and `missingStationRouteCount: 0`.
8. Run verification:

```bash
pnpm build
PYTHONPYCACHEPREFIX=pipeline/.pycache .venv/bin/python -m compileall pipeline
PYTHONPATH=pipeline .venv/bin/pytest pipeline/tests
```

9. Run the pipeline with `--publish`.
10. Verify `curl "$PUBLIC_DATA_BASE_URL/manifest/latest.json"`.
11. Check the app locally:

```bash
VITE_MANIFEST_URL="$PUBLIC_DATA_BASE_URL/manifest/latest.json" pnpm dev
```

12. Deploy Vercel if app code changed. If only R2 data changed, Vercel does not need redeploying because the app reads `manifest/latest.json`.

## Operational Notes

- The R2 run directory for a typical January-March 2026 dataset is roughly tens of MB, not hundreds of MB.
- Users do not download all trips at once. The app loads manifest, shared stations/routes, and one daily trip partition.
- `routes.parquet` is currently shared across the run. If it becomes too large later, split routes by day or by route IDs referenced by the selected day.
- DuckDB WASM workers and WASM binaries are bundled by Vite so they are served from the app origin, avoiding cross-origin worker failures.
