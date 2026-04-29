from __future__ import annotations

CKAN_PACKAGE_URL = "https://ckan0.cf.opendata.inter.prod-toronto.ca/api/3/action/package_show?id=bike-share-toronto-ridership-data"
GBFS_STATION_INFORMATION_URL = "https://tor.publicbikesystem.net/ube/gbfs/v1/en/station_information"

BIKE_MODEL_ALIASES = {
    "ICONIC": "ICONIC",
    "EFIT": "EFIT",
    "EFIT G5": "EFIT",
    "ASTRO": "ASTRO",
}

BIKE_CATEGORIES = {
    "ICONIC": "Classic",
    "EFIT": "E-bike",
    "ASTRO": "E-bike",
}

TORONTO_BOUNDS = {
    "west": -79.65,
    "south": 43.55,
    "east": -79.12,
    "north": 43.86,
}

DEFAULT_MISSING_STATION_TRIP_THRESHOLD = 250
SYNTHETIC_LOOP_SPEED_KMH = 12.5
SYNTHETIC_LOOP_MIN_WAYPOINT_METERS = 500
SYNTHETIC_LOOP_MAX_WAYPOINT_METERS = 5000
