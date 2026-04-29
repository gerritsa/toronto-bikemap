from __future__ import annotations

import argparse
import json
import os
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests

from .config import TORONTO_BOUNDS
from .io import write_json
from .stations import canonical_station_name, normalize_station_name

NOMINATIM_SEARCH_URL = "https://nominatim.openstreetmap.org/search"
OVERPASS_API_URL = "https://overpass-api.de/api/interpreter"
DEFAULT_USER_AGENT = "TorontoBikeMapStationGeocoder/1.0 (historical station override generation)"
TOKEN_ALIASES = {
    "st": "street",
    "ave": "avenue",
    "rd": "road",
    "blvd": "boulevard",
    "cres": "crescent",
    "sq": "square",
    "pl": "place",
    "w": "west",
    "e": "east",
    "n": "north",
    "s": "south",
}
ROAD_SUFFIX_ALIASES = {
    "st": "Street",
    "ave": "Avenue",
    "rd": "Road",
    "blvd": "Boulevard",
    "cres": "Crescent",
    "sq": "Square",
    "pl": "Place",
}
DIRECTION_ALIASES = {
    "e": "East",
    "w": "West",
    "n": "North",
    "s": "South",
}
SPECIAL_QUERY_ALIASES = {
    "Finch West Subway Station": ["Finch West Station"],
    "Centre Island Ferry Dock": ["Centre Island ferry dock", "Centre Island Dock"],
}


def strip_station_suffixes(name: str) -> str:
    text = normalize_station_name(name)
    text = re.sub(r"\s*-\s*smart$", "", text, flags=re.IGNORECASE)
    return text.strip()


def strip_parenthetical(name: str) -> str:
    return normalize_station_name(re.sub(r"\s*\([^)]*\)", "", name)).strip()


def station_query_kind(name: str) -> str:
    clean_name = strip_station_suffixes(name)
    if re.match(r"^\d+\s+", clean_name):
        return "address"
    if " / " in clean_name or " & " in clean_name:
        return "intersection"
    return "place"


def is_island_query(name: str) -> bool:
    name_key = canonical_station_name(name)
    return any(token in name_key for token in ("hanlans", "wards", "gibraltar", "centre island", "ferry dock", "beach"))


def expand_segment_abbreviations(segment: str) -> str:
    tokens = normalize_station_name(segment).split()
    if not tokens:
        return ""

    last_index = len(tokens) - 1
    if tokens[last_index].rstrip(".").lower() in DIRECTION_ALIASES:
        tokens[last_index] = DIRECTION_ALIASES[tokens[last_index].rstrip(".").lower()]
        last_index -= 1
    if last_index >= 0 and tokens[last_index].rstrip(".").lower() in ROAD_SUFFIX_ALIASES:
        tokens[last_index] = ROAD_SUFFIX_ALIASES[tokens[last_index].rstrip(".").lower()]
    return " ".join(tokens)


def expanded_seed_variants(seed: str, kind: str) -> list[str]:
    variants = [seed]
    if kind == "intersection":
        separators = (" / ", " & ", " and ")
        for separator in separators:
            if separator in seed:
                parts = [expand_segment_abbreviations(part) for part in seed.split(separator)]
                expanded = " / ".join(parts)
                if expanded not in variants:
                    variants.append(expanded)
                variants.extend(
                    candidate
                    for candidate in (
                        expanded.replace(" / ", " & "),
                        expanded.replace(" / ", " and "),
                    )
                    if candidate not in variants
                )
                break
    elif kind == "address":
        address_match = re.match(r"^(\d+)\s+(.+)$", seed)
        if address_match:
            house_number, street = address_match.groups()
            expanded = f"{house_number} {expand_segment_abbreviations(street)}"
            if expanded not in variants:
                variants.append(expanded)
    for alias in SPECIAL_QUERY_ALIASES.get(seed, []):
        if alias not in variants:
            variants.append(alias)
    return variants


def build_query_variants(name: str) -> list[str]:
    clean = strip_station_suffixes(name)
    unparenthesized = strip_parenthetical(clean)
    seeds: list[str] = []
    for seed in (clean, unparenthesized):
        if seed and seed not in seeds:
            seeds.append(seed)

    scope_suffixes = [", Toronto, Ontario, Canada"]
    if is_island_query(name):
        scope_suffixes.insert(0, ", Toronto Islands, Toronto, Ontario, Canada")

    variants: list[str] = []
    kind = station_query_kind(name)
    for seed in seeds:
        seed_variants = expanded_seed_variants(seed, kind)
        if kind == "intersection":
            seed_variants.extend(
                candidate
                for candidate in (
                    seed.replace(" / ", " & "),
                    seed.replace(" / ", " and "),
                )
                if candidate != seed
            )
        for seed_variant in seed_variants:
            for suffix in scope_suffixes:
                query = f"{seed_variant}{suffix}"
                if query not in variants:
                    variants.append(query)
    return variants


def within_toronto_bounds(lat: float, lon: float) -> bool:
    return (
        TORONTO_BOUNDS["south"] <= lat <= TORONTO_BOUNDS["north"]
        and TORONTO_BOUNDS["west"] <= lon <= TORONTO_BOUNDS["east"]
    )


def normalize_match_text(value: str) -> str:
    tokens = canonical_station_name(value).split()
    return " ".join(TOKEN_ALIASES.get(token, token) for token in tokens)


def canonical_text_blob(result: dict[str, Any]) -> str:
    address = result.get("address") or {}
    namedetails = result.get("namedetails") or {}
    text_parts = [result.get("display_name", "")]
    text_parts.extend(str(value) for value in address.values())
    text_parts.extend(str(value) for value in namedetails.values())
    return normalize_match_text(" ".join(text_parts))


def intersection_parts(name: str) -> list[str]:
    clean_name = strip_parenthetical(strip_station_suffixes(name))
    for separator in (" / ", " & ", " and "):
        if separator in clean_name:
            return [normalize_station_name(part) for part in clean_name.split(separator) if normalize_station_name(part)]
    return [clean_name]


def expanded_intersection_parts(name: str) -> list[str]:
    return [expand_segment_abbreviations(part) for part in intersection_parts(name)]


def score_result(name: str, kind: str, result: dict[str, Any]) -> float:
    lat = float(result["lat"])
    lon = float(result["lon"])
    if not within_toronto_bounds(lat, lon):
        return -1.0

    blob = canonical_text_blob(result)
    score = 0.0
    importance = float(result.get("importance") or 0.0)
    address = result.get("address") or {}
    city_fields = " ".join(str(address.get(field, "")) for field in ("city", "town", "municipality", "suburb", "county"))
    if "toronto" in canonical_station_name(city_fields):
        score += 1.0

    if kind == "intersection":
        parts = [canonical_station_name(part) for part in intersection_parts(name)]
        parts = [normalize_match_text(part) for part in parts]
        matches = sum(1 for part in parts if part and part in blob)
        score += matches * 3.0
        if matches == len(parts) and len(parts) >= 2:
            score += 2.0
        if result.get("category") == "highway":
            score += 0.5
    elif kind == "address":
        clean_name = strip_parenthetical(strip_station_suffixes(name))
        house_number_match = re.match(r"^(\d+)\s+(.+)$", clean_name)
        if house_number_match:
            house_number, street = house_number_match.groups()
            if house_number in blob:
                score += 3.0
            if normalize_match_text(street) in blob:
                score += 3.0
    else:
        name_key = normalize_match_text(strip_parenthetical(strip_station_suffixes(name)))
        blob_tokens = set(blob.split())
        name_tokens = {token for token in name_key.split() if token}
        if name_key and name_key in blob:
            score += 5.0
        elif name_tokens:
            overlap = len(name_tokens & blob_tokens) / len(name_tokens)
            score += overlap * 4.0

    score += min(importance, 1.0)
    return score


def confidence_from_score(kind: str, score: float) -> str | None:
    if kind == "intersection":
        if score >= 8.0:
            return "high"
        if score >= 6.0:
            return "medium"
        return None
    if kind == "address":
        if score >= 7.0:
            return "high"
        if score >= 5.0:
            return "medium"
        return None
    if score >= 6.0:
        return "high"
    if score >= 4.5:
        return "medium"
    return None


class NominatimGeocoder:
    def __init__(self, cache_path: Path, user_agent: str, email: str | None, delay_seconds: float) -> None:
        self.cache_path = cache_path
        self.user_agent = user_agent
        self.email = email
        self.delay_seconds = delay_seconds
        self._last_request_at = 0.0
        if cache_path.exists():
            self.cache: dict[str, list[dict[str, Any]]] = json.loads(cache_path.read_text())
        else:
            self.cache = {}

    def search(self, query: str) -> list[dict[str, Any]]:
        cached = self.cache.get(query)
        if cached is not None:
            return cached

        elapsed = time.monotonic() - self._last_request_at
        if elapsed < self.delay_seconds:
            time.sleep(self.delay_seconds - elapsed)

        params = {
            "q": query,
            "format": "jsonv2",
            "limit": 5,
            "addressdetails": 1,
            "namedetails": 1,
            "countrycodes": "ca",
            "viewbox": f"{TORONTO_BOUNDS['west']},{TORONTO_BOUNDS['north']},{TORONTO_BOUNDS['east']},{TORONTO_BOUNDS['south']}",
            "bounded": 1,
        }
        if self.email:
            params["email"] = self.email
        response = requests.get(
            NOMINATIM_SEARCH_URL,
            params=params,
            headers={"User-Agent": self.user_agent},
            timeout=30,
        )
        self._last_request_at = time.monotonic()
        response.raise_for_status()
        results = response.json()
        self.cache[query] = results
        return results

    def save(self) -> None:
        self.cache_path.parent.mkdir(parents=True, exist_ok=True)
        self.cache_path.write_text(json.dumps(self.cache, indent=2, sort_keys=True))


class OverpassIntersectionResolver:
    def __init__(self, cache_path: Path, user_agent: str, delay_seconds: float) -> None:
        self.cache_path = cache_path
        self.user_agent = user_agent
        self.delay_seconds = delay_seconds
        self._last_request_at = 0.0
        if cache_path.exists():
            self.cache: dict[str, dict[str, Any]] = json.loads(cache_path.read_text())
        else:
            self.cache = {}
        self.rate_limited = False

    def query(self, road_a: str, road_b: str) -> dict[str, Any]:
        cache_key = f"{road_a}|||{road_b}"
        cached = self.cache.get(cache_key)
        if cached is not None:
            return cached
        if self.rate_limited:
            return {"elements": [], "_rate_limited": True}

        elapsed = time.monotonic() - self._last_request_at
        if elapsed < self.delay_seconds:
            time.sleep(self.delay_seconds - elapsed)

        query = f"""
[out:json][timeout:25][bbox:{TORONTO_BOUNDS['south']},{TORONTO_BOUNDS['west']},{TORONTO_BOUNDS['north']},{TORONTO_BOUNDS['east']}];
way[highway][name="{road_a}"]->.w1;
way[highway][name="{road_b}"]->.w2;
node(w.w1)(w.w2);
out body;
""".strip()
        response = requests.post(
            OVERPASS_API_URL,
            data=query,
            headers={"User-Agent": self.user_agent},
            timeout=60,
        )
        self._last_request_at = time.monotonic()
        if response.status_code == 429:
            self.rate_limited = True
            payload = {"elements": [], "_rate_limited": True}
            self.cache[cache_key] = payload
            return payload
        response.raise_for_status()
        payload = response.json()
        self.cache[cache_key] = payload
        return payload

    def save(self) -> None:
        self.cache_path.parent.mkdir(parents=True, exist_ok=True)
        self.cache_path.write_text(json.dumps(self.cache, indent=2, sort_keys=True))


def candidate_road_names(part: str) -> list[str]:
    base = normalize_station_name(part)
    expanded = expand_segment_abbreviations(base)
    candidates = [base]
    if expanded not in candidates:
        candidates.append(expanded)
    for alias in SPECIAL_QUERY_ALIASES.get(base, []):
        if alias not in candidates:
            candidates.append(alias)
    return candidates


def resolve_intersection_exact(name: str, resolver: OverpassIntersectionResolver) -> tuple[dict[str, Any] | None, dict[str, Any]]:
    parts = expanded_intersection_parts(name)
    if len(parts) != 2:
        return None, {"method": "overpass_exact", "attempted_pairs": []}

    road_a_candidates = candidate_road_names(parts[0])
    road_b_candidates = candidate_road_names(parts[1])
    attempted_pairs: list[list[str]] = []
    best_elements: list[dict[str, Any]] = []
    best_pair: tuple[str, str] | None = None
    rate_limited = False
    for road_a in road_a_candidates:
        for road_b in road_b_candidates:
            attempted_pairs.append([road_a, road_b])
            payload = resolver.query(road_a, road_b)
            if payload.get("_rate_limited"):
                rate_limited = True
                continue
            elements = payload.get("elements", [])
            node_elements = [element for element in elements if element.get("type") == "node" and "lat" in element and "lon" in element]
            if not node_elements:
                continue
            if best_pair is None or len(node_elements) < len(best_elements):
                best_pair = (road_a, road_b)
                best_elements = node_elements
                if len(node_elements) == 1:
                    break
        if best_pair and len(best_elements) == 1:
            break

    if not best_pair or not best_elements:
        return None, {"method": "overpass_exact", "attempted_pairs": attempted_pairs, "rate_limited": rate_limited}

    lat = sum(float(element["lat"]) for element in best_elements) / len(best_elements)
    lon = sum(float(element["lon"]) for element in best_elements) / len(best_elements)
    confidence = "high" if len(best_elements) == 1 else "medium"
    return (
        {
            "lat": lat,
            "lon": lon,
            "matched_name": f"{best_pair[0]} / {best_pair[1]}",
            "resolution": "geocoded_intersection_exact",
            "confidence": confidence,
            "source": "generated_geocoder:overpass",
            "geocode_query": attempted_pairs,
        },
        {
            "method": "overpass_exact",
            "attempted_pairs": attempted_pairs,
            "matched_pair": [best_pair[0], best_pair[1]],
            "matched_node_count": len(best_elements),
            "confidence": confidence,
        },
    )


def resolve_candidate(
    candidate: dict[str, Any], geocoder: NominatimGeocoder, intersection_resolver: OverpassIntersectionResolver
) -> tuple[dict[str, Any] | None, dict[str, Any]]:
    station_id = candidate["station_id"]
    primary_name = candidate["primary_name"]
    trip_count = int(candidate["trip_count"])

    if candidate.get("suggested_match"):
        match = candidate["suggested_match"]
        override = {
            "station_id": station_id,
            "name": primary_name,
            "lat": float(match["lat"]),
            "lon": float(match["lon"]),
            "capacity": match.get("capacity"),
            "source": f"generated_suggested_match:{match['station_id']}",
            "confidence": "high",
            "resolution": "suggested_match",
            "matched_name": match["name"],
        }
        return override, {
            "station_id": station_id,
            "primary_name": primary_name,
            "trip_count": trip_count,
            "resolution": "suggested_match",
            "confidence": "high",
            "matched_name": match["name"],
        }

    kind = station_query_kind(primary_name)
    if kind == "intersection":
        exact_override, exact_report = resolve_intersection_exact(primary_name, intersection_resolver)
        if exact_override is not None:
            override = {
                "station_id": station_id,
                "name": primary_name,
                "lat": float(exact_override["lat"]),
                "lon": float(exact_override["lon"]),
                "capacity": None,
                "source": exact_override["source"],
                "confidence": exact_override["confidence"],
                "resolution": exact_override["resolution"],
                "geocode_query": exact_override["geocode_query"],
                "matched_name": exact_override["matched_name"],
            }
            report = {
                "station_id": station_id,
                "primary_name": primary_name,
                "trip_count": trip_count,
                "query_kind": kind,
                "resolution": override["resolution"],
                "confidence": override["confidence"],
                "matched_name": override["matched_name"],
                "attempted_pairs": exact_report["attempted_pairs"],
                "matched_pair": exact_report["matched_pair"],
                "matched_node_count": exact_report["matched_node_count"],
            }
            return override, report

    best_result: dict[str, Any] | None = None
    best_query = ""
    best_score = -1.0
    best_confidence: str | None = None
    attempted_queries: list[str] = []
    for query in build_query_variants(primary_name):
        attempted_queries.append(query)
        results = geocoder.search(query)
        for result in results:
            score = score_result(primary_name, kind, result)
            confidence = confidence_from_score(kind, score)
            if score > best_score:
                best_result = result
                best_query = query
                best_score = score
                best_confidence = confidence

    report = {
        "station_id": station_id,
        "primary_name": primary_name,
        "trip_count": trip_count,
        "query_kind": kind,
        "attempted_queries": attempted_queries,
        "best_query": best_query,
        "best_score": round(best_score, 3) if best_score >= 0 else None,
        "confidence": best_confidence,
    }

    if not best_result or not best_confidence:
        return None, report

    override = {
        "station_id": station_id,
        "name": primary_name,
        "lat": float(best_result["lat"]),
        "lon": float(best_result["lon"]),
        "capacity": None,
        "source": "generated_geocoder:nominatim",
        "confidence": best_confidence,
        "resolution": f"geocoded_{kind}",
        "geocode_query": best_query,
        "matched_name": best_result.get("display_name", ""),
    }
    report["matched_name"] = best_result.get("display_name", "")
    report["resolution"] = override["resolution"]
    return override, report


def merge_overrides(existing: list[dict[str, Any]], generated: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_station_id = {str(item.get("station_id", "")): item for item in existing}
    for item in generated:
        station_id = str(item["station_id"])
        if station_id not in by_station_id:
            by_station_id[station_id] = item
    return [by_station_id[key] for key in sorted(by_station_id)]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate historical station overrides from missing station candidates.")
    parser.add_argument("--candidate-file", type=Path, default=Path("pipeline/output/missing_station_candidates.json"))
    parser.add_argument("--output-file", type=Path, default=Path("pipeline/output/generated_station_overrides.json"))
    parser.add_argument("--report-file", type=Path, default=Path("pipeline/output/generated_station_override_report.json"))
    parser.add_argument("--cache-file", type=Path, default=Path("pipeline/work/geocode_cache.json"))
    parser.add_argument("--intersection-cache-file", type=Path, default=Path("pipeline/work/intersection_cache.json"))
    parser.add_argument("--manual-overrides", type=Path, default=Path("pipeline/manual_overrides/stations.json"))
    parser.add_argument("--merge-into", type=Path)
    parser.add_argument("--user-agent", default=os.environ.get("NOMINATIM_USER_AGENT", DEFAULT_USER_AGENT))
    parser.add_argument("--email", default=os.environ.get("NOMINATIM_EMAIL"))
    parser.add_argument("--delay-seconds", type=float, default=1.1)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    payload = json.loads(args.candidate_file.read_text())
    candidates = payload.get("candidates", [])
    existing_overrides = json.loads(args.manual_overrides.read_text()) if args.manual_overrides.exists() else []
    existing_ids = {str(item.get("station_id", "")) for item in existing_overrides}

    geocoder = NominatimGeocoder(args.cache_file, args.user_agent, args.email, args.delay_seconds)
    intersection_resolver = OverpassIntersectionResolver(args.intersection_cache_file, args.user_agent, args.delay_seconds)
    try:
        generated: list[dict[str, Any]] = []
        resolved_report: list[dict[str, Any]] = []
        unresolved_report: list[dict[str, Any]] = []
        for candidate in candidates:
            if candidate["station_id"] in existing_ids:
                continue
            override, report = resolve_candidate(candidate, geocoder, intersection_resolver)
            if override is None:
                unresolved_report.append(report)
                continue
            generated.append(override)
            resolved_report.append(report)
    finally:
        geocoder.save()
        intersection_resolver.save()

    generated = sorted(generated, key=lambda item: item["station_id"])
    args.output_file.parent.mkdir(parents=True, exist_ok=True)
    args.output_file.write_text(json.dumps(generated, indent=2, sort_keys=True))

    summary = {
        "generatedAt": datetime.now(timezone.utc).isoformat(),
        "counts": {
            "candidateCount": len(candidates),
            "existingOverrideCount": len(existing_overrides),
            "generatedOverrideCount": len(generated),
            "unresolvedCount": len(unresolved_report),
        },
        "resolved": resolved_report,
        "unresolved": unresolved_report,
    }
    write_json(summary, args.report_file)

    if args.merge_into:
        merged = merge_overrides(existing_overrides, generated)
        args.merge_into.parent.mkdir(parents=True, exist_ok=True)
        args.merge_into.write_text(json.dumps(merged, indent=2, sort_keys=True))

    print(
        f"Generated {len(generated)} station overrides; {len(unresolved_report)} unresolved. "
        f"Override file: {args.output_file}"
    )


if __name__ == "__main__":
    main()
