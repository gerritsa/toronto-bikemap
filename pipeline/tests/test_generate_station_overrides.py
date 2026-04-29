from bikeshare_pipeline.generate_station_overrides import (
    build_query_variants,
    candidate_road_names,
    confidence_from_score,
    resolve_intersection_exact,
    merge_overrides,
    score_result,
    station_query_kind,
)


def test_station_query_kind_classifies_common_station_names():
    assert station_query_kind("Simcoe St / Queen St W") == "intersection"
    assert station_query_kind("75 Holly St") == "address"
    assert station_query_kind("Finch West Subway Station") == "place"


def test_build_query_variants_normalizes_smart_suffix_and_toronto_scope():
    variants = build_query_variants("St. Joseph St / Bay St - SMART")

    assert any("Toronto, Ontario, Canada" in variant for variant in variants)
    assert any("St. Joseph St & Bay St" in variant for variant in variants)
    assert any("St. Joseph Street / Bay Street" in variant for variant in variants)
    assert all("- SMART" not in variant for variant in variants if "Toronto, Ontario, Canada" in variant)


def test_score_result_prefers_matching_intersection():
    matching = {
        "lat": "43.6519",
        "lon": "-79.3843",
        "display_name": "Simcoe Street & Queen Street West, Toronto, Ontario, Canada",
        "importance": 0.5,
        "address": {"city": "Toronto"},
        "namedetails": {},
    }
    non_matching = {
        "lat": "43.6500",
        "lon": "-79.3700",
        "display_name": "King Street East & Jarvis Street, Toronto, Ontario, Canada",
        "importance": 0.5,
        "address": {"city": "Toronto"},
        "namedetails": {},
    }

    assert score_result("Simcoe St / Queen St W", "intersection", matching) > score_result(
        "Simcoe St / Queen St W", "intersection", non_matching
    )
    assert confidence_from_score("intersection", score_result("Simcoe St / Queen St W", "intersection", matching)) == "high"


def test_merge_overrides_preserves_existing_station_ids():
    existing = [{"station_id": "7009", "name": "Existing", "lat": 1.0, "lon": 2.0, "capacity": None}]
    generated = [
        {"station_id": "7009", "name": "Generated", "lat": 3.0, "lon": 4.0, "capacity": None},
        {"station_id": "7012", "name": "New", "lat": 5.0, "lon": 6.0, "capacity": None},
    ]

    merged = merge_overrides(existing, generated)

    assert len(merged) == 2
    assert next(item for item in merged if item["station_id"] == "7009")["name"] == "Existing"
    assert next(item for item in merged if item["station_id"] == "7012")["name"] == "New"


def test_candidate_road_names_expands_common_abbreviations():
    candidates = candidate_road_names("Queen's Park Cres E")

    assert "Queen's Park Cres E" in candidates
    assert "Queen's Park Crescent East" in candidates


class FakeIntersectionResolver:
    def __init__(self, responses):
        self.responses = responses

    def query(self, road_a, road_b):
        return self.responses.get((road_a, road_b), {"elements": []})


def test_resolve_intersection_exact_uses_shared_node():
    resolver = FakeIntersectionResolver(
        {
            ("Yonge Street", "Wood Street"): {
                "elements": [{"type": "node", "id": 1, "lat": 43.6659, "lon": -79.3844}]
            }
        }
    )

    override, report = resolve_intersection_exact("Yonge St / Wood St", resolver)

    assert override is not None
    assert override["resolution"] == "geocoded_intersection_exact"
    assert round(override["lat"], 4) == 43.6659
    assert report["matched_pair"] == ["Yonge Street", "Wood Street"]
