from bikeshare_pipeline.routing import Point, deterministic_loop_waypoint, fallback_route, haversine_meters, loop_waypoint_distance_meters


def test_loop_waypoint_is_deterministic_and_not_station():
    station = Point(lon=-79.395954, lat=43.639832)
    first = deterministic_loop_waypoint("43682415", station, 1800)
    second = deterministic_loop_waypoint("43682415", station, 1800)
    assert first == second
    assert first != station


def test_loop_waypoint_distance_uses_half_duration_distance_at_12_5_kmh():
    assert round(loop_waypoint_distance_meters(1800), 1) == 3125.0
    assert loop_waypoint_distance_meters(60) == 500
    assert loop_waypoint_distance_meters(7200) == 5000


def test_loop_waypoint_distance_is_reflected_in_waypoint_location():
    station = Point(lon=-79.395954, lat=43.639832)
    waypoint = deterministic_loop_waypoint("43682415", station, 1800)
    assert 3000 <= haversine_meters([station, waypoint]) <= 3250


def test_fallback_route_encodes_points():
    encoded, distance, duration = fallback_route([Point(-79.39, 43.64), Point(-79.38, 43.65)])
    assert encoded
    assert distance > 0
    assert duration > 0
