"""Exact geometry must scale with local segments, without sampling the route."""

import math
import random

import pytest

from dieselup.clients.routing import RoutingError
from dieselup.core import remaining_route as route
from dieselup.core.optimizer import haversine_miles


def linear_projection(shape, point):
    """The original full scan is the independent compatibility oracle."""
    best = None
    walked = 0.0
    for index, (a, b) in enumerate(zip(shape, shape[1:])):
        length = haversine_miles(*a, *b)
        if length <= 1e-9:
            continue
        scale = math.cos(math.radians((a[0] + b[0]) / 2))
        dx, dy = (b[1] - a[1]) * scale, b[0] - a[0]
        px, py = (point[1] - a[1]) * scale, point[0] - a[0]
        t = (px * dx + py * dy) / (dx * dx + dy * dy)
        clipped = min(1.0, max(0.0, t))
        q = (a[0] + clipped * (b[0] - a[0]), a[1] + clipped * (b[1] - a[1]))
        distance = haversine_miles(*point, *q)
        progress = walked + clipped * length
        outside = (index == 0 and t < 0) or (index == len(shape) - 2 and t > 1)
        outside = outside or (progress <= 1e-6 and distance > 0.05)
        value = (distance, progress, outside)
        if best is None or value[:2] < best[:2]:
            best = value
        walked += length
    if best is None:
        raise RoutingError("Valhalla route geometry has no usable segments")
    return best[1], best[0], best[2]


@pytest.mark.parametrize("shape,points", [
    ([(40, -100), (40, -99), (41, -99), (39, -99)],
     [(40, -101), (39.5, -99), (38, -99), (40, -100)]),
    ([(0, 0), (0, 1), (0, 0), (0, 1), (1, 1)],
     [(0, 0.5), (0, 2), (0, -1), (1, 1), (2, 1)]),
    ([(0, 0), (0, 0), (0, 1), (0, 1), (0, 1 + 1e-14)],
     [(0, -1), (0, 2), (0, 0), (0.01, 0.5)]),
    ([(89, -170), (89, 170), (89.9, 170), (90, 170)],
     [(90, 0), (89.95, 179), (88, -179), (90, 180)]),
    ([(-89, 170), (-89, -170), (-89.9, -170), (-90, -170)],
     [(-90, 0), (-89.95, -179), (-88, 179), (-90, -180)]),
    ([(10, 179.9), (10, -179.9), (11, -179.9), (11, 179.9)],
     [(10, 180), (10, -180), (10.2, 181), (10.2, -539), (11.1, 0)]),
    ([(5, 0), (6, 1), (5, 1), (6, 0), (5, 0)],
     [(5.5, 0.5), (5, 0), (5.5, 0), (7, 1)]),
])
def test_exact_projection_preserves_poles_dateline_crossings_ties_and_outside(shape, points):
    # Repeat the geometry too: the larger case exercises tree branches rather
    # than checking only a leaf's unchanged projection formula.
    for full_shape in (shape, shape * 5):
        leg = route.RouteLeg(full_shape, 100)
        for point in points:
            assert leg.project(point) == linear_projection(full_shape, point)


def test_exact_projection_matches_deterministic_random_full_scans():
    rng = random.Random(62031)
    for _ in range(80):
        latitude = rng.uniform(-87, 87)
        longitude = rng.uniform(-170, 170)
        shape = [(max(-89.9, min(89.9, latitude + rng.uniform(-2, 2))),
                  max(-180, min(180, longitude + rng.uniform(-8, 8)))) for _ in range(40)]
        shape.insert(10, shape[9])
        leg = route.RouteLeg(shape, 100)
        points = [(rng.uniform(-90, 90), rng.uniform(-180, 180)) for _ in range(12)]
        points += shape[::4]
        for point in points:
            assert leg.project(point) == linear_projection(shape, point)


def test_no_usable_segment_retains_typed_route_failure():
    leg = route.RouteLeg([(40, -100), (40, -100), (40, -100 + 1e-14)], 0)
    with pytest.raises(RoutingError, match="no usable segments"):
        leg.project((40, -100))


def test_dense_hairpin_keeps_later_nearest_segment_and_earlier_exact_tie():
    shape = ([(0, i / 100) for i in range(101)]
             + [(i / 100, 1) for i in range(1, 101)]
             + [(1, 1 - i / 100) for i in range(1, 91)]
             + [(1 - i / 100, 0.1) for i in range(1, 101)])
    leg = route.RouteLeg(shape, 300)
    later = leg.project((0.3, 0.1))
    earlier_tie = leg.project((0, 0.1))
    assert later == linear_projection(shape, (0.3, 0.1))
    assert earlier_tie == linear_projection(shape, (0, 0.1))
    assert later[0] > 200
    assert earlier_tie[0] < 10
    assert not later[2] and not earlier_tie[2]


def test_dense_crossing_route_preserves_earlier_distance_progress_tie():
    shape = ([(i / 100, i / 100) for i in range(101)]
             + [(1, 1 - i / 100) for i in range(1, 101)]
             + [(1 - i / 100, i / 100) for i in range(1, 101)])
    leg = route.RouteLeg(shape, 300)
    point = (0.5, 0.5)
    assert leg.project(point) == linear_projection(shape, point)
    assert leg.project(point)[0] < 60


def test_every_tree_box_bound_is_below_each_exact_projected_distance():
    rng = random.Random(62782)
    shapes = [
        [(rng.uniform(-89, 89), rng.uniform(-180, 180)) for _ in range(45)],
        [(89 + i / 1000, -179 if i % 2 else 179) for i in range(45)],
        [(-89 - i / 1000, -179 if i % 2 else 179) for i in range(45)],
    ]
    points = [(rng.uniform(-90, 90), rng.uniform(-180, 180)) for _ in range(30)]
    points += [(90, 0), (-90, 180), (0, 180), (0, -180), (89, 541)]
    def descendants(node):
        return list(node.segments) + [s for child in node.children for s in descendants(child)]
    def nodes(node):
        return [node] + [n for child in node.children for n in nodes(child)]
    for shape in shapes:
        leg = route.RouteLeg(shape, 100)
        for node in nodes(leg._projection_index):
            for point in points:
                lower = node.lower_bound(point, math.cos(math.radians(point[0])))
                assert all(lower <= segment.project(point, len(shape) - 2)[0]
                           for segment in descendants(node))


def test_large_longitude_argument_keeps_original_haversine_semantics():
    shape = [(35 + i / 100, -120 + i / 50) for i in range(40)]
    leg = route.RouteLeg(shape, 100)
    for longitude in (-1e20, -1e12, 1e12, 1e20):
        point = (36, longitude)
        assert leg.project(point) == linear_projection(shape, point)


def test_projection_owns_an_immutable_geometry_snapshot():
    shape = [[40, -100], [40, -99], [41, -99]]
    original = [tuple(point) for point in shape]
    leg = route.RouteLeg(shape, 100)
    expected = linear_projection(original, (40.5, -99))
    assert leg.project((40.5, -99)) == expected
    shape[1][1] = 0
    shape.append([80, 80])
    assert leg.project((40.5, -99)) == expected
    assert leg.shape == tuple(original)


def test_all_approach_vertices_are_checked_with_bounded_segment_work(monkeypatch):
    shape = [(40 + 0.04 * math.sin(i / 100), -120 + i * 0.001) for i in range(2049)]
    points = shape[7::16]
    calls = 0
    def counted(*args):
        nonlocal calls
        calls += 1
        return haversine_miles(*args)
    monkeypatch.setattr(route, "haversine_miles", counted)
    leg = route.RouteLeg(shape, 150)
    results = [leg.project(point) for point in points]
    assert len(results) == len(points)
    assert all(result == linear_projection(shape, point) for result, point in zip(results, points))
    # A full scan requires 524,288 calls for these 128 approach vertices.
    # Building lengths once plus exact local search must stay far below that.
    assert calls < 2 * len(shape) + 64 * len(points)
