"""Conservative route-progress evidence; ambiguous loops never prove a miss."""
import math

from dieselup.core.optimizer import haversine_miles


def project_progress(shape, point):
    if not isinstance(shape, list) or len(shape) < 2:
        return None
    samples = []
    walked = 0.0
    for a, b in zip(shape, shape[1:]):
        length = haversine_miles(*a, *b)
        if length <= 1e-9:
            continue
        scale = math.cos(math.radians((a[0] + b[0]) / 2))
        dx, dy = (b[1] - a[1]) * scale, b[0] - a[0]
        px, py = (point[1] - a[1]) * scale, point[0] - a[0]
        t = min(1.0, max(0.0, (px * dx + py * dy) / (dx * dx + dy * dy)))
        q = (a[0] + t * (b[0] - a[0]), a[1] + t * (b[1] - a[1]))
        samples.append((haversine_miles(*point, *q), walked + t * length))
        walked += length
    if not samples:
        return None
    distance, progress = min(samples)
    if distance > 1.0:
        return None
    plausible = [p for d, p in samples if d <= distance + 0.05]
    if max(plausible) - min(plausible) > 2.0:
        return None
    return progress


def passed_stop_evidence(proof, location):
    monitor = proof.get("monitor") or {}
    shape = monitor.get("shape")
    target = monitor.get("first_fuel_progress_miles")
    if target is None:
        return None
    progress = project_progress(shape, (location.lat, location.lng))
    if progress is None or progress < float(target) + 2.0:
        return None
    return {"method": "ordered_road_route", "progress_miles": round(progress, 2),
            "advised_progress_miles": round(float(target), 2),
            "miles_beyond_stop": round(progress - float(target), 2),
            "latitude": location.lat, "longitude": location.lng}
