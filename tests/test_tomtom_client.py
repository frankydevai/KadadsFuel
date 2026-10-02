"""TomTom truck matrix client: request shape, parsing, and secret safety."""

from __future__ import annotations

import asyncio
import json
from urllib.parse import parse_qs

import httpx

from dieselup.clients.tomtom import TomTomClient
from dieselup.config import settings


def test_tomtom_matrix_uses_truck_options_and_parses_flat_response(monkeypatch):
    monkeypatch.setattr(settings, "TOMTOM_API_KEY", "test-key")
    seen = []

    def handler(req: httpx.Request) -> httpx.Response:
        body = json.loads(req.content.decode())
        seen.append({
            "path": req.url.path,
            "query": parse_qs(req.url.query.decode()),
            "body": body,
        })

        origin_count = len(body["origins"])
        destination_count = len(body["destinations"])
        data = []
        for i in range(origin_count):
            for j in range(destination_count):
                miles = 0
                if origin_count == 1 and destination_count > 1:
                    miles = [500, 200, 250][j]
                else:
                    miles = [300, 350][i]
                data.append({
                    "originIndex": i,
                    "destinationIndex": j,
                    "statusCode": 200,
                    "routeSummary": {"lengthInMeters": miles * 1609.344},
                })
        return httpx.Response(200, json={"data": data})

    client = httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
        base_url="https://api.tomtom.com",
    )
    router = TomTomClient(http_client=client)
    matrix = asyncio.run(router.distance_matrix_miles([
        (40.0, -74.5),
        (41.8, -87.6),
        (40.9, -81.0),
    ]))
    asyncio.run(router.close())
    asyncio.run(client.aclose())

    assert [call["path"] for call in seen] == ["/routing/matrix/2", "/routing/matrix/2"]
    assert all(call["query"]["key"] == ["test-key"] for call in seen)
    assert seen[0]["body"]["options"]["travelMode"] == "truck"
    assert seen[0]["body"]["options"]["departAt"] == "any"
    assert seen[0]["body"]["options"]["traffic"] == "historical"
    assert seen[0]["body"]["options"]["vehicleCommercial"] is True
    assert "vehicleNumberOfAxles" not in seen[0]["body"]["options"]
    assert seen[0]["body"]["origins"][0]["point"] == {"latitude": 40.0, "longitude": -74.5}
    assert len(seen[0]["body"]["origins"]) == 1
    assert len(seen[0]["body"]["destinations"]) == 2
    assert len(seen[1]["body"]["origins"]) == 1
    assert len(seen[1]["body"]["destinations"]) == 1
    assert matrix[0][0] == 0.0
    assert matrix[0][1] == 500.0
    assert matrix[0][2] == 200.0
    assert matrix[2][1] == 300.0
    assert matrix[1][2] is None
    assert "test-key" not in repr(matrix)


def test_tomtom_matrix_chunks_standard_batches(monkeypatch):
    monkeypatch.setattr(settings, "TOMTOM_API_KEY", "test-key")
    seen = []

    def handler(req: httpx.Request) -> httpx.Response:
        body = json.loads(req.content.decode())
        seen.append(body)
        assert len(body["origins"]) * len(body["destinations"]) <= 100
        data = []
        for i in range(len(body["origins"])):
            for j in range(len(body["destinations"])):
                data.append({
                    "originIndex": i,
                    "destinationIndex": j,
                    "statusCode": 200,
                    "routeSummary": {"lengthInMeters": (j + 1) * 1609.344},
                })
        return httpx.Response(200, json={"data": data})

    client = httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
        base_url="https://api.tomtom.com",
    )
    router = TomTomClient(http_client=client)
    points = [
        (40.0, -74.5),
        (41.8, -87.6),
        *[(39.0 + i / 1000, -86.0) for i in range(205)],
    ]

    matrix = asyncio.run(router.distance_matrix_miles(points))
    asyncio.run(router.close())
    asyncio.run(client.aclose())

    assert [len(call["destinations"]) for call in seen[:3]] == [100, 100, 6]
    assert [len(call["origins"]) for call in seen[3:]] == [100, 100, 5]
    assert matrix[0][1] == 1.0
    assert matrix[0][206] == 6.0
    assert matrix[206][1] == 1.0
