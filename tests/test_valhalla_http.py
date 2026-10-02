import asyncio
import json

import httpx
import pytest

from dieselup.clients.routing import RoutingError
from dieselup.clients.valhalla import TRUCK_OPTS, ValhallaClient
from dieselup.config import settings


def test_authenticated_http_matrix_uses_class8_profile(monkeypatch):
    monkeypatch.setattr(settings, "VALHALLA_URL", "https://routing.example.com")
    monkeypatch.setattr(settings, "VALHALLA_API_SECRET", "secret")
    captured = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["secret"] = request.headers["X-Valhalla-Key"]
        captured["body"] = json.loads(request.content)
        return httpx.Response(200, json={"sources_to_targets": [
            [{"distance": 0}, {"distance": 10}],
            [{"distance": 10}, {"distance": 0}],
        ]})

    http = httpx.AsyncClient(
        transport=httpx.MockTransport(handler), base_url=settings.VALHALLA_URL
    )
    client = ValhallaClient(http_client=http)
    result = asyncio.run(client.distance_matrix_miles([(40, -75), (41, -76)]))
    asyncio.run(http.aclose())
    assert captured["secret"] == "secret"
    assert captured["body"]["costing"] == "truck"
    assert captured["body"]["costing_options"]["truck"] == TRUCK_OPTS
    assert result == [[0.0, 10.0], [10.0, 0.0]]


def test_incomplete_http_matrix_fails_closed(monkeypatch):
    monkeypatch.setattr(settings, "VALHALLA_URL", "https://routing.example.com")
    monkeypatch.setattr(settings, "VALHALLA_API_SECRET", "secret")

    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"sources_to_targets": [
            [{"distance": 0}, {"distance": 10}],
            [{"distance": 10}],
        ]})

    http = httpx.AsyncClient(
        transport=httpx.MockTransport(handler), base_url=settings.VALHALLA_URL
    )
    client = ValhallaClient(http_client=http)
    try:
        with pytest.raises(RoutingError, match="incomplete"):
            asyncio.run(client.distance_matrix_miles([(40, -75), (41, -76)]))
    finally:
        asyncio.run(http.aclose())


def test_matrix_duration_is_seconds_and_distance_keeps_precision(monkeypatch):
    monkeypatch.setattr(settings, "VALHALLA_URL", "https://routing.example.com")
    monkeypatch.setattr(settings, "VALHALLA_API_SECRET", "secret")
    http = httpx.AsyncClient(transport=httpx.MockTransport(
        lambda _: httpx.Response(200,json={"sources_to_targets":[[{"distance":32.064,"time":1900}]]})
    ),base_url=settings.VALHALLA_URL)
    try:
        cells=asyncio.run(ValhallaClient(http_client=http).matrix((32,-96),[(32,-97)]))
        assert cells[0].duration_seconds == 1900
        assert cells[0].distance_miles == 32.064
    finally:
        asyncio.run(http.aclose())


@pytest.mark.parametrize('bad',[-1,"NaN","Infinity","invalid"])
def test_invalid_road_distances_fail_closed(monkeypatch,bad):
    monkeypatch.setattr(settings, "VALHALLA_URL", "https://routing.example.com")
    monkeypatch.setattr(settings, "VALHALLA_API_SECRET", "secret")
    http = httpx.AsyncClient(transport=httpx.MockTransport(
        lambda _: httpx.Response(200,json={"sources_to_targets":[[{"distance":0},{"distance":bad}],[{"distance":1},{"distance":0}]]})
    ),base_url=settings.VALHALLA_URL)
    try:
        with pytest.raises(RoutingError,match="invalid distance"):
            asyncio.run(ValhallaClient(http_client=http).distance_matrix_miles([(32,-96),(32,-97)]))
    finally:
        asyncio.run(http.aclose())
