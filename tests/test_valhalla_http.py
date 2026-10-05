import asyncio
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

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
    assert "directions_type" not in captured["body"]
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


def test_remote_disconnect_retries_same_truck_route_then_recovers(monkeypatch):
    monkeypatch.setattr(settings, "VALHALLA_URL", "https://routing.example.com")
    monkeypatch.setattr(settings, "VALHALLA_API_SECRET", "secret")
    pauses = AsyncMock()
    monkeypatch.setattr("dieselup.clients.valhalla.asyncio.sleep", pauses)
    requests = []
    route = {"trip": {"legs": [{"summary": {"length": 42}}]}}

    def handler(request):
        requests.append(request)
        if len(requests) == 1:
            raise httpx.RemoteProtocolError("Server disconnected without sending a response")
        return httpx.Response(200, json=route)

    async def run():
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler),
                                    base_url=settings.VALHALLA_URL) as http:
            return await ValhallaClient(http_client=http).route([
                {"lat": 40, "lon": -100}, {"lat": 41, "lon": -99}])

    assert asyncio.run(run()) == route
    assert len(requests) == 2
    assert all(request.url.path == "/route" for request in requests)
    assert requests[0].content == requests[1].content
    assert all(request.headers["X-Valhalla-Key"] == "secret" for request in requests)
    assert json.loads(requests[1].content)["costing_options"]["truck"] == TRUCK_OPTS
    pauses.assert_awaited_once_with(0.25)


def test_exhausted_remote_disconnect_is_bounded_and_typed_without_fallback(monkeypatch):
    monkeypatch.setattr(settings, "VALHALLA_URL", "https://routing.example.com")
    monkeypatch.setattr(settings, "VALHALLA_API_SECRET", "secret")
    pauses = AsyncMock()
    monkeypatch.setattr("dieselup.clients.valhalla.asyncio.sleep", pauses)
    requests = []

    def handler(request):
        requests.append(request)
        raise httpx.RemoteProtocolError("PRIVATE UPSTREAM RESPONSE DETAIL")

    async def run():
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler),
                                    base_url=settings.VALHALLA_URL) as http:
            client = ValhallaClient(http_client=http)
            return await client.distance_matrix_miles([(40, -100), (41, -99)])

    with pytest.raises(RoutingError, match="Valhalla transport failure: RemoteProtocolError") as failure:
        asyncio.run(run())
    assert "PRIVATE UPSTREAM" not in str(failure.value)
    assert isinstance(failure.value.__cause__, httpx.RemoteProtocolError)
    assert len(requests) == 3
    assert all(request.url.path == "/sources_to_targets" for request in requests)
    assert [call.args for call in pauses.await_args_list] == [(0.25,), (0.5,)]


def _synthetic_polyline(shape):
    """Encode all fixture vertices independently of the application decoder."""
    result = []
    previous = [0, 0]
    for point in shape:
        for axis, value in enumerate(point):
            integer = round(value * 1_000_000)
            delta = integer - previous[axis]
            previous[axis] = integer
            encoded = ~(delta << 1) if delta < 0 else delta << 1
            while encoded >= 0x20:
                result.append(chr((0x20 | (encoded & 0x1f)) + 63))
                encoded >>= 5
            result.append(chr(encoded + 63))
    return "".join(result)


def test_compact_route_recovers_from_narration_body_truncation_with_full_geometry(monkeypatch):
    from dieselup.core.remaining_route import remaining_geometry

    monkeypatch.setattr(settings, "VALHALLA_URL", "https://routing.example.com")
    monkeypatch.setattr(settings, "VALHALLA_API_SECRET", "secret")
    monkeypatch.setattr("dieselup.clients.valhalla.asyncio.sleep", AsyncMock())
    first = [(round(40 + i / 1_000_000, 6), round(-100 + i / 1_000_000, 6))
             for i in range(4097)]
    second = [(round(first[-1][0] + i / 1_000_000, 6),
               round(first[-1][1] - i / 1_000_000, 6)) for i in range(4097)]
    route = {"trip": {"units": "miles", "summary": {"length": 72.75}, "legs": [
        {"shape": _synthetic_polyline(first), "summary": {"length": 40.25}},
        {"shape": _synthetic_polyline(second), "summary": {"length": 32.5}},
    ]}}
    requests = []

    class TruncatedNarration(httpx.AsyncByteStream):
        async def __aiter__(self):
            yield b'{"trip":'
            raise httpx.RemoteProtocolError(
                "peer closed connection without sending complete message body")

    def handler(request):
        body = json.loads(request.content)
        requests.append(body)
        if body.get("directions_type") != "none":
            # Reproduce HTTP 200 headers followed by an incomplete body.
            return httpx.Response(200, headers={"content-length": "189473"},
                                  stream=TruncatedNarration())
        assert body["units"] == "miles"
        return httpx.Response(200, json=route)

    async def run():
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler),
                                    base_url=settings.VALHALLA_URL) as http:
            client = ValhallaClient(http_client=http)
            stats = SimpleNamespace(lat=first[0][0], lng=first[0][1],
                                    heading=225, speed_mph=30)
            targets = [
                {"latitude": first[-1][0], "longitude": first[-1][1], "type": "pickup"},
                {"latitude": second[-1][0], "longitude": second[-1][1], "type": "delivery"},
            ]
            return await remaining_geometry(client, stats, targets)

    legs = asyncio.run(run())
    assert len(requests) == 1
    assert requests[0]["locations"] == [
        {"lat": first[0][0], "lon": first[0][1], "type": "break",
         "heading": 225, "heading_tolerance": 60},
        {"lat": first[-1][0], "lon": first[-1][1], "type": "break"},
        {"lat": second[-1][0], "lon": second[-1][1], "type": "break"},
    ]
    assert requests[0]["costing"] == "truck"
    assert requests[0]["costing_options"]["truck"] == TRUCK_OPTS
    assert "directions_options" not in requests[0]
    assert "generalize" not in requests[0]
    assert "shape_format" not in requests[0]
    assert len(legs) == 2
    assert legs[0].shape == tuple(first)
    assert legs[1].shape == tuple(second)
    assert [leg.road_miles for leg in legs] == [40.25, 32.5]


def test_compact_route_preserves_alternates_and_unmodified_response(monkeypatch):
    monkeypatch.setattr(settings, "VALHALLA_URL", "https://routing.example.com")
    monkeypatch.setattr(settings, "VALHALLA_API_SECRET", "secret")
    captured = {}
    locations = [{"lat": 40, "lon": -100, "type": "break", "heading": 225},
                 {"lat": 41, "lon": -99, "type": "break"}]
    response = {"trip": {"units": "miles", "legs": [
        {"shape": "fixture-shape-retained", "summary": {"length": 123.456}}]},
        "alternates": [{"trip": {"units": "miles", "legs": [
            {"shape": "fixture-alternate-retained", "summary": {"length": 124}}]}}]}

    def handler(request):
        captured.update(json.loads(request.content))
        return httpx.Response(200, json=response)

    async def run():
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler),
                                    base_url=settings.VALHALLA_URL) as http:
            return await ValhallaClient(http_client=http).route(locations, alternates=2)

    assert asyncio.run(run()) == response
    assert captured["units"] == "miles"
    assert captured["directions_type"] == "none"
    assert captured["locations"] == locations
    assert captured["alternates"] == 2
    assert captured["costing_options"]["truck"] == TRUCK_OPTS
    assert "directions_options" not in captured
