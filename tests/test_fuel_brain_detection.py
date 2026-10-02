"""Detection-threshold tests for fuel_brain._process_vehicle.

Spec: a fueling event (which drives the "fueled elsewhere" warning + loss) is
only opened on a real fill of >= 30 gal. Smaller rises are sensor noise / trivial
top-ups and must be ignored. An already-open event keeps extending while fuel
keeps rising, and is finalized once it stops.
"""
import asyncio
from types import SimpleNamespace
from datetime import datetime,timedelta,timezone
from unittest.mock import AsyncMock

from dieselup.core import fuel_brain
from dieselup.config import settings

TANK = settings.TANK_CAPACITY_GALLONS  # 220 in the test env


def _pct(gallons: float) -> float:
    return gallons / TANK * 100.0


def _run(monkeypatch, *, prev_gal, now_gal, open_event):
    """Drive _process_vehicle once, returning whether _handle_fuel_rise fired
    and whether an open event was finalized."""
    calls = {"rise": 0, "finalize": 0}

    async def fake_snapshot(**_kw):
        return False

    async def fake_handle_rise(**_kw):
        calls["rise"] += 1
        return True

    async def fake_execute(query, *args):
        if "finalized_at = NOW()" in query:
            calls["finalize"] += 1
        return "UPDATE 1"

    monkeypatch.setattr(fuel_brain, "_maybe_write_snapshot", fake_snapshot)
    monkeypatch.setattr(fuel_brain, "_handle_fuel_rise", fake_handle_rise)
    monkeypatch.setattr(fuel_brain, "execute", fake_execute)

    from dieselup.core import stationary_context
    monkeypatch.setattr(stationary_context,'record_stationary_context',AsyncMock())
    now=datetime.now(timezone.utc)
    location = SimpleNamespace(lat=40.0, lng=-80.0,gps_age_minutes=1,gps_time=now)

    class FakeSamsara:
        async def get_vehicle_location(self, _vid):
            return location

        async def get_vehicle_fuel_reading(self, _vid):
            return now_gal,now

    vehicle = SimpleNamespace(id="vid-1", name="Truck 100")
    prev = {"fuel_pct": _pct(prev_gal),"fuel_observed_at":now-timedelta(minutes=5)}

    asyncio.run(
        fuel_brain._process_vehicle(
            vehicle=vehicle,
            samsara=FakeSamsara(),
            bot=object(),
            prev=prev,
            open_event=open_event,
            truck_row={"truck_unit": "100", "driver_telegram_id": None},
            pending_by_unit={},
        )
    )
    return calls


def test_no_event_below_30_gallons(monkeypatch):
    calls = _run(monkeypatch, prev_gal=100.0, now_gal=120.0, open_event=None)  # +20 gal
    assert calls["rise"] == 0


def test_event_opens_at_or_above_30_gallons(monkeypatch):
    calls = _run(monkeypatch, prev_gal=100.0, now_gal=140.0, open_event=None)  # +40 gal
    assert calls["rise"] == 1


def test_open_event_extends_while_still_rising(monkeypatch):
    calls = _run(
        monkeypatch, prev_gal=140.0, now_gal=150.0,  # +10 gal, > continue floor
        open_event={"id": 7, "fuel_pct_start": _pct(100.0)},
    )
    assert calls["rise"] == 1
    assert calls["finalize"] == 0


def test_open_event_finalizes_when_rise_stops(monkeypatch):
    calls = _run(
        monkeypatch, prev_gal=150.0, now_gal=151.0,  # +1 gal, below continue floor
        open_event={"id": 7, "fuel_pct_start": _pct(100.0)},
    )
    assert calls["rise"] == 0
    assert calls["finalize"] == 1


def test_snapshot_is_written_every_successful_gps_poll(monkeypatch):
    executed: list[tuple[str, tuple]] = []

    async def fake_execute(query, *args):
        executed.append((query, args))
        return "INSERT 0 1"

    monkeypatch.setattr(fuel_brain, "execute", fake_execute)

    wrote = asyncio.run(
        fuel_brain._maybe_write_snapshot(
            vehicle_id="vid-1",
            truck_unit="100",
            location=SimpleNamespace(lat=40.0, lng=-80.0, speed_mph=0.0),
            fuel_pct=50.0,
            prev={"latitude": 40.0, "longitude": -80.0, "fuel_pct": 50.0, "age_minutes": 1.0},
        )
    )

    assert wrote is True
    assert len(executed) == 1
    assert "INSERT INTO truck_snapshots" in executed[0][0]


def test_late_fuel_jump_at_advised_stop_uses_previous_snapshot(monkeypatch):
    async def fail_nearby(*_args, **_kwargs):
        return None  # No competing station in the advised geofence.

    monkeypatch.setattr(fuel_brain, "_nearby_priced_stop", fail_nearby)

    location = SimpleNamespace(lat=40.0, lng=-80.0)
    prev = {"latitude": 41.0, "longitude": -81.0,"speed_mph":0,"age_minutes":5,"gps_observed_at":datetime.now(timezone.utc)-timedelta(minutes=5)}
    pending = {
        "recommended_site_id": 77,
        "candidates": [
            {
                "site_id": 77,
                "station_name": "Pilot Travel Center",
                "city": "Pontoon Beach",
                "state": "IL",
                "latitude": 41.0001,
                "longitude": -81.0001,
                "your_price": 3.25,
            }
        ],
    }

    match = asyncio.run(
        fuel_brain._classify_fuel_location(
            location=location,
            prev=prev,
            pending=pending,
        )
    )

    assert match.classification == "recommended"
    assert match.nearby["site_id"] == 77
    assert match.source == "previous"
    assert match.lat == 41.0
    assert match.lng == -81.0


def test_late_fuel_jump_at_other_priced_stop_uses_previous_snapshot(monkeypatch):
    async def fake_nearby(lat, lng, *, exclude_site_id):
        if lat == 41.0 and lng == -81.0:
            return {
                "site_id": 88,
                "your_price": 3.75,
                "state": "IL",
                "station_name": "Flying J",
                "address": "1 Diesel Way",
                "city": "Troy",
                "latitude": lat,
                "longitude": lng,
            }
        return None

    monkeypatch.setattr(fuel_brain, "_nearby_priced_stop", fake_nearby)

    match = asyncio.run(
        fuel_brain._classify_fuel_location(
            location=SimpleNamespace(lat=40.0, lng=-80.0),
            prev={"latitude": 41.0, "longitude": -81.0,"speed_mph":0,"age_minutes":5,"gps_observed_at":datetime.now(timezone.utc)-timedelta(minutes=5)},
            pending={
                "recommended_site_id": 77,
                "candidates": [
                    {
                        "site_id": 77,
                        "station_name": "Pilot Travel Center",
                        "latitude": 42.0,
                        "longitude": -82.0,
                    }
                ],
            },
        )
    )

    assert match.classification == "contracted_other"
    assert match.nearby["site_id"] == 88
    assert match.source == "previous"
