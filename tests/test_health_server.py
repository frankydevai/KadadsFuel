"""Carrier bot health JSON includes safe routing configuration status."""

from dieselup.config import settings
from dieselup.health_server import _routing_status
from dieselup import health_server


def test_health_routing_status_reports_private_valhalla_without_secret(monkeypatch):
    monkeypatch.setattr(settings, "VALHALLA_URL", "https://routing.example.com")
    monkeypatch.setattr(settings, "VALHALLA_API_SECRET", "secret-routing-key")
    monkeypatch.setattr(health_server.metrics, "snapshot", lambda: {"gauges": {}})

    status = _routing_status()

    assert status == {
        "primary": "valhalla",
        "valhalla_configured": True,
        "valhalla_status": "unverified",
        "last_success_age_seconds": None,
        "fallbacks_enabled": False,
    }
    assert "secret-routing-key" not in str(status)


def _clock_and_metrics(monkeypatch, gauges):
    monkeypatch.setattr(health_server.time, "monotonic", lambda: 10000.0)
    monkeypatch.setattr(health_server, "_STARTED_MONO", 0.0)
    monkeypatch.setattr(health_server.metrics, "snapshot", lambda: {"gauges": gauges})


def test_fresh_compliance_does_not_hide_stalled_load_sync(monkeypatch):
    _clock_and_metrics(monkeypatch, {
        "load_sync_last_heartbeat_mono": 100.0,
        "compliance_last_heartbeat_mono": 9990.0,
        "dlq_retry_last_heartbeat_mono": 9990.0,
        "fuel_brain_last_heartbeat_mono": 9990.0,
    })
    code, message = health_server._ok_status()
    assert code == 503
    assert "load_sync" in message


def test_expected_job_intervals_are_healthy(monkeypatch):
    _clock_and_metrics(monkeypatch, {
        "load_sync_last_heartbeat_mono": 9100.0,
        "compliance_last_heartbeat_mono": 9700.0,
        "dlq_retry_last_heartbeat_mono": 9400.0,
        "fuel_brain_last_heartbeat_mono": 9700.0,
    })
    assert health_server._ok_status()[0] == 200


def test_missing_heartbeats_cannot_stay_booting_forever(monkeypatch):
    _clock_and_metrics(monkeypatch, {})
    assert health_server._ok_status()[0] == 503


def test_admin_pause_is_distinct_from_stalled_jobs(monkeypatch):
    _clock_and_metrics(monkeypatch, {"bot_paused": 1})
    code, message = health_server._ok_status()
    assert code == 503 and message.startswith("PAUSED")


def test_stale_prices_are_visible_as_a_blocker(monkeypatch):
    _clock_and_metrics(monkeypatch, {"fuel_prices_stale": 1})
    code, message = health_server._ok_status()
    assert code == 503 and message.startswith("BLOCKED")


def test_routing_success_failure_and_staleness(monkeypatch):
    monkeypatch.setattr(settings, "VALHALLA_URL", "https://routing.example.com")
    monkeypatch.setattr(settings, "VALHALLA_API_SECRET", "secret")
    gauges = {"valhalla_last_success_mono": 9990.0}
    _clock_and_metrics(monkeypatch, gauges)
    assert _routing_status()["valhalla_status"] == "green"
    gauges["valhalla_last_failure_mono"] = 9995.0
    assert _routing_status()["valhalla_status"] == "red"
    gauges.clear()
    gauges["valhalla_last_success_mono"] = 100.0
    assert _routing_status()["valhalla_status"] == "stale"
