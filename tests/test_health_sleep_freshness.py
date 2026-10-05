"""A suspended Mac must not make old job evidence appear fresh on wake."""
from dieselup import health_server, metrics
from dieselup.config import settings


def clocks(monkeypatch):
    clock = {'mono': 100.0, 'unix': 1700000000.0}
    monkeypatch.setattr(health_server.time, 'monotonic', lambda: clock['mono'])
    monkeypatch.setattr(health_server.time, 'time', lambda: clock['unix'])
    monkeypatch.setattr(health_server, '_STARTED_MONO', clock['mono'])
    monkeypatch.setattr(health_server, '_STARTED_UNIX', clock['unix'], raising=False)
    monkeypatch.setattr(metrics, '_gauges', {})
    monkeypatch.setattr(settings, 'BOT_MODE', 'active')
    return clock


def completed_jobs(clock):
    for job in health_server._JOB_MAX_AGE:
        metrics.gauge(job + '_last_heartbeat_mono', clock['mono'])


def test_jobs_and_last_sweep_include_sleep_when_monotonic_barely_advances(monkeypatch):
    clock = clocks(monkeypatch)
    completed_jobs(clock)
    clock['unix'] += 42 * 3600
    clock['mono'] += 10
    assert all(job['stale'] for job in health_server._job_statuses().values())
    assert health_server._seconds_since_last_sweep() == 42 * 3600
    assert health_server._ok_status()[0] == 503


def test_one_completed_job_after_wake_does_not_refresh_others(monkeypatch):
    clock = clocks(monkeypatch)
    completed_jobs(clock)
    clock['unix'] += 42 * 3600
    clock['mono'] += 10
    metrics.gauge('compliance_last_heartbeat_mono', clock['mono'])
    statuses = health_server._job_statuses()
    assert not statuses['compliance']['stale']
    assert statuses['load_sync']['stale']
    assert health_server._ok_status()[0] == 503


def test_startup_grace_expires_during_suspension_without_fake_completion(monkeypatch):
    clock = clocks(monkeypatch)
    assert health_server._ok_status()[0] == 200
    clock['unix'] += 42 * 3600
    clock['mono'] += 10
    assert health_server._ok_status()[0] == 503
    assert all(job['age_seconds'] is None for job in health_server._job_statuses().values())


def test_clock_rollback_cannot_hide_monotonic_overdue_jobs(monkeypatch):
    clock = clocks(monkeypatch)
    completed_jobs(clock)
    clock['unix'] -= 3600
    clock['mono'] += 1801
    assert health_server._job_statuses()['load_sync']['stale']
    assert health_server._seconds_since_last_sweep() == 1801


def test_routing_success_becomes_stale_after_sleep(monkeypatch):
    clock = clocks(monkeypatch)
    monkeypatch.setattr(settings, 'VALHALLA_URL', 'https://routing.example.com')
    monkeypatch.setattr(settings, 'VALHALLA_API_SECRET', 'test-only')
    metrics.gauge('valhalla_last_success_mono', clock['mono'])
    assert health_server._routing_status()['valhalla_status'] == 'green'
    clock['unix'] += 42 * 3600
    clock['mono'] += 10
    assert health_server._routing_status()['valhalla_status'] == 'stale'


def test_active_bot_with_explicitly_lost_leadership_is_unhealthy(monkeypatch):
    clock = clocks(monkeypatch)
    completed_jobs(clock)
    metrics.gauge('singleton_leader_active', 0)
    code, message = health_server._ok_status()
    assert code == 503
    assert 'leadership' in message.lower()
