"""Idle work is healthy; a failed check must not refresh its heartbeat."""
import asyncio
from unittest.mock import AsyncMock

import pytest

from dieselup import metrics
from dieselup.core import compliance, dlq_retry, fuel_replan


def test_empty_compliance_sweep_records_healthy_completion(monkeypatch):
    observed=[]
    replan=AsyncMock()
    monkeypatch.setattr(compliance,'fetch_all',AsyncMock(return_value=[]))
    monkeypatch.setattr(fuel_replan,'run_requested_replans',replan)
    monkeypatch.setattr(metrics,'gauge',lambda key,value:observed.append(key))
    asyncio.run(compliance.resolve_pending_events(object()))
    replan.assert_awaited_once()
    assert 'compliance_last_heartbeat_mono' in observed


def test_failed_idle_replan_does_not_report_healthy_completion(monkeypatch):
    observed=[]
    monkeypatch.setattr(compliance,'fetch_all',AsyncMock(return_value=[]))
    monkeypatch.setattr(fuel_replan,'run_requested_replans',AsyncMock(side_effect=RuntimeError('check failed')))
    monkeypatch.setattr(metrics,'gauge',lambda key,value:observed.append(key))
    with pytest.raises(RuntimeError,match='check failed'):
        asyncio.run(compliance.resolve_pending_events(object()))
    assert 'compliance_last_heartbeat_mono' not in observed


def test_empty_live_delivery_queue_records_healthy_completion(monkeypatch):
    observed=[]
    monkeypatch.setattr(dlq_retry.settings,'TELEGRAM_MESSAGING_MODE','live')
    monkeypatch.setattr(dlq_retry,'fetch_all',AsyncMock(return_value=[]))
    monkeypatch.setattr(metrics,'gauge',lambda key,value:observed.append(key))
    asyncio.run(dlq_retry.retry_failed_alerts(object()))
    assert 'dlq_retry_last_heartbeat_mono' in observed
