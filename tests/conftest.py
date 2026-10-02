"""
pytest configuration — set required env vars BEFORE any dieselup module is imported.

All tests in this suite run without a real DB, Telegram, or external API.
Modules that call settings at import time (config.py) will see these fake values.
"""
import os
from unittest.mock import AsyncMock
import pytest

# Must be set before dieselup.config is imported.
os.environ.setdefault("DATATRUCK_API_TOKEN", "test_datatruck_token")
os.environ.setdefault("SAMSARA_API_TOKEN", "test_samsara_token")
os.environ.setdefault("TELEGRAM_BOT_TOKEN", "1234567890:AAtest_token_here")
os.environ.setdefault("TEST_TRUCK_UNITS", "")  # full-scope isolated fixtures only
os.environ.setdefault("AUTO_LINK_ENABLED", "true")  # isolated onboarding fixtures
os.environ.setdefault("TELEGRAM_MESSAGING_MODE", "live")  # isolated fake delivery tests only
os.environ.setdefault("TELEGRAM_ADMIN_CHAT_ID", "6264960800")
os.environ.setdefault("DATABASE_URL", "postgresql://test:test@localhost/testdb")
os.environ.setdefault("QM_CLIENT_ID", "test_qm_client")
os.environ.setdefault("QM_CLIENT_SECRET", "test_qm_secret")
os.environ.setdefault("IFTA_HOME_STATE", "NJ")
os.environ.setdefault("PILOT_ACCOUNT_NUMBER", "test_pilot_account")
os.environ.setdefault("DATATRUCK_COMPANY_SLUG", "test-carrier")
os.environ.setdefault("TANK_CAPACITY_GALLONS", "220")
os.environ.setdefault("SAFETY_FLOOR_GALLONS", "20")
os.environ.setdefault("MAX_ARRIVAL_FUEL_GALLONS", "80")
os.environ.setdefault("DELIVERY_RESERVE_PCT", "20")
os.environ.setdefault("FLEET_DEFAULT_MPG", "6.5")
os.environ.setdefault("RANK_STRATEGY", "your_price")
os.environ.setdefault("COST_PER_MILE", "0.55")
os.environ.setdefault("STOP_TIME_PENALTY", "12.0")
os.environ.setdefault("VALHALLA_URL", "https://routing.example.test")
os.environ.setdefault("VALHALLA_API_SECRET", "test_valhalla_secret")


@pytest.fixture(autouse=True)
def isolated_advice_audit(monkeypatch):
    from dieselup.core import advice_audit
    monkeypatch.setattr(advice_audit, "execute", AsyncMock())
    monkeypatch.setattr(advice_audit, "fetch_one", AsyncMock(return_value=None))
