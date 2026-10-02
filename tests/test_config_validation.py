import pytest
from pydantic import ValidationError

from dieselup.config import Settings


BASE = {
    "DATATRUCK_API_TOKEN": "datatruck-token",
    "SAMSARA_API_TOKEN": "samsara-token",
    "TELEGRAM_BOT_TOKEN": "1234567890:test-token",
    "TELEGRAM_ADMIN_CHAT_ID": 123,
    "DATABASE_URL": "postgresql://test:test@localhost/test",
}


def test_active_mode_requires_private_valhalla_url():
    with pytest.raises(ValidationError, match="VALHALLA_URL is required"):
        Settings(
            _env_file=None,
            **BASE,
            BOT_MODE="active",
            VALHALLA_URL="",
            VALHALLA_API_SECRET="secret",
        )


def test_active_mode_requires_private_valhalla_secret():
    with pytest.raises(ValidationError, match="VALHALLA_API_SECRET is required"):
        Settings(
            _env_file=None,
            **BASE,
            BOT_MODE="active",
            VALHALLA_URL="https://routing.example.com/",
            VALHALLA_API_SECRET="",
        )


def test_non_active_maintenance_mode_does_not_require_routing():
    configured = Settings(
        _env_file=None,
        **BASE,
        BOT_MODE="bootstrap",
        VALHALLA_URL="",
        VALHALLA_API_SECRET="",
    )
    assert configured.BOT_MODE == "bootstrap"


def test_valhalla_url_is_normalized():
    configured = Settings(
        _env_file=None,
        **BASE,
        BOT_MODE="active",
        VALHALLA_URL=" https://routing.example.com/ ",
        VALHALLA_API_SECRET=" secret ",
    )
    assert configured.VALHALLA_URL == "https://routing.example.com"
    assert configured.VALHALLA_API_SECRET == "secret"


def test_dashboard_cookie_defaults_to_secure(monkeypatch):
    monkeypatch.delenv("DASHBOARD_COOKIE_SECURE", raising=False)
    configured = Settings(_env_file=None, **BASE, BOT_MODE="bootstrap")
    assert configured.DASHBOARD_COOKIE_SECURE is True


def test_dashboard_cookie_local_http_override_from_dotenv(monkeypatch, tmp_path):
    monkeypatch.delenv("DASHBOARD_COOKIE_SECURE", raising=False)
    env_file = tmp_path / ".env"
    env_file.write_text("DASHBOARD_COOKIE_SECURE=false\n", encoding="utf-8")
    configured = Settings(_env_file=env_file, **BASE, BOT_MODE="bootstrap")
    assert configured.DASHBOARD_COOKIE_SECURE is False
