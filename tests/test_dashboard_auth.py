"""Dashboard authentication checks without a database or external services."""

from unittest.mock import AsyncMock

import pytest
from fastapi.testclient import TestClient

from dieselup.config import Settings
from dieselup.dashboard import server


@pytest.fixture
def auth_client(monkeypatch):
    monkeypatch.setattr(server.settings, "DASHBOARD_SECRET", "isolated-dashboard-secret")
    monkeypatch.setattr(server.settings, "DASHBOARD_ADMIN_EMAIL", "")
    monkeypatch.setattr(server.settings, "DASHBOARD_ADMIN_PASSWORD", "")
    monkeypatch.setattr(server.settings, "DASHBOARD_COOKIE_SECURE", True)
    fake_fetch = AsyncMock(return_value=[{"truck_unit": "test-truck"}])
    monkeypatch.setattr(server, "_fleet_data", fake_fetch)
    monkeypatch.setattr(
        server, "get_pool", AsyncMock(side_effect=AssertionError("Unexpected database access"))
    )
    with TestClient(server.create_app(), base_url="https://dashboard.example.test") as client:
        yield client, fake_fetch


@pytest.mark.parametrize("path", ["/api/fleet", "/api/fuel-prices/status", "/metrics", "/"])
def test_missing_secret_fails_closed_before_database_access(auth_client, monkeypatch, path):
    client, fake_fetch = auth_client
    monkeypatch.setattr(server.settings, "DASHBOARD_SECRET", "")
    response = client.get(path, follow_redirects=False)
    assert response.status_code == 503
    fake_fetch.assert_not_awaited()
    server.get_pool.assert_not_awaited()


@pytest.mark.parametrize(
    "request_kwargs",
    [{}, {"headers": {"Authorization": "Bearer incorrect"}}, {"params": {"token": "incorrect"}},
     {"headers": {"Cookie": "kadads_dashboard_token=incorrect"}},
     {"params": {"token": "invalid-unicode-\u00e9"}}],
)
def test_invalid_auth_cannot_query_database(auth_client, request_kwargs):
    client, fake_fetch = auth_client
    response = client.get("/api/fleet", **request_kwargs)
    assert response.status_code == 401
    assert response.headers["cache-control"] == "private, no-store"
    fake_fetch.assert_not_awaited()
    server.get_pool.assert_not_awaited()


@pytest.mark.parametrize(
    "request_kwargs",
    [{"headers": {"Authorization": "Bearer isolated-dashboard-secret"}},
     {"headers": {"Cookie": "kadads_dashboard_token=isolated-dashboard-secret"}},
     {"params": {"token": "isolated-dashboard-secret"}}],
)
def test_configured_secret_accepts_existing_auth_methods(auth_client, request_kwargs):
    client, fake_fetch = auth_client
    response = client.get("/api/fleet", **request_kwargs)
    assert response.status_code == 200
    assert response.json() == [{"truck_unit": "test-truck"}]
    fake_fetch.assert_awaited_once()


@pytest.mark.parametrize("missing", ["secret", "email", "password"])
def test_login_rejects_missing_configuration(auth_client, monkeypatch, missing):
    client, fake_fetch = auth_client
    monkeypatch.setattr(server.settings, "DASHBOARD_ADMIN_EMAIL", "admin@example.test")
    monkeypatch.setattr(server.settings, "DASHBOARD_ADMIN_PASSWORD", "isolated-password")
    if missing == "secret":
        monkeypatch.setattr(server.settings, "DASHBOARD_SECRET", "")
    else:
        monkeypatch.setattr(server.settings, f"DASHBOARD_ADMIN_{missing.upper()}", "")
    response = client.post(
        "/api/login", json={"email": "admin@example.test", "password": "isolated-password"}
    )
    assert response.status_code == 503
    assert "set-cookie" not in response.headers
    fake_fetch.assert_not_awaited()
    server.get_pool.assert_not_awaited()


def test_login_has_no_fallback_credentials(auth_client):
    client, _ = auth_client
    response = client.post(
        "/api/login", json={"email": "legacy@example.test", "password": "legacy-password"}
    )
    assert response.status_code == 503
    assert "set-cookie" not in response.headers


@pytest.mark.parametrize("wrong", ["email", "password"])
def test_incorrect_login_rejected_without_cookie(auth_client, monkeypatch, wrong):
    client, fake_fetch = auth_client
    monkeypatch.setattr(server.settings, "DASHBOARD_ADMIN_EMAIL", "admin@example.test")
    monkeypatch.setattr(server.settings, "DASHBOARD_ADMIN_PASSWORD", "isolated-password")
    credentials = {"email": "admin@example.test", "password": "isolated-password"}
    credentials[wrong] = "incorrect"
    response = client.post("/api/login", json=credentials)
    assert response.status_code == 401
    assert "set-cookie" not in response.headers
    fake_fetch.assert_not_awaited()


def test_valid_login_sets_secure_cookie_that_authenticates_next_request(auth_client, monkeypatch):
    client, fake_fetch = auth_client
    monkeypatch.setattr(server.settings, "DASHBOARD_ADMIN_EMAIL", "admin@example.test")
    monkeypatch.setattr(server.settings, "DASHBOARD_ADMIN_PASSWORD", "isolated-password-\u00e9")
    response = client.post(
        "/api/login", json={"email": " ADMIN@EXAMPLE.TEST ", "password": "isolated-password-\u00e9"}
    )
    assert response.status_code == 200
    assert response.json() == {"ok": True}
    cookie = response.headers["set-cookie"]
    assert "Secure" in cookie
    assert "HttpOnly" in cookie
    assert "SameSite=lax" in cookie
    assert "Max-Age=2592000" in cookie
    fake_fetch.assert_not_awaited()
    assert client.get("/api/fleet").status_code == 200
    fake_fetch.assert_awaited_once()


def test_login_uses_credentials_loaded_from_dotenv(auth_client, monkeypatch, tmp_path):
    client, fake_fetch = auth_client
    for key in ("DASHBOARD_SECRET", "DASHBOARD_ADMIN_EMAIL", "DASHBOARD_ADMIN_PASSWORD"):
        monkeypatch.delenv(key, raising=False)
    env_file = tmp_path / ".env"
    env_file.write_text(
        "DASHBOARD_SECRET=dotenv-dashboard-secret\n"
        "DASHBOARD_ADMIN_EMAIL=dotenv-admin@example.test\n"
        "DASHBOARD_ADMIN_PASSWORD=dotenv-password-\u00e9\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(server, "settings", Settings(_env_file=env_file, BOT_MODE="bootstrap"))

    response = client.post(
        "/api/login",
        json={"email": " DOTENV-ADMIN@EXAMPLE.TEST ", "password": "dotenv-password-\u00e9"},
    )

    assert response.status_code == 200
    assert client.get("/api/fleet").status_code == 200
    fake_fetch.assert_awaited_once()
    server.get_pool.assert_not_awaited()


@pytest.mark.parametrize("cookie_secure, expected_status", [(True, 401), (False, 200)])
def test_http_login_requires_explicit_local_cookie_override(
    auth_client, monkeypatch, cookie_secure, expected_status
):
    _, fake_fetch = auth_client
    monkeypatch.setattr(server.settings, "DASHBOARD_ADMIN_EMAIL", "admin@example.test")
    monkeypatch.setattr(server.settings, "DASHBOARD_ADMIN_PASSWORD", "isolated-password")
    monkeypatch.setattr(server.settings, "DASHBOARD_COOKIE_SECURE", cookie_secure)
    with TestClient(server.create_app(), base_url="http://127.0.0.1:8787") as client:
        response = client.post(
            "/api/login", json={"email": "admin@example.test", "password": "isolated-password"}
        )
        assert response.status_code == 200
        cookie = response.headers["set-cookie"]
        assert ("Secure" in cookie) is cookie_secure
        assert "HttpOnly" in cookie
        assert "SameSite=lax" in cookie
        assert client.get("/api/fleet").status_code == expected_status
    if cookie_secure:
        fake_fetch.assert_not_awaited()
    else:
        fake_fetch.assert_awaited_once()
    server.get_pool.assert_not_awaited()


def test_browser_auth_redirect_still_reaches_login(auth_client):
    client, fake_fetch = auth_client
    response = client.get("/", follow_redirects=False)
    assert response.status_code == 303
    assert response.headers["location"] == "/login"
    assert client.get("/login").status_code == 200
    fake_fetch.assert_not_awaited()


def test_health_and_login_page_remain_accessible_without_configuration(auth_client, monkeypatch):
    client, fake_fetch = auth_client
    monkeypatch.setattr(server.settings, "DASHBOARD_SECRET", "")
    monkeypatch.setattr(server, "_ok_status", lambda: (200, {"test": "healthy"}))
    for path in ("/health", "/api/health", "/login"):
        assert client.get(path).status_code == 200
    fake_fetch.assert_not_awaited()
    server.get_pool.assert_not_awaited()
