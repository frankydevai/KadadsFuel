import asyncio
import pytest
import sys
import types
from types import SimpleNamespace

try:
    import xlrd
except ImportError:
    xlrd_stub = types.ModuleType("xlrd")
    xlrd_stub.XLRDError = Exception
    sys.modules.setdefault("xlrd", xlrd_stub)

from dieselup.bot import admin, group_link
from dieselup.core import driver_assignments
from dieselup.clients.samsara import SamsaraError


def test_group_link_parser_accepts_both_command_orders():
    assert admin._parse_group_link_args(["-1003870703515", "1863"]) == (
        -1003870703515,
        "1863",
    )
    assert admin._parse_group_link_args(["1863", "-1003870703515"]) == (
        -1003870703515,
        "1863",
    )
    assert admin._parse_group_link_args(["1863", "1003870703515"]) is None


def test_settgid_unlinks_existing_owner_before_upsert(monkeypatch):
    statements = []

    async def fake_execute(sql, *args):
        statements.append((sql, args))

    class Message:
        async def reply_text(self, text):
            return None

    monkeypatch.setattr(admin, "execute", fake_execute)
    update = SimpleNamespace(effective_message=Message())
    context = SimpleNamespace(args=["1863", "-1003870703515"])

    asyncio.run(admin.settgid(update, context))

    assert len(statements) == 1
    sql, args = statements[0]
    assert "WHERE driver_telegram_id = $2" in sql
    assert "truck_unit <> $1" in sql
    assert args == ("1863", -1003870703515)


def test_group_link_uses_existing_mapping_when_samsara_listing_fails(monkeypatch):
    sent: list[dict] = []
    executed: list[tuple[str, tuple]] = []
    replayed: list[tuple[int, str]] = []

    class FailingSamsara:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *exc):
            return None

        async def find_vehicles_by_unit(self, unit):
            assert unit == "1863"
            raise SamsaraError("Samsara request to /fleet/vehicles failed: ReadError")

    async def fake_fetch_one(sql, *args):
        if "FROM trucks_drivers" in sql:
            return {
                "truck_unit": "1863",
                "driver_full_name": "Driver",
                "driver_telegram_id": None,
                "samsara_vehicle_id": "281474999999999",
            }
        return None

    async def fake_connect(bot, chat_id, title, unit, vehicle_id):
        executed.append((unit, chat_id, title, vehicle_id))
        return True

    async def fake_safe_send(**kwargs):
        sent.append(kwargs)
        return 100 + len(sent)

    async def fake_replay_latest_pending_alert(*, bot, chat_id, truck_unit):
        replayed.append((chat_id, truck_unit))

    monkeypatch.setattr(group_link, "SamsaraClient", lambda: FailingSamsara())
    monkeypatch.setattr(group_link, "fetch_one", fake_fetch_one)
    monkeypatch.setattr(driver_assignments, "connect_driver_group", fake_connect)
    monkeypatch.setattr(group_link, "safe_send", fake_safe_send)
    monkeypatch.setattr(
        group_link,
        "_replay_latest_pending_alert",
        fake_replay_latest_pending_alert,
    )

    asyncio.run(
        group_link._attempt_link(
            bot=object(),
            chat_id=-1003870703515,
            chat_title="1863 Driver Name",
            reason="admin",
            unit_override="1863",
        )
    )

    assert executed == [("1863", -1003870703515, "1863 Driver Name", "281474999999999")]
    assert replayed == [(-1003870703515, "1863")]
    assert any("existing truck mapping" in call["text"] for call in sent)
    assert any("Group linked (admin_db_fallback)" in call["text"] for call in sent)


def test_home_time_title_is_detected_before_truck_digits():
    assert group_link.is_inactive_title("*Home Time* 0555 Jorel Mireus | 70 CPM")
    assert group_link.is_inactive_title("HOME   TIME - driver")
    assert not group_link.is_inactive_title("8143 Peterson Jerome |30%")


def test_group_title_extracts_driver_and_matches_team_roster():
    title = "8143 Peterson Jerome |30%"
    assert group_link._driver_name_from_group_title(title) == "Peterson Jerome"
    assert group_link.driver_names_match(
        "OTHER DRIVER / PETERSON JEROME", "Peterson Jerome"
    )
    assert not group_link.driver_names_match("JANE DRIVER", "Peterson Jerome")


def test_group_preflight_uses_shared_assignment_refresh(monkeypatch):
    async def refresh(bot):
        return {"8143": -100123}
    monkeypatch.setattr(driver_assignments, "refresh_driver_assignments", refresh)
    assert asyncio.run(group_link.refresh_and_verify_linked_groups(object())) == {"8143": -100123}


def test_read_group_title_retries_telegram_rate_limit(monkeypatch):
    calls = 0
    sleeps = []

    class Bot:
        async def get_chat(self, chat_id):
            nonlocal calls
            calls += 1
            if calls == 1:
                from telegram.error import RetryAfter
                raise RetryAfter(0.01)
            return SimpleNamespace(title="8143 Peterson Jerome |30%")

    async def fake_sleep(seconds):
        sleeps.append(seconds)

    monkeypatch.setattr(group_link.asyncio, "sleep", fake_sleep)
    title = asyncio.run(group_link._read_current_group_title(Bot(), -100123))
    assert title == "8143 Peterson Jerome |30%"
    assert calls == 2
    assert sleeps == [0.01, 0.05]


@pytest.mark.parametrize("title", [
    "*Home Time* 0555 Jorel Mireus | 70 CPM", "*Terminated* 0555 Old Driver",
    "HOME-TIME", "home_time 0555 Old Driver", "0555 Old Driver | TERMINATED",
])
def test_inactive_title_change_unlinks_before_any_link_attempt(monkeypatch, title):
    calls = []

    async def fake_unlink(**kwargs):
        calls.append(("unlink", kwargs))

    async def fail_link(**kwargs):
        raise AssertionError("inactive title must not be linked")

    monkeypatch.setattr(group_link, "_unlink_inactive_group", fake_unlink)
    monkeypatch.setattr(group_link, "_attempt_link", fail_link)
    update = SimpleNamespace(
        effective_message=SimpleNamespace(new_chat_title=title),
        effective_chat=SimpleNamespace(id=-100123, type="supergroup"),
    )
    context = SimpleNamespace(bot=object())

    asyncio.run(group_link.on_new_chat_title(update, context))

    assert calls == [("unlink", {
        "bot": context.bot,
        "chat_id": -100123,
        "chat_title": title,
        "silent": True,
    })]


def test_title_change_to_new_truck_relinks_group(monkeypatch):
    calls = []

    async def fake_link(**kwargs):
        calls.append(kwargs)

    monkeypatch.setattr(group_link, "_attempt_link", fake_link)
    update = SimpleNamespace(
        effective_message=SimpleNamespace(new_chat_title="8143 Peterson Jerome |30%"),
        effective_chat=SimpleNamespace(id=-100123, type="supergroup"),
    )
    context = SimpleNamespace(bot=object())

    asyncio.run(group_link.on_new_chat_title(update, context))

    assert calls == [{
        "bot": context.bot,
        "chat_id": -100123,
        "chat_title": "8143 Peterson Jerome |30%",
        "reason": "title_change",
    }]


def test_home_time_unlink_clears_driver_group(monkeypatch):
    sent = []

    async def fake_fetch_one(sql, *args):
        assert "driver_telegram_id = NULL" in sql
        assert "RETURNING truck_unit" in sql
        assert args == (-100123, "*Home Time* 0555 Jorel Mireus | 70 CPM", None)
        return {"truck_unit": "0555"}

    async def fake_safe_send(**kwargs):
        sent.append(kwargs)
        return 1

    monkeypatch.setattr(group_link, "fetch_one", fake_fetch_one)
    monkeypatch.setattr(group_link, "safe_send", fake_safe_send)

    asyncio.run(group_link._unlink_inactive_group(
        bot=object(),
        chat_id=-100123,
        chat_title="*Home Time* 0555 Jorel Mireus | 70 CPM",
    ))

    assert any("Fuel alerts are paused" in call["text"] for call in sent)
    assert any("Inactive group unlinked" in call["text"] for call in sent)

@pytest.mark.parametrize('title', ['Home Time', '* TERMINATED * 0555 Old Driver'])
def test_added_inactive_group_is_unlinked_without_vehicle_lookup(monkeypatch, title):
    calls = []
    async def unlink(**kwargs): calls.append(kwargs)
    async def fail_lookup(*args): raise AssertionError('Inactive group must not look up or link a truck')
    monkeypatch.setattr(group_link, '_unlink_inactive_group', unlink)
    monkeypatch.setattr(group_link, '_existing_truck_mapping', fail_lookup)
    asyncio.run(group_link._attempt_link(bot=object(), chat_id=-100123, chat_title=title, reason='auto'))
    assert len(calls) == 1
    assert calls[0]['chat_id'] == -100123
    assert calls[0]['chat_title'] == title


def test_repeated_inactive_event_does_not_notify_again(monkeypatch):
    async def already_unlinked(*args): return None
    async def fail_send(**kwargs): raise AssertionError('No notification when already unlinked')
    monkeypatch.setattr(group_link, 'fetch_one', already_unlinked)
    monkeypatch.setattr(group_link, 'safe_send', fail_send)
    asyncio.run(group_link._unlink_inactive_group(bot=object(), chat_id=-100123, chat_title='Terminated'))
