"""Adding the bot must not send confirmations, warnings or old briefings."""
import asyncio
from types import SimpleNamespace
import pytest
from dieselup.bot import group_link
from dieselup.core import driver_assignments
from dieselup.clients.samsara import SamsaraError


@pytest.mark.parametrize('event', ['membership', 'service_message', 'title_change'])
@pytest.mark.parametrize('scenario', [
    'success', 'paused', 'fallback_error', 'fallback_empty', 'missing_unit',
    'missing_vehicle', 'ambiguous', 'lookup_error', 'conflict', 'home_time', 'terminated',
])
def test_bot_added_updates_connections_without_any_messages(monkeypatch, event, scenario):
    connected, unlinked = [], []
    title = {
        'missing_unit': 'New Driver Group',
        'home_time': '*Home Time* 8089 Driver Name',
        'terminated': '*Terminated* 8089 Driver Name',
    }.get(scenario, '8089 Driver Name')

    class Samsara:
        async def __aenter__(self): return self
        async def __aexit__(self, *args): pass
        async def find_vehicles_by_unit(self, unit):
            assert unit == '8089'
            if scenario in {'lookup_error', 'fallback_error'}:
                raise SamsaraError('Vehicle service unavailable')
            if scenario in {'missing_vehicle', 'fallback_empty'}:
                return []
            if scenario == 'ambiguous':
                return [SimpleNamespace(id='v1', name='8089'), SimpleNamespace(id='v2', name='8089')]
            return [SimpleNamespace(id='v8089', name='8089')]

    async def fetch_one(sql, *args):
        if 'UPDATE trucks_drivers' in sql:
            assert scenario in {'home_time', 'terminated'}
            assert 'driver_telegram_id = NULL' in sql
            unlinked.append(args)
            return {'truck_unit': '8089'}
        if scenario in {'fallback_error', 'fallback_empty'}:
            return {'truck_unit': '8089', 'samsara_vehicle_id': 'saved-v8089'}
        return None

    async def connect(bot, chat_id, chat_title, unit, vehicle_id):
        if scenario == 'conflict':
            raise ValueError('Another group already owns this truck')
        connected.append((chat_id, chat_title, unit, vehicle_id))
        return scenario != 'paused'

    async def unexpected_send(*args, **kwargs):
        raise AssertionError('Bot-added events must not send group/admin messages or replay alerts')

    monkeypatch.setattr(group_link, 'SamsaraClient', Samsara)
    monkeypatch.setattr(group_link, 'fetch_one', fetch_one)
    monkeypatch.setattr(driver_assignments, 'connect_driver_group', connect)
    monkeypatch.setattr(group_link, 'safe_send', unexpected_send)
    monkeypatch.setattr(group_link, '_replay_latest_pending_alert', unexpected_send)
    bot = SimpleNamespace(id=12345)
    chat = SimpleNamespace(id=-1008089, type='supergroup', title=title)
    context = SimpleNamespace(bot=bot)
    if event == 'membership':
        update = SimpleNamespace(my_chat_member=SimpleNamespace(
            chat=chat, old_chat_member=SimpleNamespace(status='left'),
            new_chat_member=SimpleNamespace(status='member')))
        asyncio.run(group_link.on_my_chat_member(update, context))
    elif event == 'service_message':
        update = SimpleNamespace(effective_chat=chat, effective_message=SimpleNamespace(
            new_chat_members=[SimpleNamespace(id=bot.id)]))
        asyncio.run(group_link.on_new_chat_members(update, context))
    else:
        update = SimpleNamespace(effective_chat=chat, effective_message=SimpleNamespace(
            new_chat_title=title))
        asyncio.run(group_link.on_new_chat_title(update, context))

    if scenario in {'success', 'paused', 'fallback_error', 'fallback_empty'}:
        assert len(connected) == 1
        assert connected[0][0:3] == (chat.id, title, '8089')
        assert connected[0][3] == ('saved-v8089' if scenario.startswith('fallback') else 'v8089')
    else:
        assert not connected
    if scenario in {'home_time', 'terminated'}:
        assert unlinked == [(chat.id, title, None)]
    else:
        assert not unlinked
