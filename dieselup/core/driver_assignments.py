"""Reconcile known Telegram driver groups without sending any messages."""
from __future__ import annotations

import asyncio
import logging
import re
from collections import Counter
from typing import Any

from dieselup.clients.samsara import extract_samsara_index_keys
from dieselup.config import settings
from dieselup.core.operating_scope import allows, allowed_units, unit_key
from dieselup.db import get_pool

log = logging.getLogger(__name__)
_refresh_lock = asyncio.Lock()
FIELDS = ('truck_unit', 'driver_full_name', 'driver_telegram_id',
          'samsara_vehicle_id', 'telegram_group_name', 'assignment_status', 'alerts_paused')
INACTIVE = re.compile(r'\b(?:home[\s_-]*time|terminated)\b', re.I)
NO_FUEL = re.compile(r'\bno\s+(?:gas|fuel)\b', re.I)
TITLE = re.compile(r'^\s*(?:(?:owner\s*op|truck|unit|subunit|sub)\s*)?#?\s*(\d+[A-Za-z]?)\b\s*[-–—:|]?\s*(.*)$', re.I)
PAY = re.compile(r'\s*(?:[|I]\s*)?\d+(?:\.\d+)?\s*(?:%|cpm)\s*$', re.I)


def is_inactive_title(title: str | None) -> bool:
    """Home Time and Terminated groups must not own a truck connection."""
    return bool(INACTIVE.search(title or ''))


def key(unit: Any) -> str:
    return str(unit or '').upper().lstrip('0') or '0'


def parse_title(title: str) -> tuple[str, str] | None:
    """Parse only a truck-number prefix; never use a pay rate as a truck ID."""
    if is_inactive_title(title):
        return None
    match = TITLE.match(title)
    if not match:
        return None
    driver = match[2].split('|')[0].strip()
    driver = re.sub(r'^OWNER\s*OP\s+', '', driver, flags=re.I)
    driver = PAY.sub('', driver).strip(' -–—:|')
    if not driver or not any(c.isalpha() for c in driver):
        return None
    return match[1], driver


def plan_assignments(rows: list[dict], groups: dict[int, str | None], vehicles: list[dict]) -> dict:
    """Plan a complete snapshot first, so order cannot steal a group's truck.

    Group titles are assignment metadata; Samsara validates vehicle identity.
    Existing pause flags are preserved. Missing/error observations fail closed.
    """
    original = {str(r['truck_unit']): dict(r) for r in rows}
    result = {u: dict(r) for u, r in original.items()}
    owners = {int(r['driver_telegram_id']): u for u, r in original.items() if r.get('driver_telegram_id')}
    units: dict[str, list[str]] = {}
    for unit in original:
        units.setdefault(key(unit), []).append(unit)
    parsed = {chat: parse_title(title) for chat, title in groups.items() if title is not None}
    claims = Counter(key(p[0]) for p in parsed.values() if p)
    inactive = {chat for chat, title in groups.items() if is_inactive_title(title)}
    verified, issues, candidates = {}, [], []

    # Retire explicitly inactive groups, retaining their last title for review.
    for chat in inactive:
        if chat in owners:
            r = result[owners[chat]]
            r.update(driver_telegram_id=None, telegram_group_name=groups[chat],
                     driver_full_name=None, assignment_status='unlinked')
    for chat, title in groups.items():
        current = owners.get(chat)
        if chat in inactive:
            continue
        p = parsed.get(chat)
        reason = None
        if title is None:
            reason = 'group_unreadable'
        elif not p:
            reason = 'invalid_title'
        elif claims[key(p[0])] > 1:
            reason = 'multiple_groups_for_truck'
        elif len(units.get(key(p[0]), [])) > 1:
            reason = 'ambiguous_roster_unit'
        if reason:
            if current:
                result[current]['assignment_status'] = 'conflict'
                if title is not None:
                    result[current]['telegram_group_name'] = title
            issues.append({'chat_id': chat, 'truck_unit': current, 'reason': reason})
            continue
        unit = (units.get(key(p[0])) or [p[0]])[0]
        target = original.get(unit)
        target_chat = target.get('driver_telegram_id') if target else None
        # A moving incumbent is releasable only after its own candidate validates.
        if target_chat and target_chat != chat and target_chat not in inactive:
            incumbent = parsed.get(int(target_chat))
            if not incumbent or key(incumbent[0]) == key(unit):
                reason = 'target_group_conflict'
        matches = [v for v in vehicles if key(unit) in {key(k) for k in extract_samsara_index_keys(v['name'])}]
        prior = [v for v in matches if target and str(v['id']) == str(target.get('samsara_vehicle_id'))]
        vehicle = prior[0] if len(prior) == 1 else matches[0] if len(matches) == 1 else None
        if not vehicle:
            reason = reason or 'vehicle_not_unique'
        elif any(str(r.get('samsara_vehicle_id')) == str(vehicle['id']) and u != unit for u, r in original.items()):
            reason = reason or 'vehicle_already_assigned'
        if reason:
            if current:
                result[current]['assignment_status'] = 'conflict'
                result[current]['telegram_group_name'] = title
            issues.append({'chat_id': chat, 'truck_unit': current, 'reason': reason})
        else:
            candidates.append((chat, unit, p[1], str(vehicle['id']), title))

    # Resolve dependencies before clearing anything (supports swaps and cycles).
    vehicle_claims = Counter(c[3] for c in candidates)
    accepted = {}
    for c in candidates:
        if vehicle_claims[c[3]] > 1:
            if c[0] in owners:
                result[owners[c[0]]]['assignment_status'] = 'conflict'
            issues.append({'chat_id': c[0], 'truck_unit': c[1], 'reason': 'multiple_units_for_vehicle'})
        else:
            accepted[c[0]] = c
    while True:
        blocked = []
        for chat, unit, *_ in accepted.values():
            incumbent = original.get(unit, {}).get('driver_telegram_id')
            if incumbent and incumbent != chat and incumbent not in inactive and incumbent not in accepted:
                blocked.append(chat)
        if not blocked:
            break
        for chat in blocked:
            c = accepted.pop(chat)
            if chat in owners:
                result[owners[chat]]['assignment_status'] = 'conflict'
                result[owners[chat]]['telegram_group_name'] = groups[chat]
            issues.append({'chat_id': chat, 'truck_unit': c[1], 'reason': 'blocked_by_unverified_group'})
    for chat, unit, *_ in accepted.values():
        current = owners.get(chat)
        if current and current != unit:
            result[current].update(driver_telegram_id=None, driver_full_name=None,
                                   telegram_group_name=None, assignment_status='unlinked')
    for chat, unit, driver, vid, title in accepted.values():
        r = result.setdefault(unit, {'truck_unit': unit, 'alerts_paused': False})
        paused = bool(original.get(unit, {}).get('alerts_paused')) or bool(original.get(owners.get(chat), {}).get('alerts_paused')) or bool(NO_FUEL.search(title))
        r.update(driver_full_name=driver, driver_telegram_id=chat, samsara_vehicle_id=vid,
                 telegram_group_name=title, alerts_paused=paused,
                 assignment_status='paused' if paused else 'ready')
        if not paused:
            verified[unit] = chat
    changed = [r for u, r in result.items() if u not in original or any(r.get(f) != original[u].get(f) for f in FIELDS)]
    return {'changes': changed, 'verified': verified, 'issues': issues,
            'inactive_groups': len(inactive), 'observed_groups': len(groups)}


async def apply_assignments(conn: Any, before: list[dict], plan: dict) -> int:
    """Apply in one short transaction with an optimistic-concurrency check."""
    if not plan['changes']:
        return 0
    async with conn.transaction():
        locked = [dict(r) for r in await conn.fetch('SELECT * FROM trucks_drivers ORDER BY truck_unit FOR UPDATE')]
        fingerprint = lambda rs: {str(r['truck_unit']): tuple(r.get(f) for f in (*FIELDS, 'updated_at')) for r in rs}
        if fingerprint(locked) != fingerprint(before):
            raise RuntimeError('Assignments changed during refresh; retry with a fresh snapshot')
        # Separate statements avoid non-deferrable unique-index races in CTEs.
        for r in plan['changes']:
            await conn.execute('UPDATE trucks_drivers SET driver_telegram_id=NULL WHERE truck_unit=$1', r['truck_unit'])
        existing_units = {r['truck_unit'] for r in before}
        for r in plan['changes']:
            statement = '''
                INSERT INTO trucks_drivers (truck_unit,driver_full_name,driver_telegram_id,
                    samsara_vehicle_id,telegram_group_name,assignment_status,alerts_paused)
                VALUES ($1,$2,$3,$4,$5,$6,$7)
            '''
            # A concurrently inserted new truck must fail, never be overwritten.
            if r['truck_unit'] in existing_units:
                statement += ''' ON CONFLICT (truck_unit) DO UPDATE SET
                    driver_full_name=EXCLUDED.driver_full_name,
                    driver_telegram_id=EXCLUDED.driver_telegram_id,
                    samsara_vehicle_id=EXCLUDED.samsara_vehicle_id,
                    telegram_group_name=EXCLUDED.telegram_group_name,
                    assignment_status=EXCLUDED.assignment_status,
                    alerts_paused=EXCLUDED.alerts_paused, updated_at=NOW()
                '''
            await conn.execute(statement, *(r.get(f) for f in FIELDS))
    return len(plan['changes'])


async def refresh_driver_assignments(bot: Any) -> dict[str, int]:
    async with _refresh_lock:
        return await _refresh_driver_assignments(bot)


async def _refresh_driver_assignments(bot: Any) -> dict[str, int]:
    from dieselup.bot.group_link import _read_current_group_title
    from dieselup.clients.samsara import SamsaraClient
    pool = await get_pool()
    rows = [dict(r) for r in await pool.fetch('SELECT * FROM trucks_drivers ORDER BY truck_unit')]
    groups = {}
    for r in rows:
        if not allows(r['truck_unit']):
            continue
        chat = r.get('driver_telegram_id')
        if chat:
            try:
                groups[int(chat)] = await _read_current_group_title(bot, int(chat))
            except Exception:
                groups[int(chat)] = None
                log.warning('assignment_refresh: unreadable group for truck %s', r['truck_unit'])
    # Do not let a selected group's rename move ownership out of the test.
    for chat, title in list(groups.items()):
        parsed = parse_title(title or '')
        if parsed and not allows(parsed[0]):
            groups[chat] = None
    try:
        async with SamsaraClient() as client:
            vehicles = [{'id': v.id, 'name': v.name} for v in await client.list_vehicles()]
    except Exception:
        # Removing an explicitly inactive connection never requires a vehicle
        # lookup. Retain other assignments if Samsara cannot verify them.
        inactive_groups = {chat: title for chat, title in groups.items() if is_inactive_title(title)}
        retirement = plan_assignments(rows, inactive_groups, [])
        async with pool.acquire() as conn:
            count = await apply_assignments(conn, rows, retirement)
        log.warning('assignment_refresh: vehicle lookup failed; %d inactive connections unlinked', count)
        raise
    plan = plan_assignments(rows, groups, vehicles)
    plan = controlled_assignment_plan(rows, groups, plan)
    async with pool.acquire() as conn:
        count = await apply_assignments(conn, rows, plan)
    log.info('assignment_refresh: %d updated, %d verified, %d need review', count, len(plan['verified']), len(plan['issues']))
    return plan['verified']


def controlled_assignment_plan(rows: list[dict], groups: dict, plan: dict) -> dict:
    """Validate existing links without automatically changing active ownership.

    Explicit Home Time/Terminated safety disconnections still apply to selected
    trucks. Other trucks keep their connections and historical records intact.
    """
    result = {**plan, 'changes': [r for r in plan['changes'] if allows(r['truck_unit'])],
              'verified': {u: c for u, c in plan['verified'].items() if allows(u)}}
    if settings.AUTO_LINK_ENABLED:
        return result
    original = {str(r['truck_unit']): r for r in rows}
    proposed = {str(r['truck_unit']): r for r in plan['changes']}
    result['changes'] = [r for r in result['changes']
                         if is_inactive_title(groups.get(original.get(str(r['truck_unit']), {}).get('driver_telegram_id')))]
    verified = {}
    for unit, chat in result['verified'].items():
        old = original.get(unit)
        new = proposed.get(unit, old)
        if not old or not new or old.get('alerts_paused'):
            continue
        normalize = lambda name: ' '.join(str(name or '').casefold().split())
        if (old.get('driver_telegram_id') == chat
                and str(old.get('samsara_vehicle_id')) == str(new.get('samsara_vehicle_id'))
                and normalize(old.get('driver_full_name')) == normalize(new.get('driver_full_name'))):
            verified[unit] = chat
            # Clear a transient verification failure without creating or
            # moving a link. Keep the saved owner and vehicle exactly as-is.
            if old.get('assignment_status') == 'conflict':
                result['changes'].append({**old, 'assignment_status': 'ready'})
    result['verified'] = verified
    return result


async def connect_driver_group(bot: Any, chat_id: int, title: str, unit: str, vehicle_id: str) -> bool:
    """Store an add/rename event through the same transactional assignment path."""
    from dieselup.bot.group_link import _read_current_group_title
    if not allows(unit):
        raise ValueError('This truck is outside the active three-truck test.')
    async with _refresh_lock:
        pool = await get_pool()
        rows = [dict(r) for r in await pool.fetch('SELECT * FROM trucks_drivers ORDER BY truck_unit')]
        target = next((r for r in rows if key(r['truck_unit']) == key(unit)), None)
        parsed = parse_title(title)
        if not parsed or key(parsed[0]) != key(unit):
            raise ValueError('Use a group title containing the truck number and driver name.')
        groups = {chat_id: title}
        incumbent = target.get('driver_telegram_id') if target else None
        if incumbent and incumbent != chat_id:
            try:
                groups[int(incumbent)] = await _read_current_group_title(bot, int(incumbent))
            except Exception:
                groups[int(incumbent)] = None
        plan = plan_assignments(rows, groups, [{'id': vehicle_id, 'name': unit}])
        if any(not allows(r['truck_unit']) for r in plan['changes']):
            raise ValueError('This change would affect a truck outside the active test.')
        canonical = str(target['truck_unit']) if target else unit
        proposed = next((r for r in plan['changes'] if r['truck_unit'] == canonical), target)
        if not proposed or proposed.get('driver_telegram_id') != chat_id or proposed.get('assignment_status') == 'conflict':
            raise ValueError('Another group or vehicle assignment needs review before this connection can change.')
        async with pool.acquire() as conn:
            await apply_assignments(conn, rows, plan)
        return canonical in plan['verified']
