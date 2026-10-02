"""Atomic, visible import receipts and supplier-separated station quotes."""
from datetime import datetime
import hashlib
import json
import re
from pathlib import Path
from zoneinfo import ZoneInfo
from dieselup.config import settings
from dieselup.db import get_pool
from dieselup.ingestion.price_sheets import parse_price_sheet


def allowed_accounts():
    return {'pilot': settings.PILOT_ACCOUNT_NUMBER,
            'loves': settings.LOVES_PRICE_CUSTOMER,
            'fts': settings.FTS_PRICE_CUSTOMER}


def _norm(value):
    return re.sub(r'\s+', ' ', str(value or '').strip()).casefold()


def match_station(quote, provider, stations):
    """Require a unique catalog identity. No geocoding or guessed coordinates."""
    matches = []
    for stop in stations:
        if _norm(quote['city']) != _norm(stop['city']) or quote['state'] != stop['state'].upper():
            continue
        if provider == 'pilot':
            matches_identity = str(stop['pilot_site_id']) == quote['station_key']
        elif provider == 'loves':
            name = str(stop['station_name'])
            numbers = re.findall(r'#\s*(\d+)\b', name)
            matches_identity = ('love' in name.casefold() and len(numbers) == 1
                                and str(int(numbers[0])) == quote['station_key'])
        else:
            # FTS Plus is authorized for Pilot/Flying J only. Catalog rows
            # and the supplier name must both identify that network.
            name = str(quote['station_name'])
            if not re.search(r'\bpilot\b|\bflying\s*j\b',name,re.I):
                continue
            if not re.search(r'\bpilot\b|\bflying\s*j\b',str(stop['station_name']),re.I):
                continue
            numbers = re.findall(r'#\s*(\d+)\b',name)
            if len(numbers)==1:
                matches_identity = str(stop['pilot_site_id'])==str(int(numbers[0]))
            else:
                matches_identity = (_norm(stop['station_name']) == _norm(quote['station_name'])
                                    and _norm(stop['address']) == _norm(quote['address']))
        if matches_identity:
            matches.append(stop['id'])
    return matches[0] if len(matches) == 1 else None


async def save_import(*, upload_key, filename, path=None, caption=None, error=None, uploaded_at=None):
    digest = hashlib.sha256(Path(path).read_bytes()).hexdigest() if path and Path(path).is_file() else None
    result = None
    reason = error
    if not reason:
        try:
            result = parse_price_sheet(path, caption)
            if result['effective_date'] is None:
                upload_time = uploaded_at or datetime.now(ZoneInfo('UTC'))
                result['effective_date'] = upload_time.astimezone(ZoneInfo('America/New_York')).date()
                result['date_source'] = 'upload_day'
            expected = allowed_accounts().get(result['provider'])
            if not expected or result['account_number'] != expected:
                reason = 'Price sheet customer does not match the enabled supplier account.'
        except ValueError as exc:
            reason = str(exc)[:350]
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            receipt = await conn.fetchrow('''INSERT INTO price_file_imports
                (upload_key,filename,file_sha256,provider,account_number,effective_date,status,reason,row_count,date_source)
                VALUES($1,$2,$3,$4,$5,$6,'processing',$7,$8,$9)
                ON CONFLICT(upload_key) DO NOTHING RETURNING *''',
                upload_key,Path(filename).name[:200],digest,
                result['provider'] if result else None,result['account_number'] if result else None,
                result['effective_date'] if result else None,reason,result['row_count'] if result else 0,
                result['date_source'] if result else 'unknown')
            if receipt is None:
                return dict(await conn.fetchrow('SELECT * FROM price_file_imports WHERE upload_key=$1',upload_key))
            import_id = receipt['id']
            if reason:
                return dict(await conn.fetchrow("UPDATE price_file_imports SET status='failed' WHERE id=$1 RETURNING *",import_id))
            stations = await conn.fetch('SELECT id,pilot_site_id,station_name,address,city,state FROM fuel_stops')
            today = datetime.now(ZoneInfo('America/New_York')).date()
            day = result['effective_date']
            current = day is not None and 0 <= (today-day).days <= settings.MAX_FUEL_PRICE_AGE_DAYS
            if day is None:
                status,reason = 'held','Price date missing. Add effective_date=YYYY-MM-DD to the file caption using the supplier-confirmed date.'
            elif day > today:
                status,reason = 'held','Price date is in the future; quotes cannot be used yet.'
            elif not current:
                status,reason = 'held','Prices are outdated; upload a current supplier price sheet.'
            else:
                status,reason = 'completed',None
            matched = 0
            for quote in result['stops']:
                stop_id = match_station(quote,result['provider'],stations)
                matched += stop_id is not None
                await conn.execute('''INSERT INTO price_feed_quotes
                    (import_id,provider,account_number,station_key,fuel_stop_id,station_name,address,
                     city,state,your_price,retail_price,effective_date)
                    VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)''',
                    import_id,result['provider'],result['account_number'],quote['station_key'],stop_id,
                    quote['station_name'],quote['address'],quote['city'],quote['state'],
                    quote['your_price'],quote['retail_price'],day)
                if result['provider'] == 'pilot' and current:
                    # Retain the Pilot route contract, but never overwrite another
                    # account's quote when legacy uniqueness omits the account.
                    written = await conn.fetchval('''INSERT INTO contracted_prices
                        (site_id,city,state,your_price,retail_price,cost,effective_date,account_number)
                        VALUES($1,$2,$3,$4,$5,$6,$7,$8)
                        ON CONFLICT(site_id,effective_date) DO UPDATE SET
                        city=EXCLUDED.city,state=EXCLUDED.state,your_price=EXCLUDED.your_price,
                        retail_price=EXCLUDED.retail_price,cost=EXCLUDED.cost,uploaded_at=NOW()
                        WHERE contracted_prices.account_number=EXCLUDED.account_number
                        RETURNING id''',quote['site_id'],quote['city'],quote['state'],quote['your_price'],
                        quote['retail_price'],quote.get('cost'),day,result['account_number'])
                    if written is None:
                        raise ValueError('Legacy station/date belongs to another account; import rolled back.')
            if current and matched < result['row_count']:
                reason = f"{result['row_count']-matched} supplier rows have no unique station match; those rows are excluded from the map."
            notes = [reason] if reason else []
            if result['date_source'] == 'upload_day':
                notes.append('Undated supplier file activated using its upload day (America/New_York).')
            if result['excluded_rows']:
                notes.append(f"{result['excluded_rows']} invalid or ineligible rows excluded; see row details.")
            reason = ' '.join(notes) or None
            return dict(await conn.fetchrow('''UPDATE price_file_imports SET status=$2,reason=$3,
                matched_rows=$4,excluded_rows=$5,row_issues=$6::jsonb WHERE id=$1 RETURNING *''',
                import_id,status,reason,matched,result['excluded_rows'],json.dumps(result['row_issues'])))


async def activate_undated_import(import_id):
    """Apply the owner's upload-day policy without changing the original receipt."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            source = await conn.fetchrow('SELECT * FROM price_file_imports WHERE id=$1 FOR UPDATE',import_id)
            if not source or source['status'] != 'held' or source['effective_date'] is not None:
                return None
            if source['provider'] not in ('loves','fts'):
                return None
            if source['account_number'] != allowed_accounts().get(source['provider']):
                raise ValueError('Held import is not from an enabled supplier account')
            day = source['uploaded_at'].astimezone(ZoneInfo('America/New_York')).date()
            age = (datetime.now(ZoneInfo('America/New_York')).date()-day).days
            if not 0 <= age <= settings.MAX_FUEL_PRICE_AGE_DAYS:
                return None
            quotes = await conn.fetch('SELECT * FROM price_feed_quotes WHERE import_id=$1',import_id)
            if not quotes or len(quotes) != source['row_count']:
                raise ValueError('Held import does not have a complete stored quote set')
            if any(q['provider'] != source['provider'] or q['account_number'] != source['account_number']
                   or q['effective_date'] is not None for q in quotes):
                raise ValueError('Stored quote identity or date does not match the held import')
            key = source['upload_key']+':upload-day'
            receipt = await conn.fetchrow('''INSERT INTO price_file_imports
                (upload_key,filename,file_sha256,provider,account_number,effective_date,date_source,status,
                 reason,row_count,matched_rows,excluded_rows,row_issues)
                SELECT $2,filename,file_sha256,provider,account_number,$3,'upload_day','completed',
                    $4,row_count,matched_rows,excluded_rows,row_issues FROM price_file_imports WHERE id=$1
                ON CONFLICT(upload_key) DO NOTHING RETURNING *''',import_id,key,day,
                f'Undated sheet activated using original upload day under owner-confirmed policy; original held import #{import_id} preserved.')
            if receipt is None:
                return dict(await conn.fetchrow('SELECT * FROM price_file_imports WHERE upload_key=$1',key))
            await conn.execute('''INSERT INTO price_feed_quotes
                (import_id,provider,account_number,station_key,fuel_stop_id,station_name,address,city,state,
                 your_price,retail_price,effective_date)
                SELECT $2,provider,account_number,station_key,fuel_stop_id,station_name,address,city,state,
                    your_price,retail_price,$3 FROM price_feed_quotes WHERE import_id=$1''',import_id,receipt['id'],day)
            return dict(receipt)
