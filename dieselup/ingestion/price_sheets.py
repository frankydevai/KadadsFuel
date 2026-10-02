"""Read supplier diesel quotes without deriving their date from upload time."""
from datetime import date, datetime, timezone
import html
import math
import re
from dieselup.ingestion.pilot_parser import _open_first_sheet, parse_pilot_xls

LOVES_COLUMNS = ('Customer', 'Loves Store No.', 'City', 'State', 'Retail Price',
                 'Best Discounted Price', 'Effective Date')
FTS_COLUMNS = ('Name', 'Address', 'City', 'State', 'Retail Price', 'Customer Price')
US_STATES = set('AL AK AZ AR CA CO CT DE DC FL GA HI ID IL IN IA KS KY LA ME MD MA MI MN MS MO MT NE NV NH NJ NM NY NC ND OH OK OR PA RI SC SD TN TX UT VT VA WA WV WI WY'.split())


def _date(value):
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value).strip()
    for fmt in ('%Y-%m-%d', '%m/%d/%Y'):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    raise ValueError('Price effective date must be YYYY-MM-DD or MM/DD/YYYY')


def _text(value):
    return html.unescape(str(value or '')).strip()


def _price(value):
    try:
        price = float(str(value).replace('$', '').replace(',', '').strip())
    except (ValueError, TypeError):
        raise ValueError('Invalid diesel price') from None
    if not math.isfinite(price) or not 2 <= price <= 8:
        raise ValueError('Diesel price must be between $2 and $8 per gallon')
    return price


def _header(sheet, columns):
    for r in range(min(sheet.nrows, 40)):
        row = [_text(sheet.cell_value(r, c)) for c in range(sheet.ncols)]
        if all(name in row for name in columns):
            if any(row.count(name) != 1 for name in columns):
                raise ValueError('Duplicate required price columns')
            return r, {name: row.index(name) for name in row if name}
    return None


def caption_date(caption):
    """An explicit admin-supplied quote date; never an upload/file timestamp."""
    dates = re.findall(r'\beffective_date\s*=\s*(\d{4}-\d{2}-\d{2})\b', caption or '', re.I)
    if len(set(dates)) > 1:
        raise ValueError('Use one effective_date=YYYY-MM-DD in the file caption')
    return _date(dates[0]) if dates else None


def _unique_date(dates, explicit):
    if len(dates) > 1:
        raise ValueError('Mixed price effective dates; upload one daily price sheet')
    intrinsic = next(iter(dates)) if dates else None
    if intrinsic and explicit and intrinsic != explicit:
        raise ValueError('Caption date conflicts with the price date inside the workbook')
    return intrinsic or explicit


def parse_price_sheet(path, caption=None):
    sheet = _open_first_sheet(str(path))
    explicit = caption_date(caption)
    loves = _header(sheet, LOVES_COLUMNS)
    fts = _header(sheet, FTS_COLUMNS)
    quotes = []
    dates = set()
    row_issues = []
    if loves:
        provider = 'loves'
        header, columns = loves
        accounts = set()
        for r in range(header + 1, sheet.nrows):
            cell = lambda key: sheet.cell_value(r, columns[key])
            if not _text(cell('Loves Store No.')):
                continue
            account = _text(cell('Customer'))
            if not account:
                raise ValueError(f'Row {r+1}: missing Love\'s customer')
            accounts.add(account)
            number = _text(cell('Loves Store No.'))
            if not re.fullmatch(r'\d+(?:\.0)?', number) or int(float(number)) <= 0:
                raise ValueError(f'Row {r+1}: invalid Love\'s store number')
            number = str(int(float(number)))
            raw_day = cell('Effective Date')
            day = _date(raw_day) if _text(raw_day) else None
            if day:
                dates.add(day)
            try:
                customer_price = _price(cell('Best Discounted Price'))
                retail_price = _price(cell('Retail Price'))
            except ValueError as exc:
                row_issues.append({'row':r+1,'reason':str(exc)})
                continue
            quotes.append({'station_key': number, 'station_name': "Love's #" + number,
                           'address': None, 'city': _text(cell('City')), 'state': _text(cell('State')).upper(),
                           'your_price': customer_price,
                           'retail_price': retail_price, 'effective_date': day})
        if len(accounts) != 1:
            raise ValueError('A single Love\'s customer is required')
        account = next(iter(accounts))
    elif fts:
        provider = 'fts'
        header, columns = fts
        titles = {_text(sheet.cell_value(r,c)) for r in range(header) for c in range(sheet.ncols)}
        feeds = {m.group(1).strip() for title in titles if (m := re.fullmatch(r'Price Feed\s*-\s*(.+)', title, re.I))}
        if len(feeds) != 1:
            raise ValueError('A single Price Feed - <account> title is required')
        account = next(iter(feeds))
        for r in range(header + 1, sheet.nrows):
            cell = lambda key: sheet.cell_value(r, columns[key])
            if not _text(cell('Name')):
                continue
            identity = [_text(cell(k)) for k in ('Name','Address','City','State')]
            if not all(identity):
                row_issues.append({'row':r+1,'reason':'Incomplete station identity'})
                continue
            if not re.search(r'\bpilot\b|\bflying\s*j\b',identity[0],re.I):
                row_issues.append({'row':r+1,'reason':'FTS Plus is enabled for Pilot/Flying J only'})
                continue
            if identity[3].upper() not in US_STATES:
                row_issues.append({'row':r+1,'reason':'Non-US station; currency and volume units are not verified'})
                continue
            try:
                customer_price,retail_price = _price(cell('Customer Price')),_price(cell('Retail Price'))
            except ValueError as exc:
                row_issues.append({'row':r+1,'reason':str(exc)})
                continue
            day = _date(cell('Effective Date')) if 'Effective Date' in columns else explicit
            if day:
                dates.add(day)
            quotes.append({'station_key': '|'.join(identity).casefold(), 'station_name': identity[0],
                           'address': identity[1], 'city': identity[2], 'state': identity[3].upper(),
                           'your_price': customer_price, 'retail_price': retail_price,
                           'effective_date': day})
    else:
        parsed = parse_pilot_xls(str(path))
        provider = 'pilot'
        account = parsed['account_number']
        for r in range(min(sheet.nrows, 40)):
            for c in range(sheet.ncols):
                text = _text(sheet.cell_value(r,c))
                match = re.search(r'Effective Date\s*:\s*(.+)', text, re.I)
                if match:
                    for raw in re.findall(r'\d{1,2}/\d{1,2}/\d{4}|\d{4}-\d{2}-\d{2}', match[1]):
                        dates.add(_date(raw))
        day = _unique_date(dates, explicit)
        for q in parsed['stops']:
            quotes.append({**q, 'station_key': str(q['site_id']), 'station_name': None, 'address': None,
                           'effective_date': day})
    day = _unique_date(dates, explicit)
    if not quotes:
        raise ValueError('No priced diesel rows found')
    unique = {}
    for q in quotes:
        if not q['city'] or not re.fullmatch('[A-Z]{2}',q['state']):
            raise ValueError('A city and two-letter state are required for every station')
        q['effective_date'] = day
        key = q['station_key']
        if key in unique and unique[key] != q:
            raise ValueError('Conflicting prices for the same supplier station')
        unique[key] = q
    intrinsic = bool(dates) and (provider != 'fts' or 'Effective Date' in columns)
    return {'provider': provider, 'account_number': account, 'effective_date': day,
            'date_source':'supplier' if intrinsic else 'caption' if explicit else 'unknown',
            'stops': list(unique.values()), 'row_count': len(unique), 'excluded_rows':len(row_issues),
            'row_issues':row_issues, 'parsed_at': datetime.now(timezone.utc)}
