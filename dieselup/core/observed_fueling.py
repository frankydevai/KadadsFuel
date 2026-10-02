"""Resolve an attributed sensor fill with dated prices, including unknown-price holds."""
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from dieselup.config import settings
from dieselup.core.fueling_analysis import compare_fueling
from dieselup.db import fetch_one
from dieselup.core.price_sources import pilot_price_rows


async def analysis(event, observed, advised):
    as_of = observed.get("detected_at") or datetime.now(timezone.utc)
    price_time = as_of.astimezone(ZoneInfo("America/New_York"))
    actual = None
    if observed.get('site_id') is not None:
        actual = await fetch_one(f"""WITH prices AS ({pilot_price_rows('$2','$4')})
            SELECT your_price,effective_date,station_name,address,city,state,latitude,longitude,site_id,
                   price_provider,price_date_source FROM prices
            WHERE site_id=$1 AND $3::date-effective_date BETWEEN 0 AND $5
            ORDER BY effective_date DESC,uploaded_at DESC LIMIT 1""",
            observed['site_id'],settings.FTS_PRICE_CUSTOMER,price_time.date(),
            settings.PILOT_ACCOUNT_NUMBER,settings.MAX_FUEL_PRICE_AGE_DAYS)

    price = float(actual['your_price']) if actual else None
    result = compare_fueling(float(observed['gallons']),advised.get('your_price'),price,
                             advised.get('price_date'),actual['effective_date'] if actual else None,as_of=price_time)
    result.update(fueling_confirmed=True,classification=observed['classification'],
                  actual_site_id=observed.get('site_id'),actual_station_name=actual['station_name'] if actual else None,
                  fueling_at=as_of.isoformat())
    if actual:
        result['actual_price_date_source'] = actual.get('price_date_source','unknown')
        if result['actual_price_date_source'] == 'upload_day':
            result['price_source'] = 'Uploaded carrier contract sheet; date based on upload day; actual receipt not reconciled'
    return result,dict(actual) if actual else None
