"""Compare observed gallons with dated prices; an unknown price is never a loss."""
from datetime import date, datetime, timezone
import math
from dieselup.config import settings


def current_price_date(value, as_of=None):
    try:
        stamp = value if isinstance(value, date) else date.fromisoformat(str(value)[:10])
        if isinstance(stamp, datetime):
            stamp = stamp.date()
        today = as_of or datetime.now(timezone.utc).date()
        if isinstance(today, datetime):
            today = today.date()
        return 0 <= (today-stamp).days <= settings.MAX_FUEL_PRICE_AGE_DAYS
    except (ValueError, TypeError):
        return False


def compare_fueling(gallons, planned_price, actual_price, planned_date, actual_date, *, as_of=None):
    valid_quantity = isinstance(gallons, (int,float)) and math.isfinite(gallons) and gallons>0
    result = {"gallons_estimated":round(gallons,1) if valid_quantity else None,"price_status":"pending",
              "extra_cost":None,"saving":None,"quantity_source":"Samsara sensor estimate"}
    if not valid_quantity or not current_price_date(planned_date,as_of) or not current_price_date(actual_date,as_of):
        return result
    if not all(isinstance(v,(int,float)) and math.isfinite(v) and v>0 for v in (planned_price,actual_price)):
        return result
    difference = (actual_price-planned_price)*gallons
    return {**result,"price_status":"contracted_estimate","extra_cost":round(max(difference,0),2),
        "saving":round(max(-difference,0),2),"planned_price":planned_price,"actual_price":actual_price,
        "planned_price_date":str(planned_date),"actual_price_date":str(actual_date),
        "price_source":"Dated carrier contract prices; actual receipt not reconciled"}
