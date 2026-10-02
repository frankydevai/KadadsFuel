"""One deployment-wide boundary for automatic truck processing."""
import re
from dieselup.config import settings


def unit_key(unit):
    value = str(unit or "").strip().upper()
    if not re.fullmatch(r"\d+[A-Z]?", value):
        return None
    return value.lstrip("0") or "0"


def allowed_units():
    value = settings.TEST_TRUCK_UNITS
    return [unit_key(part) for part in value.split(",")] if value else None


def allows(unit):
    units = allowed_units()
    return units is None or unit_key(unit) in units


def status():
    return {"test_truck_units": allowed_units() or [],
            "full_fleet": allowed_units() is None,
            "auto_link_enabled": settings.AUTO_LINK_ENABLED,
            "telegram_messaging_mode": settings.TELEGRAM_MESSAGING_MODE}
