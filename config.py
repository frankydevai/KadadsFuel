"""
config.py  -  All configuration loaded from environment variables.

Required env vars:
  SAMSARA_API_TOKEN
  TELEGRAM_BOT_TOKEN
  DATABASE_URL          (PostgreSQL connection string from Railway)
  DISPATCHER_GROUP_ID   (Telegram group ID for admin/dispatcher alerts)

Optional env vars:
  PILOT_ZIP_URL              URL to download Pilot CSV zip
  LOVES_ZIP_URL              URL to download Love's XLSX zip
  FUEL_ALERT_THRESHOLD_PCT   Default: 35
  POLL_INTERVAL_HEALTHY      Default: 60 (minutes)
  POLL_INTERVAL_WATCH        Default: 20
  POLL_INTERVAL_CRITICAL_MOVING  Default: 10
  POLL_INTERVAL_CRITICAL_PARKED  Default: 60
  STATE_SAVE_INTERVAL_SECONDS    Default: 300
  CA_BORDER_REMINDER_MILES       Default: 150
  CA_BORDER_FUEL_THRESHOLD       Default: 70 (%)
  DEFAULT_TANK_GAL               Default: 150
  DEFAULT_MPG                    Default: 6.5
  MIN_SAVINGS_DISPLAY            Default: 3.0 (dollars)

Yards (up to 20):
  YARD_N=Name:latitude:longitude:radius_miles
  Example: YARD_1=Main Yard:28.4277:-81.3816:0.5
"""

import os
from dotenv import load_dotenv

load_dotenv()

# -- Samsara ------------------------------------------------------------------
SAMSARA_API_TOKEN = os.getenv("SAMSARA_API_TOKEN", "")
SAMSARA_BASE_URL  = "https://api.samsara.com"

# -- Telegram -----------------------------------------------------------------
TELEGRAM_BOT_TOKEN   = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
DISPATCHER_GROUP_ID  = os.getenv("DISPATCHER_GROUP_ID", "").strip()
ADMIN_CHAT_ID        = os.getenv("ADMIN_CHAT_ID", "").strip()   # your personal chat ID — only this user can upload price files

# -- PostgreSQL ---------------------------------------------------------------
DATABASE_URL = os.getenv("DATABASE_URL", "")

# -- Fuel price ZIP URLs ------------------------------------------------------
PILOT_ZIP_URL = os.getenv("PILOT_ZIP_URL", "")
LOVES_ZIP_URL = os.getenv("LOVES_ZIP_URL", "")

# -- Fuel threshold -----------------------------------------------------------
FUEL_ALERT_THRESHOLD_PCT = float(os.getenv("FUEL_ALERT_THRESHOLD_PCT", 35))

# -- Polling intervals (minutes) ----------------------------------------------
POLL_INTERVAL_HEALTHY          = int(os.getenv("POLL_INTERVAL_HEALTHY",          60))
POLL_INTERVAL_WATCH            = int(os.getenv("POLL_INTERVAL_WATCH",            20))
POLL_INTERVAL_CRITICAL_MOVING  = int(os.getenv("POLL_INTERVAL_CRITICAL_MOVING",  10))
POLL_INTERVAL_CRITICAL_PARKED  = int(os.getenv("POLL_INTERVAL_CRITICAL_PARKED",  60))

# -- State persistence --------------------------------------------------------
STATE_SAVE_INTERVAL_SECONDS = int(os.getenv("STATE_SAVE_INTERVAL_SECONDS", 300))

# -- Stop search --------------------------------------------------------------
SEARCH_CORRIDOR_MILES  = float(os.getenv("SEARCH_CORRIDOR_MILES",  300))  # max range ahead
CORRIDOR_WIDTH_MILES   = float(os.getenv("CORRIDOR_WIDTH_MILES",   8))    # miles either side of heading
BEHIND_PENALTY_MILES   = float(os.getenv("BEHIND_PENALTY_MILES",   15))   # penalty for stops behind truck
MIN_SAVINGS_DISPLAY    = float(os.getenv("MIN_SAVINGS_DISPLAY",    3.0))  # min $ savings to show line

# -- Truck defaults (when per-truck data unknown) -----------------------------
DEFAULT_TANK_GAL = float(os.getenv("DEFAULT_TANK_GAL", 150))
DEFAULT_MPG      = float(os.getenv("DEFAULT_MPG",      6.5))
SAFETY_RESERVE   = float(os.getenv("SAFETY_RESERVE",   0.10))  # 10% never use

# -- California border reminder -----------------------------------------------
CA_BORDER_REMINDER_MILES   = float(os.getenv("CA_BORDER_REMINDER_MILES",  150))
CA_BORDER_FUEL_THRESHOLD   = float(os.getenv("CA_BORDER_FUEL_THRESHOLD",  70))

# -- Visit detection ----------------------------------------------------------
VISIT_RADIUS_MILES = float(os.getenv("VISIT_RADIUS_MILES", 0.35))

# -- Yards --------------------------------------------------------------------
YARDS = []
for _i in range(1, 20):
    _val = os.getenv(f"YARD_{_i}", "").strip()
    if not _val:
        continue
    _parts = _val.split(":")
    if len(_parts) != 4:
        continue
    try:
        YARDS.append({
            "name":         _parts[0].strip(),
            "lat":          float(_parts[1]),
            "lng":          float(_parts[2]),
            "radius_miles": float(_parts[3]),
        })
    except ValueError:
        pass
