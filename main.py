"""
main.py  -  FleetFuel Bot entry point.

Runs two concurrent loops:
  1. Samsara polling loop  (every 30 seconds tick, trucks polled per their schedule)
  2. Price updater         (daily at 06:00 UTC via simple time check)
"""

import logging
import time
import signal
import sys
import os
from datetime import datetime, timedelta, timezone

from config import STATE_SAVE_INTERVAL_SECONDS
from database import init_db, load_all_truck_states, save_all_truck_states, reset_truck_states, auto_register_truck
from samsara_client import get_combined_vehicle_data
from config import QUICKMANAGE_API_KEY
from state_machine import process_truck
import telegram_bot
from telegram_bot import send_startup_message, send_price_update_notification, poll_for_uploads

# -- Logging ------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)

# -- State --------------------------------------------------------------------
truck_states     = {}
_running         = True

# -- Graceful shutdown --------------------------------------------------------
def _shutdown(signum, frame):
    global _running
    log.info("Shutdown signal — saving state...")
    save_all_truck_states(truck_states)
    _running = False

signal.signal(signal.SIGTERM, _shutdown)
signal.signal(signal.SIGINT,  _shutdown)


# -- Helpers ------------------------------------------------------------------
def _utcnow():
    return datetime.now(timezone.utc)


# -- Price updater scheduler --------------------------------------------------
_last_price_update = None   # Track last update time

def _should_update_prices(now: datetime) -> bool:
    """Run price update once daily at 06:00 UTC."""
    global _last_price_update
    if _last_price_update is None:
        return True  # Always run on startup
    hours_since = (now - _last_price_update).total_seconds() / 3600
    return hours_since >= 23 and now.hour == 6



# -- Main loop ----------------------------------------------------------------
def main():
    global truck_states

    log.info("FleetFuel Bot starting up...")
    log.info("Initializing database...")
    init_db()
    if os.getenv("RESET_DB", "0") == "1":
        log.info("RESET_DB=1 — clearing truck states...")
        reset_truck_states()

    # Load persisted truck states
    truck_states = load_all_truck_states()
    log.info(f"Loaded {len(truck_states)} truck states from DB.")

    try:
        send_startup_message()
    except Exception as e:
        log.warning(f"Could not send startup message: {e}")

    log.info("Polling loop started.")

    last_db_save    = _utcnow()
    last_upload_check = _utcnow()
    poll_cycle      = 0

    while _running:
        try:
            poll_cycle += 1
            now = _utcnow()

            # -- Check for admin file uploads (every 30 seconds) --------------
            if (now - last_upload_check).total_seconds() >= 30:
                try:
                    poll_for_uploads()
                except Exception as e:
                    log.error(f"Upload poll error: {e}")
                last_upload_check = now

            # -- Fetch from Samsara -------------------------------------------
            try:
                all_trucks = get_combined_vehicle_data()
            except Exception as e:
                log.error(f"Samsara fetch failed: {e}")
                time.sleep(60)
                continue

            # -- Fetch QuickManage routes (if API key configured) -------------
            qm_routes = {}
            if QUICKMANAGE_API_KEY:
                try:
                    from quickmanage_client import get_all_truck_routes
                    qm_routes = get_all_truck_routes()
                    if qm_routes:
                        log.info(f"QuickManage: routes loaded for {len(qm_routes)} trucks")
                except Exception as e:
                    log.warning(f"QuickManage fetch failed: {e}")

            # -- Find trucks due for polling -----------------------------------
            due_trucks = []
            for truck in all_trucks:
                vid = truck["vehicle_id"]
                if vid not in truck_states:
                    # Brand new truck — register and process immediately
                    auto_register_truck(vid, truck["vehicle_name"])
                    log.info(f"New truck: {truck['vehicle_name']} — registered, processing now.")
                    due_trucks.append(truck)
                else:
                    # Force check bypasses next_poll entirely
                    if telegram_bot.force_check_now:
                        due_trucks.append(truck)
                        continue
                    next_poll = truck_states[vid].get("next_poll")
                    if next_poll is None:
                        due_trucks.append(truck)
                    else:
                        if next_poll.tzinfo is None:
                            next_poll = next_poll.replace(tzinfo=timezone.utc)
                        if next_poll <= now:
                            due_trucks.append(truck)

            if telegram_bot.force_check_now:
                import main as _main
                _main.force_check_now = False
                log.info(f"/checknow: forcing check on all {len(due_trucks)} trucks")

            log.info(f"Poll #{poll_cycle}: {len(all_trucks)} trucks  "
                     f"{len(due_trucks)} due for check")

            # -- Process due trucks -------------------------------------------
            for truck in due_trucks:
                vid = truck["vehicle_id"]
                # Attach QuickManage route to truck state if available
                truck_num = truck.get("vehicle_name", "")
                if truck_num in qm_routes:
                    truck_states.setdefault(vid, {})["qm_route"] = qm_routes[truck_num]
                elif truck_states.get(vid, {}).get("qm_route"):
                    pass  # keep existing route
                try:
                    process_truck(vid, truck_states.get(vid, {}),
                                  truck, truck_states)
                except Exception as e:
                    log.error(f"Error processing {truck['vehicle_name']}: {e}", exc_info=True)

            # -- Periodic DB save ---------------------------------------------
            if (now - last_db_save).total_seconds() >= STATE_SAVE_INTERVAL_SECONDS:
                save_all_truck_states(truck_states)
                last_db_save = now

        except Exception as e:
            log.error(f"Unhandled error in poll cycle: {e}", exc_info=True)

        time.sleep(30)

    log.info("FleetFuel Bot stopped cleanly.")


if __name__ == "__main__":
    main()
