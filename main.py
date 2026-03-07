"""
main.py  -  FleetFuel Bot entry point.

Runs two concurrent loops:
  1. Samsara polling loop  (every 30 seconds tick, trucks polled per their schedule)
  2. Price updater         (daily at 06:00 UTC via simple time check)
"""

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
from state_machine import process_truck
from telegram_bot import send_startup_message, send_price_update_notification, poll_for_uploads
from price_updater import run_price_update

# -- Logging ------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)

# -- State --------------------------------------------------------------------
truck_states = {}
_running     = True

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


def _run_price_update():
    global _last_price_update
    log.info("Starting daily price update...")
    try:
        pilot_count, loves_count = run_price_update()
        _last_price_update = _utcnow()
    except Exception as e:
        log.error(f"Price update failed: {e}", exc_info=True)


# -- Main loop ----------------------------------------------------------------
def main():
    global truck_states

    log.info("FleetFuel Bot starting up...")
    log.info("Initializing database...")
    init_db()

    if os.getenv("RESET_DB", "0") == "1":
        log.info("RESET_DB=1 — clearing truck states...")
        reset_truck_states()

    # Run price update on startup
    _run_price_update()

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

            # -- Daily price update -------------------------------------------
            if _should_update_prices(now):
                _run_price_update()

            # -- Find trucks due for polling -----------------------------------
            due_trucks = []
            for vid, state in truck_states.items():
                next_poll = state.get("next_poll")
                if next_poll is None:
                    due_trucks.append(vid)
                elif isinstance(next_poll, datetime):
                    if next_poll.tzinfo is None:
                        next_poll = next_poll.replace(tzinfo=timezone.utc)
                    if next_poll <= now:
                        due_trucks.append(vid)

            # -- Fetch from Samsara -------------------------------------------
            try:
                all_trucks = get_combined_vehicle_data()
            except Exception as e:
                log.error(f"Samsara fetch failed: {e}")
                time.sleep(60)
                continue

            log.info(f"Poll #{poll_cycle}: {len(all_trucks)} trucks  "
                     f"{len(due_trucks)} due for check")

            # -- Process due trucks -------------------------------------------
            for vid in due_trucks:
                current_data = next(
                    (t for t in all_trucks if t["vehicle_id"] == vid), None
                )
                if current_data is None:
                    if vid in truck_states:
                        truck_states[vid]["next_poll"] = now + timedelta(minutes=30)
                    continue
                try:
                    process_truck(vid, truck_states.get(vid, {}),
                                  current_data, truck_states)
                except Exception as e:
                    log.error(f"Error processing {vid}: {e}", exc_info=True)

            # -- Discover new trucks ------------------------------------------
            for truck in all_trucks:
                vid = truck["vehicle_id"]
                if vid not in truck_states:
                    newly_added = auto_register_truck(vid, truck["vehicle_name"])
                    if newly_added:
                        log.info(f"New truck from Samsara: {truck['vehicle_name']} ({vid}) — auto-registered. Add group ID in DB when ready.")
                    process_truck(vid, {}, truck, truck_states)

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