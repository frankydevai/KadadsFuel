"""
price_updater.py  -  Auto-download and parse fuel prices from Pilot and Love's.

Runs daily via scheduler. Also supports manual Telegram upload as fallback.

Hash-based change detection — only updates DB if files actually changed.
"""

"""
price_updater.py  -  Auto-download and parse fuel prices from Pilot and Love's.

Runs daily via scheduler. Also supports manual Telegram upload as fallback.

Hash-based change detection — only updates DB if files actually changed.
"""

import io
import os
import hashlib
import zipfile
import logging
import requests
import pandas as pd
from datetime import datetime, timezone
from database import upsert_fuel_stop, bulk_upsert_fuel_stops, get_stops_count
from config import PILOT_ZIP_URL, LOVES_ZIP_URL

log = logging.getLogger(__name__)

# Hash cache files (stored in /tmp — only needed for change detection within session)
_HASH_DIR = os.getenv("HASH_DIR", "/tmp")

# ── Column mappings ──────────────────────────────────────────────────────────
# UPDATE THESE to match the actual column names in your files.
# Run price_updater.py once with dry_run=True to see what columns exist.

PILOT_COLUMNS = {
    # Fuel_Prices.csv columns
    "store_id":   "Pilot Travel Center",   # store number (col 0)
    "city":       "City",
    "state":      "State/Province",
    "diesel":     "Diesel",

    # all_locations.csv columns
    "loc_store_id": "Store #",
    "store_name":   "Name",
    "address":      "Address",
    "loc_city":     "City",
    "loc_state":    "State",
    "zip":          "Zip Code",
    "latitude":     "Latitude",
    "longitude":    "Longitude",
    "phone":        "Phone Number",
}

LOVES_COLUMNS = {
    "store_id":    "StoreNumber",
    "store_name":  "StoreType",       # e.g. "Travel Stop"
    "address":     "Address",
    "city":        "City",
    "state":       "State",
    "zip":         "Zip",
    "latitude":    "Latitude",
    "longitude":   "Longitude",
    "phone":       "Phone",
    "diesel":      "Diesel",
}


# -- Helpers ------------------------------------------------------------------

def _download(url: str) -> bytes:
    log.info(f"Downloading: {url}")
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    log.info(f"Downloaded {len(resp.content):,} bytes")
    return resp.content


def _extract(zip_bytes: bytes, ext: str) -> bytes:
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as z:
        names = z.namelist()
        log.info(f"ZIP contains: {names}")
        match = next((n for n in names if n.lower().endswith(ext)), None)
        if not match:
            raise FileNotFoundError(f"No {ext} file in ZIP. Files: {names}")
        log.info(f"Extracting: {match}")
        return z.read(match)


def _md5(data: bytes) -> str:
    return hashlib.md5(data).hexdigest()


def _load_hash(source: str) -> str:
    path = os.path.join(_HASH_DIR, f".fuel_hash_{source}.txt")
    try:
        with open(path) as f:
            return f.read().strip()
    except FileNotFoundError:
        return ""


def _save_hash(source: str, h: str):
    path = os.path.join(_HASH_DIR, f".fuel_hash_{source}.txt")
    with open(path, "w") as f:
        f.write(h)


# Pilot locations cache — stored in DB so it survives Railway redeploys
_PILOT_LOCATIONS_KEY = "pilot_locations_csv"

def _save_pilot_locations(csv_bytes: bytes):
    """Save all_locations.csv bytes to DB as a blob (base64 encoded)."""
    import base64
    from database import set_config_value
    encoded = base64.b64encode(csv_bytes).decode("utf-8")
    set_config_value(_PILOT_LOCATIONS_KEY, encoded)
    log.info(f"Pilot locations cached in DB ({len(csv_bytes):,} bytes)")

def _load_pilot_locations() -> bytes | None:
    """Load cached all_locations.csv bytes from DB. Returns None if not cached."""
    import base64
    from database import get_config_value
    encoded = get_config_value(_PILOT_LOCATIONS_KEY)
    if not encoded:
        return None
    return base64.b64decode(encoded.encode("utf-8"))


def _price(val) -> float | None:
    try:
        p = float(str(val).replace("$", "").replace(",", "").strip())
        return round(p, 3) if 0.5 < p < 20.0 else None
    except (ValueError, TypeError):
        return None


def _coord(val) -> float | None:
    try:
        return float(str(val).strip())
    except (ValueError, TypeError):
        return None


# -- Parsers ------------------------------------------------------------------

def _parse_pilot(csv_bytes: bytes, locations_bytes: bytes | None = None) -> list[dict]:
    """
    Parse Pilot fuel prices.

    Fuel_Prices.csv  — store#, city, state, diesel price (no coordinates)
    all_locations.csv — store#, name, address, lat, lng, phone, zip

    If locations_bytes provided: merge on store number to get coordinates.
    If not provided: fall back to prices-only (no lat/lng — stops skipped).
    """
    prices_df = pd.read_csv(io.BytesIO(csv_bytes), dtype=str)
    log.info(f"Pilot prices columns: {list(prices_df.columns)}")
    c = PILOT_COLUMNS

    if locations_bytes is not None:
        locs_df = pd.read_csv(io.BytesIO(locations_bytes), dtype=str)
        log.info(f"Pilot locations columns: {list(locs_df.columns)}")

        # Filter: only Pilot Travel Center and Flying J Travel Center
        before = len(locs_df)
        locs_df = locs_df[locs_df[c["store_name"]].str.strip().isin([
            "Pilot Travel Center",
            "Flying J Travel Center",
        ])].reset_index(drop=True)
        log.info(f"Pilot locations: {before} total → {len(locs_df)} kept (Pilot + Flying J only)")

        df = pd.merge(
            prices_df,
            locs_df,
            left_on=c["store_id"],
            right_on=c["loc_store_id"],
            how="inner",
        )
        log.info(f"Pilot: {len(prices_df)} prices + {len(locs_df)} locations → {len(df)} merged")
    else:
        log.warning("Pilot: no locations file — cannot determine coordinates, skipping all stops.")
        return []

    now = datetime.now(timezone.utc)
    records = []
    for _, row in df.iterrows():
        lat = _coord(row.get(c["latitude"]))
        lng = _coord(row.get(c["longitude"]))
        if lat is None or lng is None:
            continue
        records.append({
            "source":        "pilot",
            "store_id":      str(row.get(c["store_id"], "")).strip(),
            "store_name":    str(row.get(c["store_name"], "")).strip() or "Pilot Travel Center",
            "brand":         str(row.get(c["store_name"], "Pilot Flying J")).strip(),
            "address":       str(row.get(c["address"], "")).strip(),
            "city":          str(row.get(c["loc_city"], "")).strip(),
            "state":         str(row.get(c["loc_state"], "")).strip().upper(),
            "zip":           str(row.get(c["zip"], "")).strip(),
            "latitude":      lat,
            "longitude":     lng,
            "phone":         str(row.get(c["phone"], "")).strip(),
            "diesel_price":  _price(row.get(c["diesel"])),
            "price_updated": now,
            "has_diesel":    True,
        })
    log.info(f"Pilot: parsed {len(records)} stops with coordinates")
    return records


def _parse_loves(xlsx_bytes: bytes) -> list[dict]:
    """
    Parse Love's XLSX file.
    - Only keeps StoreType == 'Travel Stop' (606 of 721 stores)
    - 111 of those may have no diesel price (stored with has_diesel=False)
    - Store name formatted as "Love's Travel Stop #448"
    """
    df_raw = pd.read_excel(io.BytesIO(xlsx_bytes), header=None, dtype=str)
    df = df_raw.iloc[3:].copy()
    df.columns = df_raw.iloc[2].tolist()
    df = df.reset_index(drop=True)

    # Filter: Travel Stop only
    before = len(df)
    df = df[df["StoreType"].str.strip() == "Travel Stop"].reset_index(drop=True)
    log.info(f"Love's: {before} total → {len(df)} Travel Stops kept (filtered {before - len(df)} Country Store / Car Stop / etc)")

    c = LOVES_COLUMNS
    now = datetime.now(timezone.utc)
    records = []
    for _, row in df.iterrows():
        lat = _coord(row.get(c["latitude"]))
        lng = _coord(row.get(c["longitude"]))
        if lat is None or lng is None:
            continue
        diesel    = _price(row.get(c["diesel"]))
        store_num = str(row.get(c["store_id"], "")).strip()
        records.append({
            "source":        "loves",
            "store_id":      store_num,
            "store_name":    f"Love's Travel Stop #{store_num}",
            "brand":         "Love's Travel Stops",
            "address":       str(row.get(c["address"], "")).strip(),
            "city":          str(row.get(c["city"], "")).strip(),
            "state":         str(row.get(c["state"], "")).strip().upper(),
            "zip":           str(row.get(c["zip"], "")).strip(),
            "latitude":      lat,
            "longitude":     lng,
            "phone":         str(row.get(c["phone"], "")).strip(),
            "diesel_price":  diesel,
            "price_updated": now,
            "has_diesel":    diesel is not None,
        })
    with_price = sum(1 for r in records if r["has_diesel"])
    log.info(f"Love's: {len(records)} stops — {with_price} with diesel price, {len(records) - with_price} without")
    return records


# -- Main updater functions ---------------------------------------------------

def update_pilot(force: bool = False) -> int:
    """Download Pilot ZIP, parse, upsert to DB. Returns count of records processed."""
    if not PILOT_ZIP_URL:
        log.warning("PILOT_ZIP_URL not set — skipping Pilot update.")
        return 0
    try:
        zip_bytes = _download(PILOT_ZIP_URL)
        h = _md5(zip_bytes)
        if not force and h == _load_hash("pilot"):
            log.info("Pilot: no change detected.")
            return 0
        csv_bytes = _extract(zip_bytes, ".csv")
        records   = _parse_pilot(csv_bytes)
        bulk_upsert_fuel_stops(records)
        _save_hash("pilot", h)
        log.info(f"Pilot: updated {len(records)} stops in DB.")
        return len(records)
    except Exception as e:
        log.error(f"Pilot update failed: {e}", exc_info=True)
        return 0


def update_loves(force: bool = False) -> int:
    """Download Love's ZIP, parse, upsert to DB. Returns count of records processed."""
    if not LOVES_ZIP_URL:
        log.warning("LOVES_ZIP_URL not set — skipping Love's update.")
        return 0
    try:
        zip_bytes = _download(LOVES_ZIP_URL)
        h = _md5(zip_bytes)
        if not force and h == _load_hash("loves"):
            log.info("Love's: no change detected.")
            return 0
        xlsx_bytes = _extract(zip_bytes, ".xlsx")
        records    = _parse_loves(xlsx_bytes)
        bulk_upsert_fuel_stops(records)
        _save_hash("loves", h)
        log.info(f"Love's: updated {len(records)} stops in DB.")
        return len(records)
    except Exception as e:
        log.error(f"Love's update failed: {e}", exc_info=True)
        return 0


def run_price_update(force: bool = False) -> tuple[int, int]:
    """Run both updates. Returns (pilot_count, loves_count)."""
    log.info("=" * 40)
    log.info("Running fuel price update...")
    pilot = update_pilot(force=force)
    loves = update_loves(force=force)
    total = get_stops_count()
    log.info(f"Price update complete — {total} total diesel stops in DB.")
    log.info("=" * 40)
    return pilot, loves


def update_from_file(file_bytes: bytes, filename: str) -> tuple[int, str]:
    """
    Manual upload via Telegram.

    Supported uploads:
      Fuel_Prices.csv      → Pilot prices (merged with cached all_locations.csv)
      all_locations.csv    → Pilot locations cache (saved for future price merges)
      loves_prices.xlsx    → Love's prices + locations
      *.zip                → ZIP containing either of the above

    Returns (count, message).
    """
    fname = filename.lower()

    try:
        # -- Unzip if needed --------------------------------------------------
        if fname.endswith(".zip"):
            with zipfile.ZipFile(io.BytesIO(file_bytes)) as z:
                names = z.namelist()
            has_csv  = any(n.lower().endswith(".csv")  for n in names)
            has_xlsx = any(n.lower().endswith(".xlsx") for n in names)
            if has_csv:
                file_bytes = _extract(file_bytes, ".csv")
                fname = "fuel_prices.csv"
            elif has_xlsx:
                file_bytes = _extract(file_bytes, ".xlsx")
                fname = "loves_prices.xlsx"
            else:
                return 0, "ZIP file doesn't contain CSV or XLSX."

        # -- Pilot locations cache (all_locations.csv) -------------------------
        if "all_locations" in fname:
            _save_pilot_locations(file_bytes)
            df = pd.read_csv(io.BytesIO(file_bytes), dtype=str)
            pilot_count = (df['Name'].str.strip() == 'Pilot Travel Center').sum()
            flyingj_count = (df['Name'].str.strip() == 'Flying J Travel Center').sum()
            return len(df), (
                f"✅ *Pilot locations cached*\n"
                f"📍 {pilot_count} Pilot Travel Centers\n"
                f"📍 {flyingj_count} Flying J Travel Centers\n"
                f"Now send `Fuel_Prices.csv` to update prices."
            )

        # -- Pilot prices (Fuel_Prices.csv) ------------------------------------
        elif fname.endswith(".csv"):
            locs = _load_pilot_locations()
            if locs is None:
                return 0, (
                    "❌ No locations file cached yet.\n"
                    "Please send `all_locations.csv` first, then send `Fuel_Prices.csv`."
                )
            records = _parse_pilot(file_bytes, locs)
            for r in records:
                upsert_fuel_stop(r)
            with_price = sum(1 for r in records if r.get("diesel_price"))
            avg_price  = round(sum(r["diesel_price"] for r in records if r.get("diesel_price")) / with_price, 3) if with_price else 0
            msg = (
                f"✅ *Pilot prices updated*\n"
                f"📍 {len(records)} stops loaded\n"
                f"⛽ {with_price} with diesel price\n"
                f"💲 Avg diesel: ${avg_price:.3f}/gal"
            )
            return len(records), msg

        # -- Love's (XLSX) -----------------------------------------------------
        elif fname.endswith(".xlsx"):
            records = _parse_loves(file_bytes)
            for r in records:
                upsert_fuel_stop(r)
            with_price = sum(1 for r in records if r.get("diesel_price"))
            avg_price  = round(sum(r["diesel_price"] for r in records if r.get("diesel_price")) / with_price, 3) if with_price else 0
            msg = (
                f"✅ *Love's prices updated*\n"
                f"📍 {len(records)} stops loaded\n"
                f"⛽ {with_price} with diesel price\n"
                f"💲 Avg diesel: ${avg_price:.3f}/gal"
            )
            return len(records), msg

        else:
            return 0, f"Unsupported file type: {filename}"

    except Exception as e:
        log.error(f"Manual upload failed: {e}", exc_info=True)
        return 0, f"❌ Failed to parse file: {e}"


if __name__ == "__main__":
    # Run manually: python price_updater.py
    import sys
    force = "--force" in sys.argv
    pilot, loves = run_price_update(force=force)
    print(f"Done — Pilot: {pilot}  Love's: {loves}")