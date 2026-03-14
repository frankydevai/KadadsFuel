import io
import os
import hashlib
import zipfile
import logging
import pandas as pd
from datetime import datetime, timezone
from database import upsert_fuel_stop, bulk_upsert_fuel_stops, get_stops_count

log = logging.getLogger(__name__)

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

# US states only — skip Canadian provinces
_US_STATES = {
    "AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA","HI","ID","IL","IN","IA",
    "KS","KY","LA","ME","MD","MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ",
    "NM","NY","NC","ND","OH","OK","OR","PA","RI","SC","SD","TN","TX","UT","VT",
    "VA","WA","WV","WI","WY","DC",
}

def _parse_pilot(csv_bytes: bytes, locations_bytes: bytes | None = None) -> list[dict]:
    """
    Parse Pilot/Flying J fuel prices by merging:
      Fuel_Prices.csv   — store#, city, state, diesel price
      all_locations.csv — store#, name, address, city, state, zip, lat, lng

    Merges on store number. City/address come from locations file (authoritative).
    Skips Canadian stops. Only keeps Pilot Travel Center + Flying J Travel Center.
    """
    if locations_bytes is None:
        log.warning("Pilot: no locations file — skipping all Pilot stops.")
        return []

    # Load both files
    prices_df = pd.read_csv(io.BytesIO(csv_bytes), dtype=str).fillna("")
    locs_df   = pd.read_csv(io.BytesIO(locations_bytes), dtype=str).fillna("")

    # Normalize column names — strip whitespace
    prices_df.columns = [c.strip() for c in prices_df.columns]
    locs_df.columns   = [c.strip() for c in locs_df.columns]

    log.info(f"Pilot prices: {len(prices_df)} rows, cols={list(prices_df.columns)}")
    log.info(f"Pilot locations: {len(locs_df)} rows, cols={list(locs_df.columns)}")

    # Filter locations: Pilot + Flying J only
    locs_df = locs_df[locs_df["Name"].str.strip().isin([
        "Pilot Travel Center", "Flying J Travel Center"
    ])].copy()
    log.info(f"Pilot locations after brand filter: {len(locs_df)}")

    # Filter locations: US only
    locs_df = locs_df[locs_df["State"].str.strip().str.upper().isin(_US_STATES)].copy()
    log.info(f"Pilot locations after US filter: {len(locs_df)}")

    # Normalize store number columns for merge
    prices_df["_store_num"] = prices_df["Pilot Travel Center"].str.strip()
    locs_df["_store_num"]   = locs_df["Store #"].str.strip()

    # Merge — locations are authoritative for address/city/state/zip
    df = pd.merge(prices_df, locs_df, on="_store_num", how="inner", suffixes=("_price", "_loc"))
    log.info(f"Pilot merged: {len(df)} stops")

    now = datetime.now(timezone.utc)
    records = []
    skipped_no_coords = 0
    skipped_canada    = 0

    for _, row in df.iterrows():
        lat = _coord(row.get("Latitude"))
        lng = _coord(row.get("Longitude"))
        if lat is None or lng is None:
            skipped_no_coords += 1
            continue

        # Use location file values (authoritative) — never price file city
        city    = str(row.get("City_loc") or row.get("City_price") or row.get("City") or "").strip()
        state   = str(row.get("State") or row.get("State/Province") or "").strip().upper()
        # Convert full state names to abbreviations if needed
        _STATE_MAP = {
            "TEXAS":"TX","CALIFORNIA":"CA","FLORIDA":"FL","OHIO":"OH","TENNESSEE":"TN",
            "GEORGIA":"GA","ILLINOIS":"IL","PENNSYLVANIA":"PA","NEW YORK":"NY","MICHIGAN":"MI",
            "NORTH CAROLINA":"NC","VIRGINIA":"VA","WASHINGTON":"WA","ARIZONA":"AZ","COLORADO":"CO",
            "INDIANA":"IN","KENTUCKY":"KY","OREGON":"OR","OKLAHOMA":"OK","NEVADA":"NV",
            "MISSOURI":"MO","ALABAMA":"AL","ARKANSAS":"AR","LOUISIANA":"LA","MINNESOTA":"MN",
            "MISSISSIPPI":"MS","IOWA":"IA","KANSAS":"KS","UTAH":"UT","NEBRASKA":"NE",
            "NEW MEXICO":"NM","SOUTH CAROLINA":"SC","WEST VIRGINIA":"WV","MONTANA":"MT",
            "IDAHO":"ID","NORTH DAKOTA":"ND","SOUTH DAKOTA":"SD","WYOMING":"WY",
            "WISCONSIN":"WI","NEW JERSEY":"NJ","MARYLAND":"MD","CONNECTICUT":"CT",
            "MASSACHUSETTS":"MA","NEW HAMPSHIRE":"NH","VERMONT":"VT","MAINE":"ME",
            "RHODE ISLAND":"RI","DELAWARE":"DE","ALASKA":"AK","HAWAII":"HI","DC":"DC",
        }
        if len(state) > 2:
            state = _STATE_MAP.get(state, state)
        address = str(row.get("Address") or "").strip()
        zip_    = str(row.get("Zip Code") or "").strip()
        name    = str(row.get("Name") or "Pilot Travel Center").strip()
        store_id = str(row.get("_store_num") or "").strip()

        # Skip Canadian stops
        if state not in _US_STATES:
            skipped_canada += 1
            continue

        diesel = _price(row.get("Diesel"))

        records.append({
            "source":        "pilot",
            "store_id":      store_id,
            "store_name":    name,
            "brand":         name,
            "address":       address,
            "city":          city,
            "state":         state,
            "zip":           zip_,
            "latitude":      lat,
            "longitude":     lng,
            "phone":         str(row.get("Phone Number") or "").strip(),
            "diesel_price":  diesel,
            "price_updated": now,
            "has_diesel":    diesel is not None,
        })

    log.info(
        f"Pilot: {len(records)} stops saved "
        f"({skipped_no_coords} no coords, {skipped_canada} Canada skipped)"
    )
    # Sample log to verify city is populated
    for r in records[:3]:
        log.info(f"  Sample: {r['store_name']} | {r['address']}, {r['city']}, {r['state']} {r['zip']} | ${r['diesel_price']}")
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
