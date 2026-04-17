import io
import logging
import zipfile
import pandas as pd
from datetime import datetime, timezone
from database import upsert_fuel_stop, bulk_upsert_fuel_stops, get_stops_count

log = logging.getLogger(__name__)

_US_STATES = {
    "AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA","HI","ID","IL","IN","IA",
    "KS","KY","LA","ME","MD","MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ",
    "NM","NY","NC","ND","OH","OK","OR","PA","RI","SC","SD","TN","TX","UT","VT",
    "VA","WA","WV","WI","WY","DC",
}
_PILOT_BRANDS = {"Pilot Travel Center", "Flying J Travel Center"}
_LOVES_COLS = {
    "store_id":"StoreNumber","address":"Address","city":"City","state":"State",
    "zip":"Zip","latitude":"Latitude","longitude":"Longitude","phone":"Phone","diesel":"Diesel",
}


def _price(val) -> float | None:
    try:
        p = float(str(val).replace("$","").replace(",","").strip())
        return round(p,3) if 0.5 < p < 20.0 else None
    except (ValueError, TypeError):
        return None

def _coord(val) -> float | None:
    try:
        return float(str(val).strip())
    except (ValueError, TypeError):
        return None

def _extract(zip_bytes: bytes, ext: str) -> bytes:
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as z:
        for name in z.namelist():
            if name.lower().endswith(ext):
                return z.read(name)
    raise ValueError(f"No {ext} file in ZIP")


# ── MODE 1: merged_pilot_data.csv ─────────────────────────────────────────
# First-time upload — saves all locations + prices
# Columns: Store #, Name, Address, City, State, Zip Code,
#          Latitude, Longitude, Phone Number, Diesel

def _parse_pilot_merged(df: pd.DataFrame) -> list[dict]:
    df = df[df["Name"].str.strip().isin(_PILOT_BRANDS)].copy()
    df = df[df["State"].str.strip().str.upper().isin(_US_STATES)].copy()
    log.info(f"Pilot merged: {len(df)} US Pilot/Flying J stops after filter")

    now = datetime.now(timezone.utc)
    records = []
    for _, row in df.iterrows():
        lat = _coord(row.get("Latitude"))
        lng = _coord(row.get("Longitude"))
        if lat is None or lng is None:
            continue
        diesel = _price(row.get("Diesel"))
        records.append({
            "source":        "pilot",
            "store_id":      str(row.get("Store #") or "").strip(),
            "store_name":    str(row.get("Name") or "Pilot Travel Center").strip(),
            "brand":         str(row.get("Name") or "Pilot Travel Center").strip(),
            "address":       str(row.get("Address") or "").strip(),
            "city":          str(row.get("City") or "").strip(),
            "state":         str(row.get("State") or "").strip().upper(),
            "zip":           str(row.get("Zip Code") or "").strip(),
            "latitude":      lat,
            "longitude":     lng,
            "phone":         str(row.get("Phone Number") or "").strip(),
            "diesel_price":  diesel,
            "price_updated": now,
            "has_diesel":    diesel is not None,
        })

    log.info(f"Pilot merged: {len(records)} stops parsed")
    for r in records[:3]:
        log.info(f"  {r['store_name']} | {r['address']}, {r['city']}, {r['state']} {r['zip']} | ${r['diesel_price']}")
    return records


# ── MODE 2: Fuel_Prices.csv ────────────────────────────────────────────────
# Daily update — updates ONLY diesel_price, never touches address/city/lat/lng

def _upsert_price_only(records: list[dict]) -> int:
    from database import db_cursor
    updated = 0
    with db_cursor() as cur:
        for r in records:
            if not r.get("store_id") or r.get("diesel_price") is None:
                continue
            cur.execute("""
                UPDATE fuel_stops
                SET diesel_price = %s, price_updated = %s
                WHERE source = 'pilot' AND store_id = %s
            """, (r["diesel_price"], r["price_updated"], r["store_id"]))
            updated += cur.rowcount
    log.info(f"Pilot price-only: {updated} stops updated")
    return updated

def _parse_pilot_prices_only(df: pd.DataFrame) -> list[dict]:
    _CANADA = {"AB","BC","MB","ON","SK","QC","NB","NS","NL","PE","YT","NT","NU"}
    now = datetime.now(timezone.utc)
    records = []
    skipped = 0
    for _, row in df.iterrows():
        store_id = str(row.get("Pilot Travel Center") or "").strip()
        if not store_id:
            continue
        state_raw = str(row.get("State/Province") or "").strip().upper()
        if state_raw in _CANADA:
            skipped += 1
            continue
        diesel = _price(row.get("Diesel"))
        records.append({
            "source":        "pilot",
            "store_id":      store_id,
            "diesel_price":  diesel,
            "price_updated": now,
        })
    log.info(f"Pilot prices-only: {len(records)} stores ({skipped} Canada skipped)")
    return records


# ── Love's parser ──────────────────────────────────────────────────────────

def _parse_loves(xlsx_bytes: bytes) -> list[dict]:
    df_raw = pd.read_excel(io.BytesIO(xlsx_bytes), header=None, dtype=str)
    df = df_raw.iloc[3:].copy()
    df.columns = df_raw.iloc[2].tolist()
    df = df.reset_index(drop=True)
    before = len(df)
    df = df[df["StoreType"].str.strip() == "Travel Stop"].reset_index(drop=True)
    log.info(f"Love's: {before} → {len(df)} Travel Stops")
    c = _LOVES_COLS
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
    log.info(f"Love's: {len(records)} stops — {with_price} with price")
    return records


# ── Main entry point ───────────────────────────────────────────────────────

def update_from_file(file_bytes: bytes, filename: str) -> tuple[int, str]:
    """Route file to correct parser based on filename/content."""
    fname = filename.lower()

    # EFS fuel card format — has chain.name, fuelPrices, discountedPrice columns
    if fname.endswith('.csv'):
        try:
            sample = file_bytes[:500].decode('utf-8-sig', errors='ignore')
            if 'chain.name' in sample or 'discountedPrice' in sample or 'efsLocationId' in sample:
                from efs_importer import import_efs_stations
                return import_efs_stations(file_bytes, filename)
        except Exception:
            pass

    # Fall through to original parser
    """
    Handle Telegram file uploads.

    merged_pilot_data.csv  → First-time: saves ALL locations + prices (614 stops)
    Fuel_Prices.csv        → Daily: updates ONLY diesel prices, keeps addresses
    *.xlsx                 → Love's: saves locations + prices
    *.zip                  → ZIP containing any of the above
    """
    fname = filename.lower().strip()

    try:
        # Unzip
        if fname.endswith(".zip"):
            with zipfile.ZipFile(io.BytesIO(file_bytes)) as z:
                names = z.namelist()
            if any(n.lower().endswith(".csv") for n in names):
                inner = next(n for n in names if n.lower().endswith(".csv"))
                file_bytes = _extract(file_bytes, ".csv")
                fname = inner.lower()
            elif any(n.lower().endswith(".xlsx") for n in names):
                file_bytes = _extract(file_bytes, ".xlsx")
                fname = "loves_prices.xlsx"
            else:
                return 0, "❌ ZIP has no CSV or XLSX file."

        # ── CSV files ──────────────────────────────────────────────────────
        if fname.endswith(".csv"):
            df = pd.read_csv(io.BytesIO(file_bytes), dtype=str).fillna("")
            df.columns = [c.strip() for c in df.columns]

            # Detect merged file by checking for Address + City + Latitude columns
            is_merged = ("Address" in df.columns and "City" in df.columns
                         and "Latitude" in df.columns and "Store #" in df.columns)

            if is_merged:
                # MODE 1 — full location + price save
                records = _parse_pilot_merged(df)
                if not records:
                    return 0, "❌ No valid Pilot/Flying J stops found."
                for r in records:
                    upsert_fuel_stop(r)
                with_price    = sum(1 for r in records if r.get("diesel_price"))
                avg           = round(sum(r["diesel_price"] for r in records if r.get("diesel_price")) / with_price, 3) if with_price else 0
                pilot_count   = sum(1 for r in records if "Pilot" in r["store_name"])
                flyingj_count = sum(1 for r in records if "Flying J" in r["store_name"])
                return len(records), (
                    f"✅ *Pilot/Flying J locations saved*\n"
                    f"🛣 {pilot_count} Pilot Travel Centers\n"
                    f"🛣 {flyingj_count} Flying J Travel Centers\n"
                    f"⛽ {with_price} with diesel price\n"
                    f"💲 Avg diesel: ${avg:.3f}/gal\n\n"
                    f"From now on just upload `Fuel_Prices.csv` to update prices."
                )
            else:
                # MODE 2 — price update only
                if "Pilot Travel Center" not in df.columns:
                    return 0, (
                        "❌ Unrecognized CSV format.\n"
                        "Send `merged_pilot_data.csv` (first time) or `Fuel_Prices.csv` (price update)."
                    )
                records = _parse_pilot_prices_only(df)
                if not records:
                    return 0, "❌ No valid prices found in Fuel_Prices.csv."
                updated    = _upsert_price_only(records)
                with_price = sum(1 for r in records if r.get("diesel_price"))
                avg        = round(sum(r["diesel_price"] for r in records if r.get("diesel_price")) / with_price, 3) if with_price else 0
                return updated, (
                    f"✅ *Pilot prices updated*\n"
                    f"⛽ {updated} stops updated\n"
                    f"💲 Avg diesel: ${avg:.3f}/gal\n"
                    f"📍 Addresses unchanged"
                )

        # ── XLSX — Love's ──────────────────────────────────────────────────
        elif fname.endswith(".xlsx"):
            records = _parse_loves(file_bytes)
            if not records:
                return 0, "❌ No valid Love's stops found."
            for r in records:
                upsert_fuel_stop(r)
            with_price = sum(1 for r in records if r.get("diesel_price"))
            avg        = round(sum(r["diesel_price"] for r in records if r.get("diesel_price")) / with_price, 3) if with_price else 0
            return len(records), (
                f"✅ *Love's prices updated*\n"
                f"📍 {len(records)} stops\n"
                f"⛽ {with_price} with diesel price\n"
                f"💲 Avg diesel: ${avg:.3f}/gal"
            )

        else:
            return 0, (
                f"❌ Unsupported file: `{filename}`\n"
                f"Send `merged_pilot_data.csv`, `Fuel_Prices.csv`, or Love's `.xlsx`."
            )

    except Exception as e:
        log.error(f"update_from_file failed: {e}", exc_info=True)
        return 0, f"❌ Failed to parse `{filename}`: {e}"
