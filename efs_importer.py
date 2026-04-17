"""
efs_importer.py — Import EFS fuel card discount prices into DieselUp DB

Handles the client's EFS fuel card price file format:
- Pilot/J, Love's, TA/Petro stations
- Uses discountedPrice (negotiated card rate) not retailPrice
- Updates existing stations or inserts new ones
"""

import csv
import ast
import logging
from database import db_cursor

log = logging.getLogger(__name__)

CHAIN_MAP = {
    "PILOT/J":  "pilot",
    "LOVES":    "loves",
    "TA/PETRO": "ta_petro",
}


def import_efs_stations(file_bytes: bytes, filename: str) -> tuple[int, str]:
    """
    Parse EFS fuel station CSV and upsert into fuel_stops DB.
    Uses discountedPrice as diesel_price (client's negotiated rate).
    Returns (count_updated, message)
    """
    import io
    try:
        text = file_bytes.decode("utf-8-sig")
        reader = csv.DictReader(io.StringIO(text))
        rows = list(reader)
    except Exception as e:
        return 0, f"❌ Could not parse file: `{e}`"

    inserted = 0
    updated  = 0
    skipped  = 0

    with db_cursor() as cur:
        for row in rows:
            try:
                # Parse nested JSON fields
                prices_raw = row.get("fuelPrices", "")
                addrs_raw  = row.get("addresses", "")

                if not prices_raw or not addrs_raw:
                    skipped += 1
                    continue

                prices = ast.literal_eval(prices_raw)
                addrs  = ast.literal_eval(addrs_raw)

                if not prices or not addrs:
                    skipped += 1
                    continue

                p = prices[0]
                a = addrs[0]

                # Use discountedPrice — client's negotiated EFS card rate
                discounted = p.get("discountedPrice")
                best       = p.get("bestPrice")
                retail     = p.get("retailPrice")
                discount   = p.get("discountPerUnit", 0)

                # Use best price if available, otherwise discounted
                diesel_price = best if best else discounted if discounted else retail
                if not diesel_price:
                    skipped += 1
                    continue

                lat  = a.get("latitude")
                lng  = a.get("longitude")
                if not lat or not lng:
                    skipped += 1
                    continue

                chain_raw  = row.get("chain.name", "")
                source     = CHAIN_MAP.get(chain_raw, chain_raw.lower().replace("/", "_"))
                store_name = row.get("nameInEfs", row.get("nameInFile", ""))
                street     = a.get("street", "")
                city       = a.get("city", "")
                state      = a.get("state", "")
                zip_code   = a.get("zip", "")
                station_id = str(row.get("id", ""))

                cur.execute("""
                    INSERT INTO fuel_stops (
                        external_id, source, store_name,
                        address, city, state, zip,
                        latitude, longitude,
                        diesel_price, retail_price, discount_per_gallon,
                        has_diesel, price_updated
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s,
                        TRUE, NOW()
                    )
                    ON CONFLICT (external_id, source) DO UPDATE SET
                        store_name         = EXCLUDED.store_name,
                        address            = EXCLUDED.address,
                        city               = EXCLUDED.city,
                        state              = EXCLUDED.state,
                        zip                = EXCLUDED.zip,
                        latitude           = EXCLUDED.latitude,
                        longitude          = EXCLUDED.longitude,
                        diesel_price       = EXCLUDED.diesel_price,
                        retail_price       = EXCLUDED.retail_price,
                        discount_per_gallon = EXCLUDED.discount_per_gallon,
                        price_updated      = NOW()
                    RETURNING (xmax = 0) AS is_insert
                """, (
                    station_id, source, store_name,
                    street, city, state, zip_code,
                    float(lat), float(lng),
                    float(diesel_price), float(retail) if retail else None,
                    float(discount) if discount else None,
                ))
                result = cur.fetchone()
                if result and result[0]:
                    inserted += 1
                else:
                    updated += 1

            except Exception as e:
                log.warning(f"EFS row skip: {e}")
                skipped += 1

    total = inserted + updated
    msg = (
        f"✅ *EFS Prices Updated*\n"
        f"📍 Stations: *{total}* ({inserted} new, {updated} updated)\n"
        f"⏭ Skipped: {skipped}\n"
        f"⛽ Using negotiated EFS card prices (best/discounted rate)"
    )
    log.info(f"EFS import: {inserted} inserted, {updated} updated, {skipped} skipped")
    return total, msg
