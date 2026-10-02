"""
One-time seed of the fuel_stops table from the locations XLSX.
Run this once during initial deploy:
    python -m scripts.seed_locations /path/to/locations.xlsx

Idempotent: re-running updates existing rows by city+state+address but
never creates duplicates.
"""
import sys
import asyncio
import pandas as pd
from dieselup.db import get_pool
from dieselup.config import settings


async def seed(file_path: str) -> None:
    df = pd.read_excel(file_path, sheet_name=0, engine="openpyxl")

    required_cols = {"station_name", "address", "city", "state", "latitude", "longitude"}
    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(f"Locations file missing columns: {missing}")

    df = df[df["latitude"].notna() & df["longitude"].notna()].copy()

    if not (df["latitude"].between(25, 49).all() and df["longitude"].between(-125, -67).all()):
        raise ValueError("Coordinates outside CONUS bounds — check the file")

    pool = await get_pool()
    async with pool.acquire() as conn:
        for _, row in df.iterrows():
            await conn.execute(
                """
                INSERT INTO fuel_stops
                    (station_name, address, city, state, latitude, longitude)
                VALUES ($1, $2, $3, $4, $5, $6)
                ON CONFLICT (city, state, address) DO UPDATE SET
                    station_name = EXCLUDED.station_name,
                    latitude = EXCLUDED.latitude,
                    longitude = EXCLUDED.longitude,
                    updated_at = NOW()
                """,
                str(row["station_name"]).strip(),
                str(row["address"]).strip() if pd.notna(row["address"]) else None,
                str(row["city"]).strip(),
                str(row["state"]).strip().upper(),
                float(row["latitude"]),
                float(row["longitude"]),
            )
    print(f"Seeded {len(df)} fuel stops.")


if __name__ == "__main__":
    asyncio.run(seed(sys.argv[1]))
