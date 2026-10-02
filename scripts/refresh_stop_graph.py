"""Refresh Valhalla-snapped fuel stops and the nearby truck-stop graph.

Run after Valhalla tiles are installed and `VALHALLA_CONFIG` points at the
runtime config:

    python -m scripts.refresh_stop_graph
"""
from __future__ import annotations

import asyncio
import sys

from dieselup.core.stop_graph import format_refresh_result, refresh_stop_graph
from dieselup.db import close_pool


async def _main() -> int:
    try:
        result = await refresh_stop_graph()
        print(format_refresh_result(result))
        return 1 if result.aborted_reason else 0
    finally:
        await close_pool()


if __name__ == "__main__":
    raise SystemExit(asyncio.run(_main()))
