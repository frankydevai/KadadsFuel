"""
Pilot/Flying J daily price-quote XLS parser.

Uses `xlrd` (1.2.x) directly because pandas 2.x+ rejects xlrd <2.0 and
xlrd >=2.0 no longer reads legacy BIFF .xls files — Pilot's daily quote is
BIFF, so we read the workbook row-by-row without pandas in the loop.

Public surface: parse_pilot_xls(file_path) -> dict
  * account_number  — derived from the row-4 "Account: <num> - <name>" cell
  * stops[]         — one dict per priced DSL row, schema below
  * parsed_at       — UTC datetime when parsing finished
  * row_count       — len(stops)

Each stop dict mirrors the contract the Telegram admin handler upserts into
`contracted_prices` (and the columns it doesn't write are kept for future
use): site_id, city, state, rack_id, rack_city, rack_state, cost,
total_cost, retail_price, your_price, savings_vs_retail.

Raises ValueError (never returns partial data) when:
  * Row 4 doesn't carry "Account:"
  * Required columns are missing from row 5
  * Fewer than 100 valid priced DSL rows survive
  * Any Your Price < $2.00 or > $8.00

Blank Your Price cells mean Pilot did not provide a contract price for that
DSL row, so those rows are skipped instead of aborting the whole import.
"""
from __future__ import annotations

from datetime import datetime, timezone
import re
from typing import Any

import xlrd
from openpyxl import load_workbook


REQUIRED_COLUMNS = (
    "Site",
    "City",
    "ST",
    "Prod",
    "Rack ID",
    "Rack City",
    "Rack ST",
    "Cost",
    "Total Cost",
    "Retail Price",
    "Your Price",
    "Savings Total",
)


def parse_pilot_xls(file_path: str) -> dict[str, Any]:
    """Parse a Pilot/Flying J daily price-quote XLS. See module docstring."""
    sheet = _open_first_sheet(file_path)
    if sheet.nrows < 7:
        raise ValueError(f"Sheet only has {sheet.nrows} rows — file looks empty")

    accounts = set()
    for row in range(min(20,sheet.nrows)):
        for column in range(sheet.ncols):
            match=re.search(r'\bAccount\s*:\s*(\d+)\b',str(sheet.cell_value(row,column)),re.I)
            if match:accounts.add(match[1])
    if len(accounts)!=1:
        raise ValueError('A single numeric Account header is required near the top of the price sheet')
    account_number=next(iter(accounts))
    header_row=None
    for row in range(min(40,sheet.nrows)):
        header=[str(sheet.cell_value(row,c)).strip() for c in range(sheet.ncols)]
        if all(name in header for name in REQUIRED_COLUMNS):
            header_row=row
            break
    if header_row is None:
        raise ValueError('Required price columns are missing from the sheet header')
    col_index: dict[str, int] = {}
    for name in REQUIRED_COLUMNS:
        try:
            col_index[name] = header.index(name)
        except ValueError as exc:
            raise ValueError(f"Required column missing from header: {name!r}") from exc

    stops: list[dict[str, Any]] = []
    for r in range(header_row+1, sheet.nrows):
        site_raw = sheet.cell_value(r, col_index["Site"])
        if not _is_real_value(site_raw):
            continue
        prod = str(sheet.cell_value(r, col_index["Prod"])).strip().upper()
        if prod != "DSL":
            continue

        your_price_raw = sheet.cell_value(r, col_index["Your Price"])
        if not _is_real_value(your_price_raw):
            continue
        try:
            your_price = _as_float(your_price_raw)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"Row {r + 1}: invalid Your Price ({exc})") from exc

        try:
            stops.append({
                "site_id": _as_int(site_raw),
                "city": _as_str(sheet.cell_value(r, col_index["City"])),
                "state": _as_state(sheet.cell_value(r, col_index["ST"])),
                "rack_id": _as_optional_int(sheet.cell_value(r, col_index["Rack ID"])),
                "rack_city": _as_optional_str(sheet.cell_value(r, col_index["Rack City"])),
                "rack_state": _as_optional_state(sheet.cell_value(r, col_index["Rack ST"])),
                "cost": _as_optional_float(sheet.cell_value(r, col_index["Cost"])),
                "total_cost": _as_optional_float(sheet.cell_value(r, col_index["Total Cost"])),
                "retail_price": _as_optional_float(sheet.cell_value(r, col_index["Retail Price"])),
                "your_price": your_price,
                "savings_vs_retail": _as_optional_float(
                    sheet.cell_value(r, col_index["Savings Total"])
                ),
            })
        except (TypeError, ValueError) as exc:
            raise ValueError(f"Row {r + 1} failed to parse: {exc}") from exc

    if len(stops) < 100:
        raise ValueError(f"Only {len(stops)} rows parsed — file may be corrupt")

    prices = [s["your_price"] for s in stops]
    min_price, max_price = min(prices), max(prices)
    if min_price < 2.00 or max_price > 8.00:
        raise ValueError(
            f"Price out of sanity range: ${min_price:.2f} to ${max_price:.2f}"
        )

    return {
        "account_number": account_number,
        "stops": stops,
        "parsed_at": datetime.now(timezone.utc),
        "row_count": len(stops),
    }


class _OpenpyxlSheet:
    """Small xlrd-compatible adapter for modern XLSX workbooks."""

    def __init__(self, worksheet: Any) -> None:
        # Some valid exports omit worksheet dimensions. Read rows once rather
        # than trusting max_row/max_column or rescanning the XML for every cell.
        self._rows = list(worksheet.iter_rows(values_only=True))
        self.nrows = len(self._rows)
        self.ncols = max((len(row) for row in self._rows), default=0)

    def cell_value(self, row: int, column: int) -> Any:
        values = self._rows[row]
        value = values[column] if column < len(values) else None
        return "" if value is None else value


def _open_first_sheet(file_path: str) -> Any:
    """Open legacy XLS with xlrd and XLSX with openpyxl.

    Telegram filenames are not trusted to describe the actual workbook type,
    so an xlrd format rejection falls back to openpyxl before reporting a
    parse failure.
    """
    if file_path.lower().endswith(".xlsx"):
        try:
            workbook = load_workbook(file_path, read_only=True, data_only=True)
            try:
                return _OpenpyxlSheet(workbook.worksheets[0])
            finally:
                workbook.close()
        except Exception as exc:  # noqa: BLE001
            raise ValueError(f"Could not open XLSX workbook: {exc}") from exc
    try:
        book = xlrd.open_workbook(file_path)
        return book.sheet_by_index(0)
    except xlrd.XLRDError as xls_exc:
        try:
            workbook = load_workbook(file_path, read_only=True, data_only=True)
            try:
                return _OpenpyxlSheet(workbook.worksheets[0])
            finally:
                workbook.close()
        except Exception as xlsx_exc:  # noqa: BLE001
            raise ValueError(
                f"Workbook is neither a readable XLS nor XLSX file: {xls_exc}; {xlsx_exc}"
            ) from xlsx_exc
    except Exception as exc:  # noqa: BLE001 — surface every parse failure to admin
        raise ValueError(f"Could not open workbook: {exc}") from exc


def _is_real_value(v: Any) -> bool:
    if v is None:
        return False
    if isinstance(v, str):
        return bool(v.strip()) and v.strip() != "\n"
    if isinstance(v, float):
        # xlrd returns 0.0 for empty numeric cells in some workbooks.
        return v != 0.0 or True  # 0 is a legitimate Site only in junk rows; we keep it
    return True


def _as_int(v: Any) -> int:
    if isinstance(v, (int, float)):
        return int(v)
    s = str(v).strip()
    return int(float(s))


def _as_optional_int(v: Any) -> int | None:
    if v is None or v == "" or (isinstance(v, float) and v == 0.0):
        return None
    try:
        return _as_int(v)
    except (TypeError, ValueError):
        return None


def _as_float(v: Any) -> float:
    if isinstance(v, (int, float)):
        return float(v)
    return float(str(v).strip())


def _as_optional_float(v: Any) -> float | None:
    """Empty cells and blank strings -> None, matching pandas NaN semantics."""
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v).strip()
    if not s:
        return None
    try:
        return float(s)
    except ValueError:
        return None


def _as_str(v: Any) -> str:
    return str(v).strip()


def _as_optional_str(v: Any) -> str | None:
    if v is None:
        return None
    s = str(v).strip()
    return s or None


def _as_state(v: Any) -> str:
    return str(v).strip().upper()


def _as_optional_state(v: Any) -> str | None:
    if v is None:
        return None
    s = str(v).strip().upper()
    return s or None
