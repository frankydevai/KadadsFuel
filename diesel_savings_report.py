"""
diesel_savings_report.py — Targeted Diesel Savings & Losses Report

Generates an Excel report for a specific list of trucks showing:
  - Every fuel alert fired
  - Whether driver followed the recommendation (savings) or skipped/fueled elsewhere (loss)
  - Per-truck and fleet-wide totals

Usage:
    DATABASE_URL=postgresql://... python diesel_savings_report.py
    DATABASE_URL=postgresql://... python diesel_savings_report.py --days 30
    DATABASE_URL=postgresql://... python diesel_savings_report.py --days 7 --out report.xlsx
"""

import os
import sys
import argparse
from datetime import datetime, timezone, timedelta
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter

# ── Target trucks (from fleet list) ───────────────────────────────────────────
# Format: (unit_number_as_stored_in_db, driver_name)
TARGET_TRUCKS = [
    ("702658",                    "MILTON MEDINA"),
    ("401000",                    "BRUNOSAIRE GABRIEL"),
    ("401999",                    "OMAR SALMAN"),
    ("541910",                    "RUKUNDO YVAN"),
    ("567667",                    "NZEYIMANA KAMALI"),
    ("9299",                      "RUBEN HAMPTON"),
    ("2477",                      "MINANI JEAN"),
    ("7764",                      "Ahmed Ben-Marzouk"),
    ("SUBUNIT# 727424 (551802)",  "JETSON ANDRE"),
    ("585979",                    "PIERRE ANDRE"),
    ("431666",                    "Rodny Louis Jacques"),
    ("9298",                      "GLORIA NIWEMUGENI"),
]

# ── Color palette ──────────────────────────────────────────────────────────────
BG_DARK   = "060608"
CARD      = "101014"
CARD2     = "1C1C24"
CARD3     = "141420"
GREEN     = "7FFF5F"
GREEN_DRK = "3A7A2A"
RED       = "FF4545"
AMBER     = "F59E0B"
BLUE      = "3B82F6"
WHITE     = "F8F8FF"
MUTED     = "505060"

def _fill(c):
    return PatternFill("solid", fgColor=c)

def _border():
    s = Side(style="thin", color="2A2A35")
    return Border(left=s, right=s, top=s, bottom=s)

def _align(h="left"):
    return Alignment(horizontal=h, vertical="center", wrap_text=False)

def _hdr(ws, r, c, v, w=None, color=WHITE, bg=CARD2, bold=True, size=10):
    cell = ws.cell(row=r, column=c, value=v)
    cell.font      = Font(name="Arial", bold=bold, color=color, size=size)
    cell.fill      = _fill(bg)
    cell.alignment = _align("center")
    cell.border    = _border()
    if w:
        ws.column_dimensions[get_column_letter(c)].width = w
    return cell

def _cell(ws, r, c, v, fmt=None, color=WHITE, bold=False, center=False, bg=None):
    cell = ws.cell(row=r, column=c, value=v)
    cell.font      = Font(name="Arial", bold=bold, color=color, size=10)
    cell.fill      = _fill(bg or (BG_DARK if r % 2 == 0 else CARD3))
    cell.alignment = _align("center" if center else "left")
    cell.border    = _border()
    if fmt:
        cell.number_format = fmt
    return cell

def _section(ws, row, text, ncols, color=GREEN):
    ws.merge_cells(start_row=row, start_column=1, end_row=row, end_column=ncols)
    c = ws.cell(row=row, column=1, value=text)
    c.font      = Font(name="Arial", bold=True, color=color, size=11)
    c.fill      = _fill(CARD2)
    c.alignment = _align("left")
    ws.row_dimensions[row].height = 22


# ── Database queries ───────────────────────────────────────────────────────────

def fetch_truck_data(since: datetime) -> dict:
    """
    Pull fuel alert history, stop visit outcomes, and driver flags
    for every target truck.

    Returns a dict keyed by vehicle_name with:
      alerts  — list of fuel_alerts rows
      visits  — list of stop_visits rows (joined with fuel_alerts)
      flags   — list of driver_flags rows
    """
    import psycopg2
    import psycopg2.extras

    db_url = os.environ.get("DATABASE_URL", "")
    if not db_url:
        print("ERROR: DATABASE_URL environment variable is not set.", file=sys.stderr)
        sys.exit(1)

    conn = psycopg2.connect(db_url, connect_timeout=15,
                            keepalives=1, keepalives_idle=30)
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    # Build IN list dynamically — we try both the bare unit number and common
    # name variants stored in the trucks table.
    target_names = [unit for unit, _ in TARGET_TRUCKS]
    placeholders = ",".join(["%s"] * len(target_names))

    # Resolve actual vehicle_names from trucks table (handles partial matches)
    cur.execute(f"""
        SELECT vehicle_name FROM trucks
        WHERE vehicle_name = ANY(%s::text[]) OR is_active = TRUE
        ORDER BY vehicle_name
    """, (target_names,))
    registered = {r["vehicle_name"] for r in cur.fetchall()}

    # Use target list directly — names may not be in trucks table if bot not running
    result = {}
    for unit, driver in TARGET_TRUCKS:
        # ── Stop visits (main savings/loss source) ────────────────────────────
        cur.execute("""
            SELECT
                sv.id,
                sv.visited_at,
                sv.recommended_stop_name,
                sv.recommended_stop_lat,
                sv.recommended_stop_lng,
                sv.actual_stop_name,
                sv.actual_stop_state,
                sv.visited,
                sv.fuel_before,
                sv.fuel_after,
                sv.gallons_purchased,
                sv.savings_usd,
                fa.fuel_pct,
                fa.best_stop_price,
                fa.alerted_at,
                fa.status   AS alert_status
            FROM stop_visits sv
            LEFT JOIN fuel_alerts fa ON sv.alert_id = fa.id
            WHERE sv.vehicle_name = %s
              AND sv.visited_at   >= %s
            ORDER BY sv.visited_at DESC
        """, (unit, since))
        visits = [dict(r) for r in cur.fetchall()]

        # ── Raw fuel alerts (catches alerts with no visit record yet) ─────────
        cur.execute("""
            SELECT
                id, vehicle_name, fuel_pct,
                best_stop_name, best_stop_price, best_stop_state,
                alt_stop_name,  alt_stop_price,
                gallons_purchased, savings_usd,
                alert_type, status,
                alerted_at, resolved_at
            FROM fuel_alerts
            WHERE vehicle_name = %s
              AND alerted_at  >= %s
            ORDER BY alerted_at DESC
        """, (unit, since))
        alerts = [dict(r) for r in cur.fetchall()]

        # ── Driver flags (confirmed losses) ───────────────────────────────────
        cur.execute("""
            SELECT
                flag_type, details,
                recommended_stop, actual_stop,
                fuel_pct, state,
                card_price, savings_lost,
                flagged_at
            FROM driver_flags
            WHERE vehicle_name = %s
              AND flagged_at  >= %s
            ORDER BY flagged_at DESC
        """, (unit, since))
        flags = [dict(r) for r in cur.fetchall()]

        result[unit] = {
            "driver": driver,
            "visits": visits,
            "alerts": alerts,
            "flags":  flags,
        }

    cur.close()
    conn.close()
    return result


# ── Compute per-truck stats ────────────────────────────────────────────────────

def compute_stats(truck_data: dict) -> dict:
    """
    For each truck compute:
      alerts_fired   — total fuel-low alerts sent
      followed       — times driver went to recommended stop
      skipped        — times driver fueled elsewhere or skipped
      savings_usd    — total confirmed savings (driver followed recommendation)
      losses_usd     — total confirmed losses (driver skipped / fueled elsewhere)
      net_usd        — savings - losses
      compliance_pct — followed / (followed + skipped) * 100
    """
    visits  = truck_data["visits"]
    alerts  = truck_data["alerts"]
    flags   = truck_data["flags"]

    followed = sum(1 for v in visits if v.get("visited") is True)
    skipped  = sum(1 for v in visits if v.get("visited") is False)

    savings_usd = sum(float(v["savings_usd"] or 0) for v in visits if v.get("visited") is True)
    # Losses from stop_visits (estimated) — fallback if no driver_flags
    sv_losses   = sum(abs(float(v["savings_usd"] or 0)) for v in visits if v.get("visited") is False)
    # Confirmed losses from driver_flags
    flag_losses = sum(float(f["savings_lost"] or 0) for f in flags if (f.get("savings_lost") or 0) > 0)
    losses_usd  = flag_losses if flag_losses > 0 else sv_losses

    total_sv       = followed + skipped
    compliance_pct = round(followed / total_sv * 100, 1) if total_sv else 0.0

    return {
        "alerts_fired":   len(alerts),
        "followed":       followed,
        "skipped":        skipped,
        "savings_usd":    round(savings_usd, 2),
        "losses_usd":     round(losses_usd,  2),
        "net_usd":        round(savings_usd - losses_usd, 2),
        "compliance_pct": compliance_pct,
        "flag_count":     len(flags),
    }


# ── Excel builder ──────────────────────────────────────────────────────────────

def build_report(truck_data: dict, since: datetime, until: datetime, output_path: str):
    period = since.strftime("%b %d") + " – " + until.strftime("%b %d, %Y")
    wb     = Workbook()

    # ══════════════════════════════════════════════════════════════════════════
    # SHEET 1 — FLEET SUMMARY
    # ══════════════════════════════════════════════════════════════════════════
    ws = wb.active
    ws.title = "Summary"
    ws.sheet_view.showGridLines = False
    ws.sheet_properties.tabColor = GREEN_DRK

    # Title
    ws.merge_cells("A1:K1")
    t = ws["A1"]
    t.value     = f"DieselUp — Diesel Savings & Losses Report  |  {period}"
    t.font      = Font(name="Arial", bold=True, color=GREEN, size=14)
    t.fill      = _fill(CARD2)
    t.alignment = _align("center")
    ws.row_dimensions[1].height = 34

    ws.merge_cells("A2:K2")
    ws["A2"].fill = _fill(CARD2)
    ws.row_dimensions[2].height = 6

    # Column headers
    cols = [
        ("Unit #",          12),
        ("Driver",          22),
        ("Alerts Fired",    13),
        ("Followed",        10),
        ("Skipped",         10),
        ("Compliance %",    14),
        ("Savings ($)",     13),
        ("Losses ($)",      13),
        ("Net ($)",         13),
        ("Flags",           8),
        ("Status",          10),
    ]
    for ci, (h, w) in enumerate(cols, 1):
        _hdr(ws, 3, ci, h, w=w, color=GREEN)
    ws.row_dimensions[3].height = 26

    total_savings = 0.0
    total_losses  = 0.0
    total_alerts  = 0
    total_followed = 0
    total_skipped  = 0

    for ri, (unit, driver) in enumerate(TARGET_TRUCKS, 4):
        td   = truck_data.get(unit, {"driver": driver, "visits": [], "alerts": [], "flags": []})
        st   = compute_stats(td)

        total_savings  += st["savings_usd"]
        total_losses   += st["losses_usd"]
        total_alerts   += st["alerts_fired"]
        total_followed += st["followed"]
        total_skipped  += st["skipped"]

        c_pct   = st["compliance_pct"]
        net     = st["net_usd"]

        c_comp  = GREEN if c_pct >= 80 else (AMBER if c_pct >= 50 else RED)
        c_net   = GREEN if net  >= 0   else RED
        c_skip  = RED   if st["skipped"] > 0 else WHITE
        c_flag  = RED   if st["flag_count"] > 0 else WHITE
        has_data = st["alerts_fired"] > 0 or st["followed"] > 0 or st["skipped"] > 0
        status   = "Active" if has_data else "No Data"
        c_status = GREEN if has_data else MUTED

        _cell(ws, ri, 1,  unit,                   bold=True)
        _cell(ws, ri, 2,  driver)
        _cell(ws, ri, 3,  st["alerts_fired"],      center=True)
        _cell(ws, ri, 4,  st["followed"],           center=True, color=GREEN)
        _cell(ws, ri, 5,  st["skipped"],            center=True, color=c_skip)
        _cell(ws, ri, 6,  f"{c_pct}%",             center=True, color=c_comp, bold=True)
        _cell(ws, ri, 7,  st["savings_usd"],        center=True, color=GREEN,  fmt="$#,##0.00")
        _cell(ws, ri, 8,  st["losses_usd"],         center=True, color=RED if st["losses_usd"] else WHITE, fmt="$#,##0.00")
        _cell(ws, ri, 9,  net,                      center=True, color=c_net, bold=True, fmt="$#,##0.00")
        _cell(ws, ri, 10, st["flag_count"],         center=True, color=c_flag, bold=st["flag_count"] > 0)
        _cell(ws, ri, 11, status,                   center=True, color=c_status)
        ws.row_dimensions[ri].height = 20

    # Totals row
    tr  = 4 + len(TARGET_TRUCKS)
    net_total = total_savings - total_losses
    total_sv  = total_followed + total_skipped
    fleet_pct = round(total_followed / total_sv * 100, 1) if total_sv else 0.0
    totals    = [
        "FLEET TOTAL", "",
        total_alerts, total_followed, total_skipped,
        f"{fleet_pct}%",
        total_savings, total_losses, net_total, "", "",
    ]
    fmts = [None, None, None, None, None, None, "$#,##0.00", "$#,##0.00", "$#,##0.00", None, None]
    for ci, (v, fmt) in enumerate(zip(totals, fmts), 1):
        c = ws.cell(row=tr, column=ci, value=v)
        c.font      = Font(name="Arial", bold=True, color=WHITE, size=10)
        c.fill      = _fill(GREEN_DRK)
        c.alignment = _align("center")
        c.border    = _border()
        if fmt:
            c.number_format = fmt
    ws.row_dimensions[tr].height = 24

    # Net summary callout
    tr2 = tr + 2
    ws.merge_cells(f"A{tr2}:K{tr2}")
    msg = (f"Fleet Net Position: {'SAVING' if net_total >= 0 else 'LOSING'} "
           f"${abs(net_total):,.2f}  |  "
           f"Fleet Compliance: {fleet_pct}%  |  "
           f"Period: {period}")
    c2 = ws[f"A{tr2}"]
    c2.value     = msg
    c2.font      = Font(name="Arial", bold=True, color=GREEN if net_total >= 0 else RED, size=11)
    c2.fill      = _fill(CARD2)
    c2.alignment = _align("center")
    ws.row_dimensions[tr2].height = 26

    ws.freeze_panes = "A4"

    # ══════════════════════════════════════════════════════════════════════════
    # SHEET 2 — ALERT DETAIL (every alert, every truck)
    # ══════════════════════════════════════════════════════════════════════════
    ws2 = wb.create_sheet("Alert Detail")
    ws2.sheet_view.showGridLines = False
    ws2.sheet_properties.tabColor = BLUE

    ws2.merge_cells("A1:L1")
    t2 = ws2["A1"]
    t2.value     = f"Fuel Alert Detail — {period}"
    t2.font      = Font(name="Arial", bold=True, color=BLUE, size=13)
    t2.fill      = _fill(CARD2)
    t2.alignment = _align("center")
    ws2.row_dimensions[1].height = 30

    cols2 = [
        ("Alert Date",      18),
        ("Unit #",          12),
        ("Driver",          22),
        ("Fuel % at Alert", 15),
        ("Recommended Stop",28),
        ("Actual Stop",     28),
        ("State",           7),
        ("Card $/gal",      12),
        ("Gallons",         10),
        ("Outcome",         12),
        ("Savings ($)",     13),
        ("Loss ($)",        13),
    ]
    for ci, (h, w) in enumerate(cols2, 1):
        _hdr(ws2, 2, ci, h, w=w, color=BLUE, bg=CARD2)
    ws2.row_dimensions[2].height = 26

    detail_row = 3
    for unit, driver in TARGET_TRUCKS:
        td     = truck_data.get(unit, {"driver": driver, "visits": [], "alerts": [], "flags": []})
        visits = td["visits"]
        alerts = td["alerts"]

        # Merge alerts and visits — prefer visit records (they have outcome)
        # Build lookup: alert_id → visit row
        alert_visit_map = {}
        for v in visits:
            # visits joined with alerts via alert_id (may be None for walk-in fuels)
            key = v.get("id")  # stop_visits.id — but we need alert linkage
            # We use alerted_at / visited_at proximity; simpler: just show visits
            pass

        # Show stop_visits rows (most complete data)
        for v in visits:
            visited_at = v.get("visited_at")
            date_str   = visited_at.strftime("%b %d %H:%M") if visited_at else ""
            alerted_at = v.get("alerted_at")
            alert_str  = alerted_at.strftime("%b %d %H:%M") if alerted_at else date_str

            followed   = v.get("visited")
            outcome    = "Followed ✓" if followed is True else ("Skipped ✗" if followed is False else "Unknown")
            c_out      = GREEN if followed is True else (RED if followed is False else AMBER)
            savings    = float(v.get("savings_usd") or 0) if followed is True  else 0.0
            loss       = abs(float(v.get("savings_usd") or 0)) if followed is False else 0.0
            gallons    = float(v.get("gallons_purchased") or 0)
            card_price = float(v.get("best_stop_price") or 0)

            _cell(ws2, detail_row, 1,  alert_str)
            _cell(ws2, detail_row, 2,  unit,                              bold=True)
            _cell(ws2, detail_row, 3,  driver)
            _cell(ws2, detail_row, 4,  f"{int(v.get('fuel_pct') or 0)}%", center=True,
                  color=RED if (v.get("fuel_pct") or 0) < 25 else AMBER)
            _cell(ws2, detail_row, 5,  v.get("recommended_stop_name") or "—")
            _cell(ws2, detail_row, 6,  v.get("actual_stop_name")      or "—")
            _cell(ws2, detail_row, 7,  v.get("actual_stop_state")     or "—", center=True)
            _cell(ws2, detail_row, 8,  card_price or None,              center=True, fmt="$#,##0.000")
            _cell(ws2, detail_row, 9,  gallons    or None,              center=True, fmt="#,##0.0")
            _cell(ws2, detail_row, 10, outcome,                          center=True, color=c_out, bold=True)
            _cell(ws2, detail_row, 11, savings or None,                  center=True, color=GREEN if savings else WHITE, fmt="$#,##0.00")
            _cell(ws2, detail_row, 12, loss    or None,                  center=True, color=RED   if loss    else WHITE, fmt="$#,##0.00")
            ws2.row_dimensions[detail_row].height = 19
            detail_row += 1

        # Also show open alerts that have no visit record yet
        visited_alert_ids = {v.get("id") for v in visits}
        for a in alerts:
            if a.get("status") == "open":
                alerted_at = a.get("alerted_at")
                date_str   = alerted_at.strftime("%b %d %H:%M") if alerted_at else ""
                _cell(ws2, detail_row, 1,  date_str)
                _cell(ws2, detail_row, 2,  unit,                   bold=True)
                _cell(ws2, detail_row, 3,  driver)
                _cell(ws2, detail_row, 4,  f"{int(a.get('fuel_pct') or 0)}%", center=True, color=RED)
                _cell(ws2, detail_row, 5,  a.get("best_stop_name") or "—")
                _cell(ws2, detail_row, 6,  "—")
                _cell(ws2, detail_row, 7,  a.get("best_stop_state") or "—", center=True)
                _cell(ws2, detail_row, 8,  float(a.get("best_stop_price") or 0) or None, center=True, fmt="$#,##0.000")
                _cell(ws2, detail_row, 9,  None, center=True)
                _cell(ws2, detail_row, 10, "Open ⏳", center=True, color=AMBER, bold=True)
                _cell(ws2, detail_row, 11, None, center=True)
                _cell(ws2, detail_row, 12, None, center=True)
                ws2.row_dimensions[detail_row].height = 19
                detail_row += 1

    ws2.freeze_panes = "A3"

    # ══════════════════════════════════════════════════════════════════════════
    # SHEET 3 — FLAGS / VIOLATIONS
    # ══════════════════════════════════════════════════════════════════════════
    ws3 = wb.create_sheet("Flags & Violations")
    ws3.sheet_view.showGridLines = False
    ws3.sheet_properties.tabColor = RED

    ws3.merge_cells("A1:I1")
    t3 = ws3["A1"]
    t3.value     = f"Driver Flags & Violations — {period}"
    t3.font      = Font(name="Arial", bold=True, color=RED, size=13)
    t3.fill      = _fill(CARD2)
    t3.alignment = _align("center")
    ws3.row_dimensions[1].height = 30

    cols3 = [
        ("Date",           18),
        ("Unit #",         12),
        ("Driver",         22),
        ("Flag Type",      18),
        ("Recommended",    30),
        ("Actual",         30),
        ("Fuel %",          8),
        ("Card $/gal",     12),
        ("Loss ($)",       13),
    ]
    for ci, (h, w) in enumerate(cols3, 1):
        _hdr(ws3, 2, ci, h, w=w, color=RED, bg="3A1010")
    ws3.row_dimensions[2].height = 26

    flag_row     = 3
    total_flagged = 0
    for unit, driver in TARGET_TRUCKS:
        td    = truck_data.get(unit, {"driver": driver, "visits": [], "alerts": [], "flags": []})
        flags = td["flags"]
        for f in flags:
            flagged_at = f.get("flagged_at")
            date_str   = flagged_at.strftime("%b %d %H:%M") if flagged_at else ""
            flag_type  = (f.get("flag_type") or "").replace("_", " ").title()
            loss       = float(f.get("savings_lost") or 0)
            FLAG_COLORS = {"Wrong Stop": RED, "Missed Stop": AMBER, "Low-Stop State": "FF8800"}
            c_flag_type = FLAG_COLORS.get(flag_type, WHITE)

            _cell(ws3, flag_row, 1, date_str)
            _cell(ws3, flag_row, 2, unit,                            bold=True)
            _cell(ws3, flag_row, 3, driver)
            _cell(ws3, flag_row, 4, flag_type,                       color=c_flag_type, bold=True)
            _cell(ws3, flag_row, 5, f.get("recommended_stop") or "—")
            _cell(ws3, flag_row, 6, f.get("actual_stop")      or "—", color=RED)
            _cell(ws3, flag_row, 7, f"{int(f.get('fuel_pct') or 0)}%", center=True,
                  color=AMBER if (f.get("fuel_pct") or 0) < 30 else WHITE)
            _cell(ws3, flag_row, 8, float(f.get("card_price") or 0) or None, center=True, fmt="$#,##0.000")
            _cell(ws3, flag_row, 9, loss or None,                    center=True,
                  color=RED if loss > 0 else WHITE, bold=loss > 0, fmt="$#,##0.00")
            ws3.row_dimensions[flag_row].height = 19
            flag_row     += 1
            total_flagged += 1

    if total_flagged == 0:
        ws3.merge_cells(f"A3:I3")
        c = ws3["A3"]
        c.value     = "No flags recorded in this period — excellent compliance!"
        c.font      = Font(name="Arial", color=GREEN, size=11)
        c.fill      = _fill(CARD3)
        c.alignment = _align("left")
        ws3.row_dimensions[3].height = 24

    ws3.freeze_panes = "A3"

    wb.save(output_path)
    print(f"Report saved: {output_path}")
    return output_path


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Generate diesel savings/losses report for target fleet trucks."
    )
    parser.add_argument("--days", type=int, default=30,
                        help="Look-back window in days (default: 30)")
    parser.add_argument("--out",  type=str, default="",
                        help="Output file path (default: diesel_savings_YYYYMMDD.xlsx)")
    args = parser.parse_args()

    until = datetime.now(timezone.utc)
    since = until - timedelta(days=args.days)
    out   = args.out or f"diesel_savings_{until.strftime('%Y%m%d')}.xlsx"

    print(f"Fetching data for {len(TARGET_TRUCKS)} trucks  |  "
          f"{since.strftime('%b %d')} – {until.strftime('%b %d, %Y')}  ({args.days}d)")

    truck_data = fetch_truck_data(since)

    # Print console summary before writing file
    print()
    print(f"{'UNIT':<28} {'DRIVER':<25} {'ALERTS':>7} {'FOLLOWED':>9} {'SKIPPED':>8} "
          f"{'COMP%':>7} {'SAVINGS':>10} {'LOSSES':>10} {'NET':>10}")
    print("─" * 120)

    g_savings = g_losses = 0.0
    for unit, driver in TARGET_TRUCKS:
        td = truck_data.get(unit, {"driver": driver, "visits": [], "alerts": [], "flags": []})
        st = compute_stats(td)
        g_savings += st["savings_usd"]
        g_losses  += st["losses_usd"]
        sign = "+" if st["net_usd"] >= 0 else ""
        print(f"{unit:<28} {driver:<25} {st['alerts_fired']:>7} {st['followed']:>9} "
              f"{st['skipped']:>8} {st['compliance_pct']:>6.1f}% "
              f"${st['savings_usd']:>9,.2f} ${st['losses_usd']:>9,.2f} "
              f"{sign}${st['net_usd']:>8,.2f}")

    print("─" * 120)
    g_net = g_savings - g_losses
    sign  = "+" if g_net >= 0 else ""
    print(f"{'FLEET TOTAL':<54} {'':>7} {'':>9} {'':>8} {'':>7} "
          f"${g_savings:>9,.2f} ${g_losses:>9,.2f} {sign}${g_net:>8,.2f}")
    print()

    build_report(truck_data, since, until, out)
    print(f"Done. Open '{out}' to view the report.")


if __name__ == "__main__":
    main()
