"""
Weekly PDF report — DieselUp NJ.

Generates a 4-page PDF covering the previous 7 days of fueling decisions and
posts it to the admin Telegram chat as a document. Filename pattern is
`dieselup_weekly_YYYY-MM-DD.pdf` where the date is the report's run date in EST.

Pages:
  1. Fleet summary — week range, savings, losses, net, compliance, gallons.
  2. Per-driver breakdown — recommendations, saved/lost/skipped, net $.
  3. Top 5 red flags — biggest wrong/missed-stop losses.
  4. Top 5 best saves — biggest savings.

Header on every page: "DieselUp Weekly Report — Zamin Transport".
Footer: generation timestamp in EST.

Wire into APScheduler from main.py:

    scheduler.add_job(
        run_weekly_report,
        CronTrigger(day_of_week="sat", hour=6, minute=0, timezone="America/New_York"),
        args=[application.bot],
        id="weekly_report",
        replace_existing=True,
    )
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path
from tempfile import gettempdir
from zoneinfo import ZoneInfo

from reportlab.lib import colors
from reportlab.lib.pagesizes import LETTER
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import inch
from reportlab.platypus import (
    PageBreak,
    Paragraph,
    SimpleDocTemplate,
    Spacer,
    Table,
    TableStyle,
)
from telegram import Bot

from dieselup.config import settings
from dieselup.db import fetch_all, fetch_one

log = logging.getLogger(__name__)

EST = ZoneInfo("America/New_York")
REPORT_HEADER = "DieselUp Weekly Report — Zamin Transport"


def _week_range_est(now_est: datetime) -> tuple[datetime, datetime]:
    """Return [start, end) in EST covering the 7 days ending at this run's 06:00 EST."""
    end_est = now_est.replace(hour=6, minute=0, second=0, microsecond=0)
    start_est = end_est - timedelta(days=7)
    return start_est, end_est


def _money(value) -> str:
    """Format a numeric value as $1,234.56 — handles None, Decimal, float, int."""
    if value is None:
        return "$0.00"
    return f"${float(value):,.2f}"


def _signed_money(value) -> str:
    """Same as _money, but explicit sign for negatives."""
    if value is None:
        return "$0.00"
    v = float(value)
    if v < 0:
        return f"-${abs(v):,.2f}"
    return f"${v:,.2f}"


def _fmt_gallons(gallons) -> str:
    """Format a raw gallon value (None, Decimal, float, int) as '1,234 gal'."""
    if gallons is None:
        return "0 gal"
    return f"{int(float(gallons)):,} gal"


async def _fleet_summary(start_utc: datetime, end_utc: datetime):
    return await fetch_one(
        """
        SELECT
            COUNT(*) AS total_recs,
            COUNT(*) FILTER (WHERE status = 'saved') AS saved_count,
            COUNT(*) FILTER (WHERE status = 'lost') AS lost_count,
            COUNT(*) FILTER (WHERE status = 'skipped') AS skipped_count,
            COUNT(*) FILTER (WHERE status = 'pending') AS pending_count,
            COALESCE(SUM(dollar_impact) FILTER (WHERE status = 'saved'), 0) AS savings,
            COALESCE(SUM(
                CASE
                    WHEN status = 'lost' AND dollar_impact < 0
                        THEN -dollar_impact
                    WHEN status = 'skipped'
                        THEN GREATEST(worst_candidate_true_cost - recommended_true_cost, 0) * gallons
                    ELSE 0
                END
            ), 0) AS losses,
            COALESCE(SUM(gallons), 0) AS gallons_recommended,
            COALESCE(SUM(gallons) FILTER (WHERE status = 'saved'), 0) AS gallons_saved
        FROM stop_events
        WHERE recommended_at >= $1 AND recommended_at < $2
        """,
        start_utc,
        end_utc,
    )


async def _per_driver(start_utc: datetime, end_utc: datetime):
    return await fetch_all(
        """
        SELECT
            COALESCE(td.driver_full_name, '(unassigned)') AS driver,
            se.truck_unit,
            COUNT(*) AS recommendations,
            COUNT(*) FILTER (WHERE se.status = 'saved') AS saved,
            COUNT(*) FILTER (WHERE se.status = 'lost') AS lost,
            COUNT(*) FILTER (WHERE se.status = 'skipped') AS skipped,
            COALESCE(SUM(
                CASE
                    WHEN se.status = 'skipped'
                         AND (se.dollar_impact IS NULL OR se.dollar_impact >= 0)
                        THEN -GREATEST(se.worst_candidate_true_cost - se.recommended_true_cost, 0) * se.gallons
                    ELSE COALESCE(se.dollar_impact, 0)
                END
            ), 0) AS net
        FROM stop_events se
        LEFT JOIN trucks_drivers td ON td.truck_unit = se.truck_unit
        WHERE se.recommended_at >= $1 AND se.recommended_at < $2
        GROUP BY td.driver_full_name, se.truck_unit
        ORDER BY net DESC
        """,
        start_utc,
        end_utc,
    )


async def _top_losses(start_utc: datetime, end_utc: datetime, limit: int = 5):
    return await fetch_all(
        """
        WITH site_loc AS (
            SELECT DISTINCT ON (site_id) site_id, city, state
            FROM contracted_prices
            ORDER BY site_id, effective_date DESC
        )
        SELECT
            se.recommended_at,
            CASE
                WHEN se.status = 'skipped' THEN 'missed stop'
                ELSE 'wrong stop'
            END AS result,
            COALESCE(td.driver_full_name, '(unassigned)') AS driver,
            se.truck_unit,
            COALESCE(rec.city || ', ' || rec.state, 'site ' || se.recommended_site_id::text) AS recommended_loc,
            COALESCE(
                act.city || ', ' || act.state,
                CASE WHEN se.status = 'skipped' THEN 'not detected' ELSE 'unknown' END
            ) AS actual_loc,
            CASE
                WHEN se.status = 'skipped'
                    THEN GREATEST(se.worst_candidate_true_cost - se.recommended_true_cost, 0) * se.gallons
                ELSE -se.dollar_impact
            END AS lost_dollars
        FROM stop_events se
        LEFT JOIN trucks_drivers td ON td.truck_unit = se.truck_unit
        LEFT JOIN site_loc rec ON rec.site_id = se.recommended_site_id
        LEFT JOIN site_loc act ON act.site_id = se.actual_site_id
        WHERE (
            (se.status = 'lost' AND se.dollar_impact < 0)
            OR (
                se.status = 'skipped'
                AND GREATEST(se.worst_candidate_true_cost - se.recommended_true_cost, 0) * se.gallons > 0
            )
        )
          AND se.recommended_at >= $1 AND se.recommended_at < $2
        ORDER BY lost_dollars DESC NULLS LAST
        LIMIT $3
        """,
        start_utc,
        end_utc,
        limit,
    )


async def _top_saves(start_utc: datetime, end_utc: datetime, limit: int = 5):
    return await fetch_all(
        """
        WITH site_loc AS (
            SELECT DISTINCT ON (site_id) site_id, city, state
            FROM contracted_prices
            ORDER BY site_id, effective_date DESC
        )
        SELECT
            se.recommended_at,
            COALESCE(td.driver_full_name, '(unassigned)') AS driver,
            se.truck_unit,
            COALESCE(rec.city || ', ' || rec.state, 'site ' || se.recommended_site_id::text) AS recommended_loc,
            se.dollar_impact AS saved_dollars
        FROM stop_events se
        LEFT JOIN trucks_drivers td ON td.truck_unit = se.truck_unit
        LEFT JOIN site_loc rec ON rec.site_id = se.recommended_site_id
        WHERE se.status = 'saved'
          AND se.recommended_at >= $1 AND se.recommended_at < $2
        ORDER BY se.dollar_impact DESC NULLS LAST
        LIMIT $3
        """,
        start_utc,
        end_utc,
        limit,
    )


def _header_footer(canvas, doc, generated_est: datetime) -> None:
    """Draw the per-page header and footer."""
    canvas.saveState()
    width, height = LETTER
    canvas.setFont("Helvetica-Bold", 12)
    canvas.drawString(0.75 * inch, height - 0.5 * inch, REPORT_HEADER)
    canvas.setStrokeColor(colors.grey)
    canvas.line(0.75 * inch, height - 0.6 * inch, width - 0.75 * inch, height - 0.6 * inch)
    canvas.setFont("Helvetica", 8)
    canvas.setFillColor(colors.grey)
    footer = f"Generated {generated_est.strftime('%Y-%m-%d %H:%M %Z')}  |  Page {doc.page}"
    canvas.drawString(0.75 * inch, 0.5 * inch, footer)
    canvas.restoreState()


def _styles() -> dict:
    base = getSampleStyleSheet()
    return {
        "title": ParagraphStyle(
            "title", parent=base["Heading1"], fontSize=16, spaceAfter=12
        ),
        "h2": ParagraphStyle(
            "h2", parent=base["Heading2"], fontSize=12, spaceAfter=8
        ),
        "body": ParagraphStyle(
            "body", parent=base["BodyText"], fontSize=10, leading=14
        ),
        "small": ParagraphStyle(
            "small", parent=base["BodyText"], fontSize=9, leading=12,
            textColor=colors.grey,
        ),
    }


def _table_style() -> TableStyle:
    return TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#1f3a5f")),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.whitesmoke),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, 0), 9),
        ("FONTNAME", (0, 1), (-1, -1), "Helvetica"),
        ("FONTSIZE", (0, 1), (-1, -1), 9),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.whitesmoke, colors.white]),
        ("GRID", (0, 0), (-1, -1), 0.25, colors.lightgrey),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("LEFTPADDING", (0, 0), (-1, -1), 6),
        ("RIGHTPADDING", (0, 0), (-1, -1), 6),
        ("TOPPADDING", (0, 0), (-1, -1), 4),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
    ])


def _build_pdf(
    path: Path,
    start_est: datetime,
    end_est: datetime,
    generated_est: datetime,
    summary,
    per_driver,
    losses,
    saves,
) -> None:
    """Assemble and write the PDF to `path`."""
    doc = SimpleDocTemplate(
        str(path),
        pagesize=LETTER,
        leftMargin=0.75 * inch,
        rightMargin=0.75 * inch,
        topMargin=0.9 * inch,
        bottomMargin=0.75 * inch,
        title="DieselUp Weekly Report",
        author="DieselUp NJ",
    )
    styles = _styles()
    story = []

    week_label = f"{start_est.strftime('%a %b %d, %Y')} — {end_est.strftime('%a %b %d, %Y')} (EST)"

    # ---- Page 1: Fleet summary
    total = int(summary["total_recs"] or 0)
    saved_count = int(summary["saved_count"] or 0)
    lost_count = int(summary["lost_count"] or 0)
    skipped_count = int(summary["skipped_count"] or 0)
    pending_count = int(summary["pending_count"] or 0)
    savings = Decimal(summary["savings"] or 0)
    losses_amt = Decimal(summary["losses"] or 0)
    net = savings - losses_amt
    resolved = saved_count + lost_count + skipped_count
    compliance = (saved_count / resolved * 100.0) if resolved else 0.0

    story.append(Paragraph("Fleet Summary", styles["title"]))
    story.append(Paragraph(week_label, styles["small"]))
    story.append(Spacer(1, 0.2 * inch))

    gallons_recommended = summary["gallons_recommended"]
    gallons_saved = summary["gallons_saved"]

    fleet_rows = [
        ["Metric", "Value"],
        ["Recommendations issued", f"{total:,}"],
        ["  Saved (driver followed)", f"{saved_count:,}"],
        ["  Lost (driver went elsewhere)", f"{lost_count:,}"],
        ["  Skipped (no Pilot/FJ stop)", f"{skipped_count:,}"],
        ["  Pending (unresolved)", f"{pending_count:,}"],
        ["Total savings", _money(savings)],
        ["Total losses", _money(losses_amt)],
        ["Net result", _signed_money(net)],
        ["Compliance rate", f"{compliance:.1f}%"],
        ["Gallons recommended (all recs)", _fmt_gallons(gallons_recommended)],
        ["Gallons at recommended stop", _fmt_gallons(gallons_saved)],
    ]
    fleet_table = Table(fleet_rows, colWidths=[3.2 * inch, 2.5 * inch])
    fleet_table.setStyle(_table_style())
    story.append(fleet_table)
    story.append(PageBreak())

    # ---- Page 2: Per-driver
    story.append(Paragraph("Per-Driver Breakdown", styles["title"]))
    story.append(Paragraph(week_label, styles["small"]))
    story.append(Spacer(1, 0.2 * inch))

    if per_driver:
        driver_rows = [["Driver", "Truck", "Recs", "Saved", "Lost", "Skipped", "Net $"]]
        for row in per_driver:
            driver_rows.append([
                row["driver"],
                row["truck_unit"],
                f"{int(row['recommendations']):,}",
                f"{int(row['saved']):,}",
                f"{int(row['lost']):,}",
                f"{int(row['skipped']):,}",
                _signed_money(row["net"]),
            ])
        driver_table = Table(
            driver_rows,
            colWidths=[2.0 * inch, 0.8 * inch, 0.6 * inch, 0.7 * inch, 0.6 * inch, 0.8 * inch, 1.0 * inch],
            repeatRows=1,
        )
        driver_table.setStyle(_table_style())
        story.append(driver_table)
    else:
        story.append(Paragraph("No driver activity recorded this week.", styles["body"]))
    story.append(PageBreak())

    # ---- Page 3: Top 5 red flags
    story.append(Paragraph("Top 5 Red Flags", styles["title"]))
    story.append(Paragraph(week_label, styles["small"]))
    story.append(Spacer(1, 0.2 * inch))

    if losses:
        loss_rows = [["Date", "Driver", "Truck", "Result", "Recommended", "Actual", "$ Lost"]]
        for row in losses:
            occurred_est = row["recommended_at"].astimezone(EST)
            loss_rows.append([
                occurred_est.strftime("%Y-%m-%d"),
                row["driver"],
                row["truck_unit"],
                row["result"],
                row["recommended_loc"],
                row["actual_loc"],
                _money(row["lost_dollars"]),
            ])
        loss_table = Table(
            loss_rows,
            colWidths=[
                0.8 * inch,
                1.3 * inch,
                0.6 * inch,
                0.8 * inch,
                1.3 * inch,
                1.2 * inch,
                0.8 * inch,
            ],
            repeatRows=1,
        )
        loss_table.setStyle(_table_style())
        story.append(loss_table)
    else:
        story.append(Paragraph("No red flags recorded this week.", styles["body"]))
    story.append(PageBreak())

    # ---- Page 4: Top 5 best saves
    story.append(Paragraph("Top 5 Best Saves", styles["title"]))
    story.append(Paragraph(week_label, styles["small"]))
    story.append(Spacer(1, 0.2 * inch))

    if saves:
        save_rows = [["Date", "Driver", "Truck", "Recommended Stop", "$ Saved"]]
        for row in saves:
            occurred_est = row["recommended_at"].astimezone(EST)
            save_rows.append([
                occurred_est.strftime("%Y-%m-%d"),
                row["driver"],
                row["truck_unit"],
                row["recommended_loc"],
                _money(row["saved_dollars"]),
            ])
        save_table = Table(
            save_rows,
            colWidths=[0.9 * inch, 1.8 * inch, 0.8 * inch, 2.3 * inch, 1.0 * inch],
            repeatRows=1,
        )
        save_table.setStyle(_table_style())
        story.append(save_table)
    else:
        story.append(Paragraph("No saves recorded this week.", styles["body"]))

    def _on_page(canvas, doc_):
        _header_footer(canvas, doc_, generated_est)

    doc.build(story, onFirstPage=_on_page, onLaterPages=_on_page)


async def generate_weekly_report(now_est: datetime | None = None) -> tuple[Path, str]:
    """
    Build the PDF for the week ending at `now_est` (defaults to now in EST).
    Returns (pdf_path, one_line_summary). Caller is responsible for sending it.
    """
    generated_est = (now_est or datetime.now(EST)).astimezone(EST)
    start_est, end_est = _week_range_est(generated_est)
    start_utc = start_est.astimezone(ZoneInfo("UTC"))
    end_utc = end_est.astimezone(ZoneInfo("UTC"))

    summary = await _fleet_summary(start_utc, end_utc)
    per_driver = await _per_driver(start_utc, end_utc)
    losses = await _top_losses(start_utc, end_utc)
    saves = await _top_saves(start_utc, end_utc)

    out_dir = Path(gettempdir())
    out_path = out_dir / f"dieselup_weekly_{generated_est.strftime('%Y-%m-%d')}.pdf"

    _build_pdf(out_path, start_est, end_est, generated_est, summary, per_driver, losses, saves)

    savings = Decimal(summary["savings"] or 0)
    losses_amt = Decimal(summary["losses"] or 0)
    net = savings - losses_amt
    total = int(summary["total_recs"] or 0)
    saved_count = int(summary["saved_count"] or 0)
    resolved = saved_count + int(summary["lost_count"] or 0) + int(summary["skipped_count"] or 0)
    compliance = (saved_count / resolved * 100.0) if resolved else 0.0

    one_liner = (
        f"Weekly report {start_est.strftime('%b %d')}–{end_est.strftime('%b %d')}: "
        f"net {_signed_money(net)} across {total} recs, compliance {compliance:.0f}%."
    )

    return out_path, one_liner


async def run_weekly_report(bot: Bot) -> None:
    """APScheduler entrypoint. Builds the PDF and posts it to the admin chat."""
    from dieselup.bot.sender import safe_send

    try:
        pdf_path, one_liner = await generate_weekly_report()
    except Exception as exc:
        log.exception("Weekly report generation failed")
        await safe_send(
            bot=bot,
            chat_id=settings.TELEGRAM_ADMIN_CHAT_ID,
            text=f"Weekly report failed: {exc}",
            alert_type="weekly_report_build_err",
            parse_mode=None,
        )
        return

    try:
        with pdf_path.open("rb") as fh:
            await bot.send_document(
                chat_id=settings.TELEGRAM_ADMIN_CHAT_ID,
                document=fh,
                filename=pdf_path.name,
                caption=one_liner,
            )
        log.info("Weekly report posted: %s", pdf_path.name)
    except Exception as exc:
        log.exception("Failed to post weekly report to Telegram")
        await safe_send(
            bot=bot,
            chat_id=settings.TELEGRAM_ADMIN_CHAT_ID,
            text=f"Weekly report built but upload failed: {exc}\nLocal path: {pdf_path}",
            alert_type="weekly_report_upload_err",
            parse_mode=None,
        )
