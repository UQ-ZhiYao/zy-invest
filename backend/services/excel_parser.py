"""
Excel parser service  v1.0.0
Parses Portfolio_Dashboard.xlsm and upserts all data into Supabase.
Handles: Historical, Investors, Transactions, Settlement, Dividends,
         Distributions, Others sheets.
Historical rows are imported as is_locked = TRUE (read-only).
"""
import io
import asyncio
from datetime import datetime, date
from typing import Optional
import openpyxl
import logging

logger = logging.getLogger(__name__)

# Row offset where data begins in each sheet (0-indexed, after header scan)
SHEET_CONFIG = {
    "Historical":    {"header_row": 11, "data_start": 12},
    "Investors":     {"header_row": 10, "data_start": 11},
    "Transaction":   {"header_row": 10, "data_start": 11},
    "Settlement":    {"header_row": 10, "data_start": 11},
    "Dividend":      {"header_row": 10, "data_start": 11},
    "Distributions": {"header_row": 10, "data_start": 11},
    "Others":        {"header_row": 10, "data_start": 11},
}


def safe_float(val) -> Optional[float]:
    try:
        if val is None or val == "" or val == "-":
            return None
        return float(val)
    except (ValueError, TypeError):
        return None


def safe_date(val) -> Optional[date]:
    if val is None:
        return None
    if isinstance(val, (datetime,)):
        return val.date()
    if isinstance(val, date):
        return val
    if isinstance(val, str):
        for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y"):
            try:
                return datetime.strptime(val.strip(), fmt).date()
            except ValueError:
                continue
    return None


def parse_and_import(file_content: bytes, db) -> dict:
    """
    Synchronous parse + import — run in executor from async context.
    Returns import summary dict.
    """
    wb = openpyxl.load_workbook(io.BytesIO(file_content), data_only=True)
    summary = {}

    # Run async imports via a new event loop in this thread
    loop = asyncio.new_event_loop()
    try:
        results = loop.run_until_complete(_import_all(wb, db))
        summary = results
    finally:
        loop.close()

    return summary


async def _import_all(wb, db) -> dict:
    summary = {}
    summary["historical"]    = await _import_historical(wb, db)
    summary["investors"]     = await _import_investors(wb, db)
    summary["transactions"]  = await _import_transactions(wb, db)
    summary["settlement"]    = await _import_settlement(wb, db)
    summary["dividends"]     = await _import_dividends(wb, db)
    summary["distributions"] = await _import_distributions(wb, db)
    summary["others"]        = await _import_others(wb, db)
    return summary


async def _import_historical(wb, db) -> dict:
    if "Historical" not in wb.sheetnames:
        return {"skipped": True}
    ws     = wb["Historical"]
    count  = 0
    errors = []

    for row in ws.iter_rows(min_row=12, values_only=True):
        if not row[4] or not isinstance(row[4], (datetime, date)):
            continue
        try:
            d = safe_date(row[4])
            if not d:
                continue
            await db.execute(
                """
                INSERT INTO historical (
                    date, derivatives, securities, reits, bonds, money_market,
                    receivables, cash, mng_fees, ints_on_fees, loans, ints_on_loans,
                    capital, earnings, total_units, nta, is_locked, source
                ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,TRUE,'excel_import')
                ON CONFLICT (date) DO UPDATE SET
                    nta         = EXCLUDED.nta,
                    securities  = EXCLUDED.securities,
                    cash        = EXCLUDED.cash,
                    mng_fees    = EXCLUDED.mng_fees,
                    total_units = EXCLUDED.total_units,
                    is_locked   = TRUE,
                    source      = 'excel_import'
                WHERE historical.is_locked = TRUE OR historical.source = 'excel_import'
                """,
                d,
                safe_float(row[5]) or 0,   # derivatives
                safe_float(row[6]) or 0,   # securities
                safe_float(row[7]) or 0,   # reits
                safe_float(row[8]) or 0,   # bonds
                safe_float(row[9]) or 0,   # mmf
                safe_float(row[10]) or 0,  # receivables
                safe_float(row[11]) or 0,  # cash
                safe_float(row[12]) or 0,  # mng_fees
                safe_float(row[13]) or 0,  # ints_on_fees
                safe_float(row[14]) or 0,  # loans
                safe_float(row[15]) or 0,  # ints_on_loans
                safe_float(row[16]) or 0,  # capital
                safe_float(row[17]) or 0,  # earnings
                safe_float(row[18]) or 0,  # units
                safe_float(row[19]) or 1,  # nta
            )
            count += 1
        except Exception as e:
            errors.append(str(e))

    return {"imported": count, "errors": len(errors)}


async def _import_investors(wb, db) -> dict:
    if "Investors" not in wb.sheetnames:
        return {"skipped": True}
    ws     = wb["Investors"]
    count  = 0

    for row in ws.iter_rows(min_row=11, values_only=True):
        name = row[4]
        if not name or name in ("Investors", "Total"):
            continue
        if not isinstance(name, str):
            continue
        try:
            await db.execute(
                """
                INSERT INTO investors (name, units, vwap, total_costs, current_nta,
                    market_value, unrealized_pl, realized_pl)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
                ON CONFLICT DO NOTHING
                """,
                str(name).strip(),
                safe_float(row[5]) or 0,
                safe_float(row[6]),
                safe_float(row[7]) or 0,
                safe_float(row[8]),
                safe_float(row[9]),
                safe_float(row[10]),
                safe_float(row[11]) or 0,
            )
            count += 1
        except Exception:
            pass

    return {"imported": count}


async def _import_transactions(wb, db) -> dict:
    if "Transaction" not in wb.sheetnames:
        return {"skipped": True}
    ws    = wb["Transaction"]
    count = 0

    for row in ws.iter_rows(min_row=11, values_only=True):
        d = safe_date(row[4])
        if not d:
            continue
        instrument = row[8]
        if not instrument:
            continue
        try:
            # Resolve investor_id — match by name (first investor for now; admin links manually)
            # In production, the admin maps transactions to investors via the UI
            await db.execute(
                """
                INSERT INTO transactions
                    (date, region, asset_class, sector, instrument,
                     units, price, amount, total_fees, net_amount, theme,
                     investor_id)
                SELECT $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11, id
                FROM investors LIMIT 1
                ON CONFLICT DO NOTHING
                """,
                d,
                str(row[5] or "MY"),
                str(row[6] or ""),
                str(row[7] or "") if row[7] else None,
                str(instrument),
                safe_float(row[9]) or 0,
                safe_float(row[10]) or 0,
                safe_float(row[11]) or 0,
                safe_float(row[12]) or 0,
                safe_float(row[13]) or 0,
                str(row[14]) if row[14] else None,
            )
            count += 1
        except Exception:
            pass

    return {"imported": count, "note": "Review investor_id assignments in admin panel"}


async def _import_settlement(wb, db) -> dict:
    if "Settlement" not in wb.sheetnames:
        return {"skipped": True}
    ws    = wb["Settlement"]
    count = 0

    for row in ws.iter_rows(min_row=11, values_only=True):
        d = safe_date(row[4])
        if not d:
            continue
        instrument = row[8]
        if not instrument:
            continue
        try:
            await db.execute(
                """
                INSERT INTO settlement
                    (date, region, asset_class, sector, instrument,
                     units, bought_price, sale_price, profit_loss, return_pct,
                     investor_id)
                SELECT $1,$2,$3,$4,$5,$6,$7,$8,$9,$10, id FROM investors LIMIT 1
                ON CONFLICT DO NOTHING
                """,
                d,
                str(row[5] or "MY"),
                str(row[6] or ""),
                str(row[7]) if row[7] else None,
                str(instrument),
                safe_float(row[9]) or 0,
                safe_float(row[10]) or 0,
                safe_float(row[11]) or 0,
                safe_float(row[12]) or 0,
                safe_float(row[13]),
            )
            count += 1
        except Exception:
            pass

    return {"imported": count}


async def _import_dividends(wb, db) -> dict:
    if "Dividend" not in wb.sheetnames:
        return {"skipped": True}
    ws    = wb["Dividend"]
    count = 0

    for row in ws.iter_rows(min_row=11, values_only=True):
        ex_date = safe_date(row[5])
        if not ex_date:
            continue
        instrument = row[8]
        if not instrument:
            continue
        try:
            await db.execute(
                """
                INSERT INTO dividends
                    (ann_date, ex_date, pmt_date, asset_class, instrument,
                     units, dps_sen, amount, entitlement, investor_id)
                SELECT $1,$2,$3,$4,$5,$6,$7,$8,$9, id FROM investors LIMIT 1
                ON CONFLICT DO NOTHING
                """,
                safe_date(row[4]),
                ex_date,
                safe_date(row[6]),
                str(row[7]) if row[7] else None,
                str(instrument),
                safe_float(row[9]) or 0,
                safe_float(row[10]) or 0,
                safe_float(row[11]) or 0,
                str(row[12]) if row[12] else None,
            )
            count += 1
        except Exception:
            pass

    return {"imported": count}


async def _import_distributions(wb, db) -> dict:
    if "Distributions" not in wb.sheetnames:
        return {"skipped": True}
    ws    = wb["Distributions"]
    count = 0

    for row in ws.iter_rows(min_row=11, values_only=True):
        ex_date = safe_date(row[5])
        if not ex_date:
            continue
        title = row[8]
        if not title or title == "Title":
            continue
        try:
            pmt_date = safe_date(row[6])
            fy       = str(row[7]) if row[7] else "FY?"
            dist_type = "interim" if "Interim" in str(title) else \
                        "final"   if "Final"   in str(title) else "special"
            await db.execute(
                """
                INSERT INTO distributions
                    (ann_date, ex_date, pmt_date, financial_year, title, dist_type,
                     dps_sen, total_units, total_dividend, payout_ratio)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
                ON CONFLICT DO NOTHING
                """,
                safe_date(row[4]),
                ex_date,
                pmt_date,
                fy,
                str(title),
                dist_type,
                safe_float(row[9]) or 0,
                safe_float(row[10]),
                safe_float(row[11]),
                safe_float(row[12]),
            )
            count += 1
        except Exception:
            pass

    return {"imported": count}


async def _import_others(wb, db) -> dict:
    if "Others" not in wb.sheetnames:
        return {"skipped": True}
    ws    = wb["Others"]
    count = 0

    for row in ws.iter_rows(min_row=11, values_only=True):
        d = safe_date(row[4])
        if not d:
            continue
        try:
            await db.execute(
                """
                INSERT INTO others (record_date, title, income_type, amount, platform, description)
                VALUES ($1,$2,$3,$4,$5,$6)
                ON CONFLICT DO NOTHING
                """,
                d,
                str(row[5]) if row[5] else "Other",
                str(row[6]) if row[6] else "Others",
                safe_float(row[7]) or 0,
                str(row[8]) if row[8] else None,
                str(row[9]) if row[9] else None,
            )
            count += 1
        except Exception:
            pass

    return {"imported": count}
