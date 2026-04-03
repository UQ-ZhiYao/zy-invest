"""
NTA computation engine  v1.0.0
Computes daily Net Asset Value per unit after fee accruals.

Formula:
  1. Gross NTA = sum(units × price for all holdings) + cash / total_units
  2. Base fee   = AUM × base_rate / 365
  3. Perf fee   = AUM × max(0, daily_return − hurdle/365) × perf_rate
  4. Net NTA    = Gross NTA − (base_fee + perf_fee + other_expenses) / total_units
"""
from datetime import date, timedelta
from typing import Optional
import logging

logger = logging.getLogger(__name__)


async def compute_daily_nta(db, target_date: date = None) -> Optional[dict]:
    """
    Compute NTA for target_date (default: today).
    Saves result to historical table.
    Returns computed NTA dict or None if insufficient data.
    """
    if target_date is None:
        target_date = date.today()

    # Don't recompute locked (Excel-imported) rows
    existing = await db.fetchrow(
        "SELECT nta, is_locked FROM historical WHERE date = $1", target_date
    )
    if existing and existing["is_locked"]:
        logger.info(f"NTA for {target_date} is locked — skipping recompute")
        return {"date": str(target_date), "nta": float(existing["nta"]), "locked": True}

    # Get previous day NTA
    prev = await db.fetchrow(
        "SELECT nta, date FROM historical WHERE date < $1 ORDER BY date DESC LIMIT 1",
        target_date
    )
    if not prev:
        logger.warning(f"No previous NTA found before {target_date}")
        return None

    prev_nta = float(prev["nta"])

    # Get fund settings
    settings = await db.fetchrow("SELECT * FROM fund_settings WHERE id = 1")
    total_units = float(settings["total_units"])
    if total_units <= 0:
        logger.warning("total_units is zero — cannot compute NTA")
        return None

    # Compute gross AUM from holdings
    holdings = await db.fetch(
        "SELECT instrument, units, last_price FROM holdings WHERE units > 0"
    )
    securities_value = sum(
        float(h["units"]) * float(h["last_price"] or 0)
        for h in holdings
        if h["last_price"]
    )

    # Get cash balance (from last historical row)
    prev_hist = await db.fetchrow(
        "SELECT cash FROM historical WHERE date < $1 ORDER BY date DESC LIMIT 1",
        target_date
    )
    cash = float(prev_hist["cash"]) if prev_hist else 0.0

    # Deduct any fee withdrawals on this date from cash
    fee_w = await db.fetch(
        "SELECT fee_type, amount FROM fee_withdrawals WHERE date = $1",
        target_date
    )
    mgmt_withdrawn = sum(float(r["amount"]) for r in fee_w if r["fee_type"] == "management")
    perf_withdrawn = sum(float(r["amount"]) for r in fee_w if r["fee_type"] == "performance")
    cash -= (mgmt_withdrawn + perf_withdrawn)

    gross_aum = securities_value + cash
    gross_nta = gross_aum / total_units if total_units > 0 else 0

    # Daily return vs previous NTA
    daily_return = (gross_nta / prev_nta) - 1 if prev_nta > 0 else 0

    # Get active fee schedules for target_date
    base_sched = await db.fetchrow(
        """
        SELECT rate FROM fee_schedules
        WHERE fee_type = 'base'
          AND valid_from <= $1
          AND (valid_to IS NULL OR valid_to >= $1)
        ORDER BY valid_from DESC LIMIT 1
        """,
        target_date
    )
    perf_sched = await db.fetchrow(
        """
        SELECT rate, hurdle_rate FROM fee_schedules
        WHERE fee_type = 'performance'
          AND valid_from <= $1
          AND (valid_to IS NULL OR valid_to >= $1)
        ORDER BY valid_from DESC LIMIT 1
        """,
        target_date
    )

    # Base fee accrual: AUM × rate / 365
    base_fee = 0.0
    if base_sched:
        base_fee = gross_aum * float(base_sched["rate"]) / 365.0

    # Performance fee accrual: AUM × max(0, daily_return − daily_hurdle) × rate
    perf_fee = 0.0
    if perf_sched:
        annual_hurdle = float(perf_sched["hurdle_rate"] or 0)
        daily_hurdle  = (1 + annual_hurdle) ** (1/365) - 1
        excess_return = max(0.0, daily_return - daily_hurdle)
        perf_fee = gross_aum * excess_return * float(perf_sched["rate"])

    total_fee = base_fee + perf_fee

    # Net NTA after fee deduction
    net_nta = gross_nta - (total_fee / total_units if total_units > 0 else 0)

    # Categorise securities by type for historical row
    sec_classes = {h["instrument"]: float(h["units"]) * float(h["last_price"] or 0)
                   for h in holdings if h["last_price"]}

    # Get class mappings
    class_map = await db.fetch(
        "SELECT instrument, asset_class FROM ticker_map WHERE instrument = ANY($1)",
        list(sec_classes.keys())
    )
    class_lookup = {r["instrument"]: r["asset_class"] for r in class_map}

    sec_total  = sum(v for k, v in sec_classes.items()
                    if "Securities" in class_lookup.get(k, ""))
    reit_total = sum(v for k, v in sec_classes.items()
                    if "Real Estate" in class_lookup.get(k, ""))
    mm_total   = sum(v for k, v in sec_classes.items()
                    if "Money Market" in class_lookup.get(k, ""))

    # Get accumulated fee liabilities from previous row
    prev_mng = await db.fetchrow(
        "SELECT mng_fees, perf_fees FROM historical WHERE date < $1 ORDER BY date DESC LIMIT 1",
        target_date
    )
    acc_mng  = float(prev_mng["mng_fees"] or 0)  + base_fee if prev_mng else base_fee
    acc_perf = float(prev_mng["perf_fees"] or 0) + perf_fee if prev_mng else perf_fee
    # Reduce liabilities by fee withdrawals on this date
    acc_mng  = max(0.0, acc_mng  - mgmt_withdrawn)
    acc_perf = max(0.0, acc_perf - perf_withdrawn)

    # Upsert to historical
    await db.execute(
        """
        INSERT INTO historical (
            date, securities, reits, money_market, cash,
            mng_fees, perf_fees, capital, earnings, total_units, nta,
            is_locked, source
        ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11, FALSE, 'system')
        ON CONFLICT (date) DO UPDATE SET
            securities  = EXCLUDED.securities,
            reits       = EXCLUDED.reits,
            money_market= EXCLUDED.money_market,
            cash        = EXCLUDED.cash,
            mng_fees    = EXCLUDED.mng_fees,
            perf_fees   = EXCLUDED.perf_fees,
            total_units = EXCLUDED.total_units,
            nta         = EXCLUDED.nta,
            source      = 'system'
        WHERE historical.is_locked = FALSE
        """,
        target_date,
        round(sec_total, 2), round(reit_total, 2), round(mm_total, 2),
        round(cash, 2), round(acc_mng, 2), round(acc_perf, 2),
        float(settings["aum"]), 0.0,  # capital = AUM, earnings computed separately
        total_units, round(net_nta, 6)
    )

    # Update fund_settings with latest NTA
    await db.execute(
        """
        UPDATE fund_settings
        SET current_nta = $1, aum = $2, last_nta_date = $3, updated_at = NOW()
        WHERE id = 1
        """,
        round(net_nta, 6), round(gross_aum - total_fee, 2), target_date
    )

    logger.info(
        f"NTA computed for {target_date}: {round(net_nta, 6)} "
        f"(base_fee={round(base_fee,2)}, perf_fee={round(perf_fee,2)})"
    )

    return {
        "date":       str(target_date),
        "gross_nta":  round(gross_nta, 6),
        "net_nta":    round(net_nta, 6),
        "base_fee":   round(base_fee, 2),
        "perf_fee":   round(perf_fee, 2),
        "daily_return": round(daily_return * 100, 4),
        "locked":     False,
    }
