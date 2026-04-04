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
    total_units = float(settings["total_units"]) if settings else 0.0

    # Fallback: sum from investors, then from last historical
    if total_units <= 0:
        ur = await db.fetchrow(
            "SELECT COALESCE(SUM(units),0) AS t FROM investors WHERE is_active=TRUE")
        total_units = float(ur["t"]) if ur else 0.0
    if total_units <= 0:
        ph = await db.fetchrow(
            "SELECT total_units FROM historical WHERE date < $1 ORDER BY date DESC LIMIT 1",
            target_date)
        total_units = float(ph["total_units"]) if ph else 0.0
    if total_units <= 0:
        logger.warning(f"total_units is zero for {target_date} — skipping")
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

    # Performance fee: annualised return since inception vs hurdle
    perf_fee = 0.0
    if perf_sched:
        try:
            inc = await db.fetchrow(
                "SELECT date FROM historical ORDER BY date ASC LIMIT 1")
            days_elapsed = max(1, (target_date - inc["date"]).days) if inc else 1
            annualised   = max(0.0, gross_nta) ** (365.0 / days_elapsed) - 1
            hurdle = float(perf_sched["hurdle_rate"] or 0)
            if annualised > hurdle:
                excess_return = annualised - hurdle
                perf_fee = total_units * (excess_return / 365) * float(perf_sched["rate"])
        except Exception as e:
            logger.warning(f"Perf fee calc failed {target_date}: {e}")
            perf_fee = 0.0

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


async def compute_nta_range(db, from_date=None, to_date=None):
    """Compute NTA for every calendar day from from_date to to_date."""
    from datetime import date as date_t
    if to_date is None:
        to_date = date_t.today()
    if from_date is None:
        last = await db.fetchrow("SELECT MAX(date) AS d FROM historical")
        from_date = (last["d"] + timedelta(days=1)) if last and last["d"] else to_date
    if from_date > to_date:
        return {"computed": 0, "message": f"Already up to date"}
    computed = 0; errors = 0; current = from_date
    while current <= to_date:
        try:
            r = await compute_daily_nta(db, current)
            if r:
                computed += 1
                logger.info(f"✓ {current}: NTA={r.get('net_nta')}")
            else:
                logger.warning(f"✗ {current}: returned None (check total_units/holdings/prev_hist)")
                errors += 1
        except Exception as e:
            import traceback
            logger.error(f"✗ {current}: {e}\n{traceback.format_exc()}")
            errors += 1
        current += timedelta(days=1)
    logger.info(f"Range done: {computed} computed, {errors} failed")
    return {"computed": computed, "errors": errors,
            "from": str(from_date), "to": str(to_date)}
