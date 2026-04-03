"""
NTA computation engine  v2.0.0
Computes daily Net Asset Value per unit after fee accruals.

Formula:
  1. Securities value = sum(units × last_price for all holdings)
  2. Cash = prev_historical.cash + net_cashflows_since_prev + net_others_since_prev
           - fee_withdrawals_since_prev
  3. Gross AUM = securities_value + cash
  4. Base fee   = Gross AUM × base_rate / 365
  5. Perf fee   = Gross AUM × max(0, daily_return − hurdle/365) × perf_rate
  6. Net NTA    = (Gross AUM − daily_fees) / total_units
"""
from datetime import date, timedelta
from typing import Optional, List
import logging

logger = logging.getLogger(__name__)


async def compute_daily_nta(db, target_date: date = None) -> Optional[dict]:
    """
    Compute NTA for a single target_date and upsert to historical.
    Returns computed NTA dict or None if insufficient data.
    """
    if target_date is None:
        target_date = date.today()

    # Skip locked (Excel-imported) rows
    existing = await db.fetchrow(
        "SELECT nta, is_locked FROM historical WHERE date = $1", target_date)
    if existing and existing["is_locked"]:
        logger.info(f"NTA for {target_date} is locked — skipping")
        return {"date": str(target_date), "nta": float(existing["nta"]), "locked": True}

    # Need a previous historical row as the base
    prev = await db.fetchrow(
        "SELECT nta, date, cash, mng_fees, perf_fees "
        "FROM historical WHERE date < $1 ORDER BY date DESC LIMIT 1",
        target_date)
    if not prev:
        logger.warning(f"No previous NTA before {target_date}")
        return None

    prev_nta      = float(prev["nta"])
    prev_date     = prev["date"]
    prev_cash     = float(prev["cash"])
    prev_mng_fees = float(prev["mng_fees"] or 0)
    prev_per_fees = float(prev["perf_fees"] or 0)

    # ── total_units from investors (always current) ─────────────
    units_row = await db.fetchrow(
        "SELECT COALESCE(SUM(units), 0) AS total FROM investors WHERE is_active = TRUE")
    total_units = float(units_row["total"]) if units_row and float(units_row["total"]) > 0 \
                  else 0.0
    if total_units <= 0:
        # fallback to latest historical total_units
        hu = await db.fetchrow(
            "SELECT total_units FROM historical WHERE date < $1 ORDER BY date DESC LIMIT 1",
            target_date)
        total_units = float(hu["total_units"]) if hu else 0.0
    if total_units <= 0:
        logger.warning("total_units is zero — cannot compute NTA")
        return None

    # ── Securities value ────────────────────────────────────────
    holdings = await db.fetch(
        "SELECT instrument, units, last_price FROM holdings WHERE units > 0")
    securities_value = sum(
        float(h["units"]) * float(h["last_price"] or 0)
        for h in holdings if h["last_price"])

    # ── Cash = prev_cash + cashflows since prev + others since prev
    #          - fee_withdrawals since prev ─────────────────────
    cash = prev_cash

    # Net principal cashflows (subscriptions +, redemptions -)
    cf = await db.fetchrow("""
        SELECT COALESCE(SUM(amount), 0) AS net
        FROM principal_cashflows
        WHERE date > $1 AND date <= $2
    """, prev_date, target_date)
    cash += float(cf["net"])

    # Net others income/expense since prev
    try:
        ot = await db.fetchrow("""
            SELECT COALESCE(SUM(amount), 0) AS net
            FROM others
            WHERE record_date > $1 AND record_date <= $2
        """, prev_date, target_date)
        cash += float(ot["net"])
    except Exception:
        pass

    # Fee withdrawals since prev (reduce cash)
    mgmt_withdrawn = 0.0
    perf_withdrawn = 0.0
    try:
        fw = await db.fetch("""
            SELECT fee_type, COALESCE(SUM(amount), 0) AS total
            FROM fee_withdrawals
            WHERE date > $1 AND date <= $2
            GROUP BY fee_type
        """, prev_date, target_date)
        for row in fw:
            if row["fee_type"] == "management":
                mgmt_withdrawn = float(row["total"])
            elif row["fee_type"] == "performance":
                perf_withdrawn = float(row["total"])
        cash -= (mgmt_withdrawn + perf_withdrawn)
    except Exception:
        pass  # fee_withdrawals table may not exist yet

    # ── Gross AUM & return ──────────────────────────────────────
    gross_aum    = securities_value + cash
    gross_nta    = gross_aum / total_units if total_units > 0 else 0
    daily_return = (gross_nta / prev_nta) - 1 if prev_nta > 0 else 0

    # ── Fee accruals ────────────────────────────────────────────
    base_sched = await db.fetchrow("""
        SELECT rate FROM fee_schedules
        WHERE fee_type = 'base'
          AND valid_from <= $1 AND (valid_to IS NULL OR valid_to >= $1)
        ORDER BY valid_from DESC LIMIT 1
    """, target_date)

    perf_sched = await db.fetchrow("""
        SELECT rate, hurdle_rate FROM fee_schedules
        WHERE fee_type = 'performance'
          AND valid_from <= $1 AND (valid_to IS NULL OR valid_to >= $1)
        ORDER BY valid_from DESC LIMIT 1
    """, target_date)

    base_fee = gross_aum * float(base_sched["rate"]) / 365.0 if base_sched else 0.0

    perf_fee = 0.0
    if perf_sched:
        annual_hurdle = float(perf_sched["hurdle_rate"] or 0)
        daily_hurdle  = (1 + annual_hurdle) ** (1/365) - 1
        excess        = max(0.0, daily_return - daily_hurdle)
        perf_fee      = gross_aum * excess * float(perf_sched["rate"])

    total_fee = base_fee + perf_fee

    # ── Net NTA ─────────────────────────────────────────────────
    net_nta = gross_nta - (total_fee / total_units if total_units > 0 else 0)

    # ── Accumulated fee liabilities ─────────────────────────────
    acc_mng  = max(0.0, prev_mng_fees + base_fee - mgmt_withdrawn)
    acc_perf = max(0.0, prev_per_fees + perf_fee - perf_withdrawn)

    # ── Classify securities ─────────────────────────────────────
    sec_vals  = {h["instrument"]: float(h["units"]) * float(h["last_price"] or 0)
                 for h in holdings if h["last_price"]}
    class_map = await db.fetch(
        "SELECT instrument, asset_class FROM ticker_map WHERE instrument = ANY($1)",
        list(sec_vals.keys()))
    cls = {r["instrument"]: r["asset_class"] for r in class_map}

    sec_total  = sum(v for k, v in sec_vals.items() if "Securities"   in cls.get(k, ""))
    reit_total = sum(v for k, v in sec_vals.items() if "Real Estate"  in cls.get(k, ""))
    mm_total   = sum(v for k, v in sec_vals.items() if "Money Market" in cls.get(k, ""))

    # ── Upsert historical ───────────────────────────────────────
    await db.execute("""
        INSERT INTO historical (
            date, securities, reits, money_market, cash,
            mng_fees, perf_fees, capital, earnings, total_units, nta,
            is_locked, source
        ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11, FALSE, 'system')
        ON CONFLICT (date) DO UPDATE SET
            securities   = EXCLUDED.securities,
            reits        = EXCLUDED.reits,
            money_market = EXCLUDED.money_market,
            cash         = EXCLUDED.cash,
            mng_fees     = EXCLUDED.mng_fees,
            perf_fees    = EXCLUDED.perf_fees,
            total_units  = EXCLUDED.total_units,
            nta          = EXCLUDED.nta,
            source       = 'system'
        WHERE historical.is_locked = FALSE
    """,
        target_date,
        round(sec_total, 2), round(reit_total, 2), round(mm_total, 2),
        round(cash, 2), round(acc_mng, 2), round(acc_perf, 2),
        0.0, 0.0, total_units, round(net_nta, 6))

    # Update fund_settings
    await db.execute("""
        UPDATE fund_settings
        SET current_nta = $1, aum = $2, last_nta_date = $3, updated_at = NOW()
        WHERE id = 1
    """, round(net_nta, 6), round(gross_aum - total_fee, 2), target_date)

    logger.info(
        f"NTA {target_date}: net={round(net_nta,6)} "
        f"cash={round(cash,2)} units={round(total_units,4)} "
        f"sec={round(securities_value,2)} "
        f"base_fee={round(base_fee,2)} perf_fee={round(perf_fee,2)}")

    return {
        "date":         str(target_date),
        "gross_nta":    round(gross_nta, 6),
        "net_nta":      round(net_nta, 6),
        "base_fee":     round(base_fee, 2),
        "perf_fee":     round(perf_fee, 2),
        "cash":         round(cash, 2),
        "total_units":  round(total_units, 4),
        "daily_return": round(daily_return * 100, 4),
        "locked":       False,
    }


async def compute_nta_range(db, from_date: date = None, to_date: date = None) -> List[dict]:
    """
    Compute NTA for every calendar day from from_date to to_date (inclusive).
    Skips weekends and locked rows.
    from_date defaults to the day after the last historical record.
    to_date defaults to today.
    """
    if to_date is None:
        to_date = date.today()

    if from_date is None:
        # Start from the day after the last historical record
        last = await db.fetchrow(
            "SELECT MAX(date) AS last_date FROM historical")
        if last and last["last_date"]:
            from_date = last["last_date"] + timedelta(days=1)
        else:
            logger.warning("No historical data — cannot compute range")
            return []

    if from_date > to_date:
        logger.info(f"Historical already up to date ({from_date} > {to_date})")
        return []

    logger.info(f"Computing NTA range: {from_date} → {to_date}")
    results = []
    current = from_date

    while current <= to_date:
        # Skip weekends (Saturday=5, Sunday=6)
        if current.weekday() < 5:
            try:
                result = await compute_daily_nta(db, current)
                if result:
                    results.append(result)
                    logger.info(f"  ✓ {current}: NTA={result.get('net_nta')}")
                else:
                    logger.warning(f"  ✗ {current}: compute returned None")
            except Exception as e:
                logger.error(f"  ✗ {current}: {e}")
        current += timedelta(days=1)

    logger.info(f"Range compute done: {len(results)} days computed")
    return results
