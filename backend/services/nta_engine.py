"""
NTA + Holdings computation engine  v4.0.0

Single compute function that:
  1. Reconstructs portfolio holdings AS OF each date from transactions
  2. Fetches price from price_history for that date (latest on-or-before)
  3. Builds full balance sheet
  4. Writes to historical
  5. Updates compute_status to clean for that date

Triggered by: earliest dirty date in compute_status → recomputes forward to today.
Dirty dates are set automatically by SQL triggers on all input tables.

Balance Sheet:
  ASSETS
    derivatives   = Derivative/Warrant holdings × price
    securities    = Securities [H/M/L] holdings × price
    reits         = Real Estate holdings × price
    bonds         = Bond holdings × price
    money_market  = Money Market holdings at cost (no live price)
    receivables   = stock dividends: ex_date <= D < pmt_date
    cash          = prev_cash
                  + principal_cashflows on D
                  + dividend payments received on D (pmt_date = D)
                  + others income on D
                  - fee_withdrawals on D

  LIABILITIES
    mng_fees      = prev_mng_fees + daily_accrual - mgmt_withdrawn_on_D
    perf_fees     = prev_perf_fees + daily_accrual - perf_withdrawn_on_D
    ints_on_fees  = carried forward
    loans         = carried forward
    ints_on_loans = carried forward

  EQUITY
    capital  = SUM(all principal_cashflows up to D)
    earnings = (total_assets - total_liabilities) - capital

  NTA = (total_assets - total_liabilities) / total_units_on_D
"""
from datetime import date, timedelta
from decimal import Decimal
from typing import Optional, List
import logging

logger = logging.getLogger(__name__)


# ── Portfolio reconstruction helper ──────────────────────────
async def _portfolio_as_of(db, as_of_date: date) -> List[dict]:
    """
    Reconstruct portfolio positions as of as_of_date from transactions.
    Returns list of {instrument, asset_class, units, avg_cost, total_cost}.
    Uses VWAP for average cost.
    """
    rows = await db.fetch("""
        SELECT date, instrument,
               COALESCE(tm.asset_class, 'Securities [H]') AS asset_class,
               units, price, net_amount, total_fees
        FROM transactions t
        LEFT JOIN ticker_map tm ON tm.instrument = t.instrument
        WHERE t.date <= $1
        ORDER BY t.date ASC, t.created_at ASC
    """, as_of_date)

    positions = {}  # instrument → {units, total_cost}

    for r in rows:
        instr  = r["instrument"]
        units  = float(r["units"])           # negative for sells
        net_am = float(r["net_amount"])      # negative for buys (cash out)
        ac     = r["asset_class"]

        if instr not in positions:
            positions[instr] = {"units": 0.0, "total_cost": 0.0, "asset_class": ac}

        p = positions[instr]

        if units > 0:    # BUY — add units and cost
            p["total_cost"] += abs(net_am)
            p["units"]      += units
        else:            # SELL — reduce proportionally (VWAP basis)
            sell_units = abs(units)
            if p["units"] > 0:
                ratio           = sell_units / p["units"]
                p["total_cost"] = p["total_cost"] * (1 - ratio)
            p["units"] += units  # units is negative

    # Filter out zero/negative positions
    return [
        {
            "instrument":  k,
            "asset_class": v["asset_class"],
            "units":       v["units"],
            "avg_cost":    v["total_cost"] / v["units"] if v["units"] > 0 else 0,
            "total_cost":  v["total_cost"],
        }
        for k, v in positions.items()
        if v["units"] > 0.0001
    ]


async def _get_price(db, instrument: str, as_of_date: date) -> float:
    """Get latest price on or before as_of_date from price_history."""
    row = await db.fetchrow("""
        SELECT price FROM price_history
        WHERE instrument = $1 AND date <= $2
        ORDER BY date DESC LIMIT 1
    """, instrument, as_of_date)
    return float(row["price"]) if row else 0.0


# ── Main compute function ─────────────────────────────────────
async def compute_daily_nta(db, target_date: date) -> Optional[dict]:
    """
    Compute full balance sheet NTA for target_date.
    Rebuilds portfolio from transactions as-of target_date.
    Writes result to historical and marks compute_status clean.
    """
    # Skip locked rows (Excel imports)
    existing = await db.fetchrow(
        "SELECT nta, is_locked FROM historical WHERE date = $1", target_date)
    if existing and existing["is_locked"]:
        await _mark_clean(db, target_date)
        return {"date": str(target_date), "nta": float(existing["nta"]), "locked": True}

    # Need previous row for carried-forward values
    prev = await db.fetchrow(
        "SELECT * FROM historical WHERE date < $1 ORDER BY date DESC LIMIT 1",
        target_date)
    if not prev:
        logger.warning(f"No previous historical row before {target_date} — cannot compute")
        return None

    prev_date   = prev["date"]
    prev_cash   = float(prev["cash"])
    prev_mng    = float(prev["mng_fees"]     or 0)
    prev_perf   = float(prev["perf_fees"]    or 0)
    prev_i_fees = float(prev["ints_on_fees"] or 0)
    prev_loans  = float(prev["loans"]        or 0)
    prev_i_lns  = float(prev["ints_on_loans"]or 0)
    prev_nta    = float(prev["nta"])

    # ── total_units on target_date ────────────────────────────
    ur = await db.fetchrow(
        "SELECT COALESCE(SUM(units),0) AS t FROM investors WHERE is_active=TRUE")
    total_units = float(ur["t"]) if ur and float(ur["t"]) > 0 \
                  else float(prev["total_units"])
    if total_units <= 0:
        logger.warning(f"total_units is zero for {target_date}")
        return None

    # ── Reconstruct portfolio as of target_date ───────────────
    portfolio = await _portfolio_as_of(db, target_date)

    derivatives  = 0.0
    securities   = 0.0
    reits        = 0.0
    bonds        = 0.0
    money_market = 0.0

    for pos in portfolio:
        ac    = pos["asset_class"]
        units = pos["units"]
        cost  = pos["total_cost"]

        if "Money Market" in ac:
            money_market += cost                          # at cost, no live price
        else:
            price = await _get_price(db, pos["instrument"], target_date)
            val   = units * price
            if   "Derivative" in ac or "Warrant" in ac:  derivatives += val
            elif "Securities"  in ac:                     securities  += val
            elif "Real Estate" in ac or "REIT" in ac:    reits       += val
            elif "Bond"        in ac:                     bonds       += val
            else:                                         securities  += val  # fallback

    # ── Receivables ───────────────────────────────────────────
    # Accrues on ex_date, clears when payment received (pmt_date)
    rec = await db.fetchrow("""
        SELECT COALESCE(SUM(amount), 0) AS total
        FROM dividends
        WHERE ex_date  <= $1
          AND (pmt_date IS NULL OR pmt_date > $1)
    """, target_date)
    receivables = float(rec["total"])

    # ── Cash ──────────────────────────────────────────────────
    cash = prev_cash

    # Principal cashflows ON target_date only (not since prev — each day handles its own)
    cf = await db.fetchrow("""
        SELECT COALESCE(SUM(amount), 0) AS net
        FROM principal_cashflows WHERE date = $1
    """, target_date)
    cash += float(cf["net"])

    # Dividend payments received ON target_date (pmt_date = today → cash in)
    dp = await db.fetchrow("""
        SELECT COALESCE(SUM(amount), 0) AS net
        FROM dividends WHERE pmt_date = $1
    """, target_date)
    cash += float(dp["net"])

    # Others income/expense ON target_date
    try:
        ot = await db.fetchrow("""
            SELECT COALESCE(SUM(amount), 0) AS net
            FROM others WHERE record_date = $1
        """, target_date)
        cash += float(ot["net"])
    except Exception:
        pass

    # Fee withdrawals ON target_date (reduce cash)
    mgmt_withdrawn = 0.0
    perf_withdrawn = 0.0
    try:
        fw = await db.fetch("""
            SELECT fee_type, COALESCE(SUM(amount), 0) AS total
            FROM fee_withdrawals WHERE date = $1
            GROUP BY fee_type
        """, target_date)
        for row in fw:
            if row["fee_type"] == "management":  mgmt_withdrawn = float(row["total"])
            else:                                 perf_withdrawn = float(row["total"])
        cash -= (mgmt_withdrawn + perf_withdrawn)
    except Exception:
        pass

    # ── Total assets ──────────────────────────────────────────
    total_assets = derivatives + securities + reits + bonds + money_market + receivables + cash

    # ── Fee accruals ──────────────────────────────────────────
    gross_nta    = total_assets / total_units if total_units > 0 else 0
    daily_return = (gross_nta / prev_nta) - 1 if prev_nta > 0 else 0

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

    base_fee = total_assets * float(base_sched["rate"]) / 365.0 if base_sched else 0.0
    perf_fee = 0.0
    if perf_sched:
        hurdle   = float(perf_sched["hurdle_rate"] or 0)
        d_hurdle = (1 + hurdle) ** (1/365) - 1
        excess   = max(0.0, daily_return - d_hurdle)
        perf_fee = total_assets * excess * float(perf_sched["rate"])

    # Accumulated liabilities
    acc_mng   = max(0.0, prev_mng   + base_fee - mgmt_withdrawn)
    acc_perf  = max(0.0, prev_perf  + perf_fee - perf_withdrawn)
    total_liab = acc_mng + acc_perf + prev_i_fees + prev_loans + prev_i_lns

    # ── Capital & Earnings ────────────────────────────────────
    cap_row = await db.fetchrow(
        "SELECT COALESCE(SUM(amount), 0) AS total FROM principal_cashflows WHERE date <= $1",
        target_date)
    capital  = float(cap_row["total"])
    nav      = total_assets - total_liab
    earnings = nav - capital
    net_nta  = nav / total_units if total_units > 0 else 0

    # ── Upsert historical ─────────────────────────────────────
    await db.execute("""
        INSERT INTO historical (
            date, derivatives, securities, reits, bonds, money_market,
            receivables, cash, mng_fees, perf_fees, ints_on_fees,
            loans, ints_on_loans, capital, earnings, total_units, nta,
            is_locked, source
        ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,FALSE,'system')
        ON CONFLICT (date) DO UPDATE SET
            derivatives   = EXCLUDED.derivatives,
            securities    = EXCLUDED.securities,
            reits         = EXCLUDED.reits,
            bonds         = EXCLUDED.bonds,
            money_market  = EXCLUDED.money_market,
            receivables   = EXCLUDED.receivables,
            cash          = EXCLUDED.cash,
            mng_fees      = EXCLUDED.mng_fees,
            perf_fees     = EXCLUDED.perf_fees,
            ints_on_fees  = EXCLUDED.ints_on_fees,
            loans         = EXCLUDED.loans,
            ints_on_loans = EXCLUDED.ints_on_loans,
            capital       = EXCLUDED.capital,
            earnings      = EXCLUDED.earnings,
            total_units   = EXCLUDED.total_units,
            nta           = EXCLUDED.nta,
            source        = 'system'
        WHERE historical.is_locked = FALSE
    """,
        target_date,
        round(derivatives,2), round(securities,2), round(reits,2),
        round(bonds,2),       round(money_market,2), round(receivables,2),
        round(cash,2),        round(acc_mng,2),      round(acc_perf,2),
        round(prev_i_fees,2), round(prev_loans,2),   round(prev_i_lns,2),
        round(capital,2),     round(earnings,2),
        total_units,          round(net_nta, 6))

    # Update fund_settings with latest
    await db.execute("""
        UPDATE fund_settings
        SET current_nta = $1, aum = $2, last_nta_date = $3, updated_at = NOW()
        WHERE id = 1
    """, round(net_nta, 6), round(nav, 2), target_date)

    # Mark clean
    await _mark_clean(db, target_date)

    logger.info(
        f"NTA {target_date}: {round(net_nta,6)} | "
        f"assets={round(total_assets,2)} sec={round(securities,2)} "
        f"mm={round(money_market,2)} recv={round(receivables,2)} "
        f"cash={round(cash,2)} cap={round(capital,2)} earn={round(earnings,2)}")

    return {
        "date":         str(target_date),
        "net_nta":      round(net_nta, 6),
        "total_assets": round(total_assets, 2),
        "securities":   round(securities, 2),
        "money_market": round(money_market, 2),
        "receivables":  round(receivables, 2),
        "cash":         round(cash, 2),
        "capital":      round(capital, 2),
        "earnings":     round(earnings, 2),
        "total_units":  round(total_units, 4),
        "base_fee":     round(base_fee, 2),
        "perf_fee":     round(perf_fee, 2),
        "locked":       False,
    }


async def _mark_clean(db, d: date):
    try:
        await db.execute("""
            INSERT INTO compute_status (date, is_dirty, reason, updated_at)
            VALUES ($1, FALSE, 'computed', NOW())
            ON CONFLICT (date) DO UPDATE
                SET is_dirty = FALSE, reason = 'computed', updated_at = NOW()
        """, d)
    except Exception:
        pass  # table may not exist yet


# ── Range compute: from earliest dirty date forward ──────────
async def compute_from_dirty(db, force_from: date = None) -> dict:
    """
    Find earliest dirty date in compute_status (or force_from),
    then compute every calendar day from there to today.
    Each day uses only data ON that date for cashflows (not cumulative window).
    """
    to_date = date.today()

    if force_from:
        from_date = force_from
    else:
        try:
            dirty = await db.fetchrow("""
                SELECT MIN(date) AS earliest
                FROM compute_status
                WHERE is_dirty = TRUE
            """)
            if dirty and dirty["earliest"]:
                from_date = dirty["earliest"]
            else:
                # No dirty dates — find first uncomputed date after last historical
                last = await db.fetchrow(
                    "SELECT MAX(date) AS d FROM historical")
                from_date = (last["d"] + timedelta(days=1)) if last and last["d"] else None
        except Exception:
            # compute_status table doesn't exist yet — recompute from last historical
            last = await db.fetchrow("SELECT MAX(date) AS d FROM historical")
            from_date = (last["d"] + timedelta(days=1)) if last and last["d"] else None

    if not from_date:
        return {"computed": 0, "message": "Nothing to compute"}
    if from_date > to_date:
        return {"computed": 0, "message": f"Already up to date ({from_date})"}

    logger.info(f"Computing from {from_date} → {to_date}")
    computed = 0
    errors   = 0
    current  = from_date

    while current <= to_date:
        try:
            r = await compute_daily_nta(db, current)
            if r:
                computed += 1
        except Exception as e:
            logger.error(f"  ✗ {current}: {e}")
            errors += 1
        current += timedelta(days=1)

    logger.info(f"Done: {computed} computed, {errors} errors")
    return {
        "computed":  computed,
        "errors":    errors,
        "from":      str(from_date),
        "to":        str(to_date),
    }
