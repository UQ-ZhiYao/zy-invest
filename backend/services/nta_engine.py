"""
NTA computation engine  v3.0.0

Balance Sheet:
  ASSETS
    derivatives   = holdings with asset_class LIKE 'Derivative%'     (mark-to-market)
    securities    = holdings with asset_class LIKE 'Securities%'      (all H, M, L)
    reits         = holdings with asset_class LIKE 'Real Estate%'
    bonds         = holdings with asset_class LIKE 'Bond%'
    money_market  = holdings with asset_class LIKE 'Money Market%'    (at cost price)
    receivables   = dividends where ex_date <= target_date AND (pmt_date IS NULL OR pmt_date > target_date)
    cash          = prev_cash + net_principal_cashflows + net_others
                  + dividend payments received (pmt_date <= target_date, paid since prev)
                  - fee_withdrawals since prev

  LIABILITIES
    mng_fees      = accumulated management fee accruals - withdrawals
    perf_fees     = accumulated performance fee accruals - withdrawals
    ints_on_fees  = (carried forward, not computed here)
    loans         = (carried forward)
    ints_on_loans = (carried forward)

  EQUITY
    capital       = SUM(principal_cashflows.amount) — all subscriptions minus redemptions
    earnings      = total_assets - total_liabilities - capital

  NTA = (total_assets - total_liabilities) / total_units
"""
from datetime import date, timedelta
from typing import Optional, List
import logging

logger = logging.getLogger(__name__)


async def compute_daily_nta(db, target_date: date = None) -> Optional[dict]:
    if target_date is None:
        target_date = date.today()

    # Skip locked rows
    existing = await db.fetchrow(
        "SELECT nta, is_locked FROM historical WHERE date = $1", target_date)
    if existing and existing["is_locked"]:
        return {"date": str(target_date), "nta": float(existing["nta"]), "locked": True}

    # Need previous row as base for accumulated values
    prev = await db.fetchrow(
        "SELECT * FROM historical WHERE date < $1 ORDER BY date DESC LIMIT 1",
        target_date)
    if not prev:
        logger.warning(f"No previous historical row before {target_date}")
        return None

    prev_date     = prev["date"]
    prev_cash     = float(prev["cash"])
    prev_mng      = float(prev["mng_fees"]     or 0)
    prev_perf     = float(prev["perf_fees"]    or 0)
    prev_i_fees   = float(prev["ints_on_fees"] or 0)
    prev_loans    = float(prev["loans"]        or 0)
    prev_i_loans  = float(prev["ints_on_loans"]or 0)
    prev_nta      = float(prev["nta"])

    # ── total_units ──────────────────────────────────────────────
    ur = await db.fetchrow(
        "SELECT COALESCE(SUM(units),0) AS t FROM investors WHERE is_active=TRUE")
    total_units = float(ur["t"]) if ur and float(ur["t"]) > 0 else float(prev["total_units"])
    if total_units <= 0:
        logger.warning("total_units is zero")
        return None

    # ── Holdings grouped by asset class ─────────────────────────
    holdings = await db.fetch("""
        SELECT h.instrument, h.units, h.last_price, h.total_costs,
               COALESCE(tm.asset_class, 'Securities [H]') AS asset_class
        FROM holdings h
        LEFT JOIN ticker_map tm ON tm.instrument = h.instrument
        WHERE h.units > 0
    """)

    derivatives  = 0.0
    securities   = 0.0   # all Securities [H], [M], [L]
    reits        = 0.0
    bonds        = 0.0
    money_market = 0.0

    for h in holdings:
        ac    = h["asset_class"] or ""
        units = float(h["units"])
        price = float(h["last_price"] or 0)
        cost  = float(h["total_costs"] or 0)

        if "Derivative" in ac or "Warrant" in ac:
            derivatives  += units * price
        elif "Securities" in ac:
            securities   += units * price          # mark-to-market
        elif "Real Estate" in ac or "REIT" in ac:
            reits        += units * price
        elif "Bond" in ac or "Fixed Income" in ac:
            bonds        += units * price
        elif "Money Market" in ac:
            money_market += cost                   # at cost price (no live data)

    # ── Receivables ──────────────────────────────────────────────
    # Accrued when ex_date reached; cleared when dividend payment arrives
    rec_rows = await db.fetch("""
        SELECT SUM(amount) AS total
        FROM dividends
        WHERE ex_date  <= $1
          AND (pmt_date IS NULL OR pmt_date > $1)
    """, target_date)
    receivables = float(rec_rows[0]["total"] or 0) if rec_rows else 0.0

    # ── Cash ─────────────────────────────────────────────────────
    cash = prev_cash

    # Net principal cashflows since prev (subscriptions +, redemptions -)
    cf = await db.fetchrow("""
        SELECT COALESCE(SUM(amount), 0) AS net
        FROM principal_cashflows
        WHERE date > $1 AND date <= $2
    """, prev_date, target_date)
    cash += float(cf["net"])

    # Dividend payments received since prev (pmt_date in window → cash in, receivable out)
    dp = await db.fetchrow("""
        SELECT COALESCE(SUM(amount), 0) AS net
        FROM dividends
        WHERE pmt_date > $1 AND pmt_date <= $2
    """, prev_date, target_date)
    cash += float(dp["net"])

    # Others income/expense since prev
    try:
        ot = await db.fetchrow("""
            SELECT COALESCE(SUM(amount), 0) AS net
            FROM others
            WHERE record_date > $1 AND record_date <= $2
        """, prev_date, target_date)
        cash += float(ot["net"])
    except Exception:
        pass

    # Fee withdrawals reduce cash since prev
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
            else:
                perf_withdrawn = float(row["total"])
        cash -= (mgmt_withdrawn + perf_withdrawn)
    except Exception:
        pass

    # ── Total assets ─────────────────────────────────────────────
    total_assets = derivatives + securities + reits + bonds + money_market + receivables + cash

    # ── Fee accruals ─────────────────────────────────────────────
    gross_aum    = total_assets
    gross_nta    = gross_aum / total_units if total_units > 0 else 0
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

    base_fee = gross_aum * float(base_sched["rate"]) / 365.0 if base_sched else 0.0
    perf_fee = 0.0
    if perf_sched:
        hurdle    = float(perf_sched["hurdle_rate"] or 0)
        d_hurdle  = (1 + hurdle) ** (1/365) - 1
        excess    = max(0.0, daily_return - d_hurdle)
        perf_fee  = gross_aum * excess * float(perf_sched["rate"])

    # Accumulated liabilities
    acc_mng   = max(0.0, prev_mng  + base_fee - mgmt_withdrawn)
    acc_perf  = max(0.0, prev_perf + perf_fee - perf_withdrawn)
    # Carry forward other liabilities unchanged
    acc_i_fees  = prev_i_fees
    acc_loans   = prev_loans
    acc_i_loans = prev_i_loans

    total_liab = acc_mng + acc_perf + acc_i_fees + acc_loans + acc_i_loans

    # ── Capital = sum of all principal cashflows ever ─────────────
    cap_row = await db.fetchrow(
        "SELECT COALESCE(SUM(amount), 0) AS total FROM principal_cashflows")
    capital = float(cap_row["total"])

    # ── Earnings = NAV - Capital ─────────────────────────────────
    nav      = total_assets - total_liab
    earnings = nav - capital

    # ── Net NTA ──────────────────────────────────────────────────
    total_fee = base_fee + perf_fee
    net_nta   = (gross_aum - total_fee) / total_units if total_units > 0 else 0

    # ── Upsert historical ─────────────────────────────────────────
    await db.execute("""
        INSERT INTO historical (
            date, derivatives, securities, reits, bonds, money_market,
            receivables, cash, mng_fees, perf_fees, ints_on_fees,
            loans, ints_on_loans, capital, earnings, total_units, nta,
            is_locked, source
        ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,FALSE,'system')
        ON CONFLICT (date) DO UPDATE SET
            derivatives  = EXCLUDED.derivatives,
            securities   = EXCLUDED.securities,
            reits        = EXCLUDED.reits,
            bonds        = EXCLUDED.bonds,
            money_market = EXCLUDED.money_market,
            receivables  = EXCLUDED.receivables,
            cash         = EXCLUDED.cash,
            mng_fees     = EXCLUDED.mng_fees,
            perf_fees    = EXCLUDED.perf_fees,
            ints_on_fees = EXCLUDED.ints_on_fees,
            loans        = EXCLUDED.loans,
            ints_on_loans= EXCLUDED.ints_on_loans,
            capital      = EXCLUDED.capital,
            earnings     = EXCLUDED.earnings,
            total_units  = EXCLUDED.total_units,
            nta          = EXCLUDED.nta,
            source       = 'system'
        WHERE historical.is_locked = FALSE
    """,
        target_date,
        round(derivatives, 2), round(securities, 2), round(reits, 2),
        round(bonds, 2), round(money_market, 2), round(receivables, 2),
        round(cash, 2), round(acc_mng, 2), round(acc_perf, 2),
        round(acc_i_fees, 2), round(acc_loans, 2), round(acc_i_loans, 2),
        round(capital, 2), round(earnings, 2),
        total_units, round(net_nta, 6))

    await db.execute("""
        UPDATE fund_settings
        SET current_nta = $1, aum = $2, last_nta_date = $3, updated_at = NOW()
        WHERE id = 1
    """, round(net_nta, 6), round(nav, 2), target_date)

    logger.info(
        f"NTA {target_date}: nta={round(net_nta,6)} "
        f"assets={round(total_assets,2)} liab={round(total_liab,2)} "
        f"cap={round(capital,2)} earn={round(earnings,2)} "
        f"cash={round(cash,2)} sec={round(securities,2)} "
        f"mm={round(money_market,2)} recv={round(receivables,2)}")

    return {
        "date":         str(target_date),
        "net_nta":      round(net_nta, 6),
        "gross_nta":    round(gross_nta, 6),
        "total_assets": round(total_assets, 2),
        "securities":   round(securities, 2),
        "money_market": round(money_market, 2),
        "receivables":  round(receivables, 2),
        "cash":         round(cash, 2),
        "capital":      round(capital, 2),
        "earnings":     round(earnings, 2),
        "base_fee":     round(base_fee, 2),
        "perf_fee":     round(perf_fee, 2),
        "total_units":  round(total_units, 4),
        "locked":       False,
    }


async def compute_nta_range(db, from_date: date = None, to_date: date = None) -> List[dict]:
    """Compute NTA for every weekday from from_date to to_date."""
    if to_date is None:
        to_date = date.today()
    if from_date is None:
        last = await db.fetchrow("SELECT MAX(date) AS d FROM historical")
        if last and last["d"]:
            from_date = last["d"] + timedelta(days=1)
        else:
            return []

    if from_date > to_date:
        return []

    logger.info(f"NTA range: {from_date} → {to_date}")
    results = []
    current = from_date
    while current <= to_date:
        if current.weekday() < 5:   # Mon–Fri only
            try:
                r = await compute_daily_nta(db, current)
                if r:
                    results.append(r)
            except Exception as e:
                logger.error(f"  ✗ {current}: {e}")
        current += timedelta(days=1)

    logger.info(f"Range done: {len(results)} days computed")
    return results
