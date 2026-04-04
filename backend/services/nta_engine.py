"""
Holdings + NTA Computation Engine  v5.0.0
==========================================
Single super-function that does everything:

  1. Scan 6 input DBs for earliest is_computed=FALSE → date (a)
  2. c = min(a - 1 day, last_historical_date)
  3. Reconstruct holdings portfolio as-of date c from ALL transactions <= c
  4. Loop d = c+1 → today:
       a. Apply trades on d  (BUY: add units+cost, SELL: reduce VWAP)
       b. Update cash:
            + principal_cashflows on d (subscriptions net of redemptions)
            + dividends.pmt_date = d   (dividend cash received)
            + others.record_date = d   (interest, misc)
            - fee_withdrawals.date = d (fees paid out)
            - distributions.pmt_date = d (fund payouts to investors)
       c. Receivables: dividends.ex_date = d → add to receivables balance
                       dividends.pmt_date = d → remove from receivables
       d. Get prices from price_history (latest on or before d)
       e. Build balance sheet, write to historical
       f. Mark all records on d as is_computed = TRUE
  5. Update holdings table with final current snapshot

Holdings table = current fund portfolio snapshot (no investor_id).
Cash is stored in holdings as a single row with instrument = '__CASH__'.
"""
from datetime import date, timedelta
from decimal import Decimal, getcontext
from typing import Optional, Dict
import logging

logger = logging.getLogger(__name__)
getcontext().prec = 28

D = lambda v: Decimal('0') if v is None else Decimal(str(v))


# ─────────────────────────────────────────────────────────────
# Internal state passed through the loop
# ─────────────────────────────────────────────────────────────
class PortfolioState:
    def __init__(self):
        # positions[instrument] = {units, total_cost, avg_cost, asset_class}
        self.positions: Dict[str, dict] = {}
        self.cash        = Decimal('0')
        self.receivables = Decimal('0')
        self.prev_mng    = Decimal('0')
        self.prev_perf   = Decimal('0')
        self.prev_i_fees = Decimal('0')
        self.prev_loans  = Decimal('0')
        self.prev_i_lns  = Decimal('0')
        self.prev_nta    = Decimal('1')

    def apply_buy(self, instrument: str, units: Decimal,
                  net_cost: Decimal, asset_class: str):
        if instrument not in self.positions:
            self.positions[instrument] = {
                'units': Decimal('0'), 'total_cost': Decimal('0'),
                'avg_cost': Decimal('0'), 'asset_class': asset_class}
        p = self.positions[instrument]
        p['total_cost'] += net_cost
        p['units']      += units
        p['avg_cost']    = p['total_cost'] / p['units'] if p['units'] > 0 else net_cost / units
        p['asset_class'] = asset_class

    def apply_sell(self, instrument: str, units_sold: Decimal):
        if instrument not in self.positions:
            return
        p = self.positions[instrument]
        if p['units'] <= 0:
            return
        ratio           = units_sold / p['units']
        p['total_cost'] = p['total_cost'] * (1 - ratio)
        p['units']     -= units_sold
        if p['units'] < Decimal('0.0001'):
            p['units'] = Decimal('0')
            p['total_cost'] = Decimal('0')


# ─────────────────────────────────────────────────────────────
# Helper: get price for instrument on or before a date
# ─────────────────────────────────────────────────────────────
async def _price(db, instrument: str, as_of: date) -> Decimal:
    row = await db.fetchrow("""
        SELECT price FROM price_history
        WHERE instrument = $1 AND date <= $2
        ORDER BY date DESC LIMIT 1
    """, instrument, as_of)
    return D(row['price']) if row else Decimal('0')


# ─────────────────────────────────────────────────────────────
# Helper: build portfolio state as-of a given date
# ─────────────────────────────────────────────────────────────
async def _build_state_as_of(db, as_of: date, hist_row) -> PortfolioState:
    """Reconstruct portfolio from all transactions up to as_of."""
    state = PortfolioState()

    # Carry forward from historical row
    state.cash        = D(hist_row['cash'])
    state.receivables = D(hist_row['receivables'])
    state.prev_mng    = D(hist_row['mng_fees'])
    state.prev_perf   = D(hist_row['perf_fees'])
    state.prev_i_fees = D(hist_row['ints_on_fees'])
    state.prev_loans  = D(hist_row['loans'])
    state.prev_i_lns  = D(hist_row['ints_on_loans'])
    state.prev_nta    = D(hist_row['nta'])

    # Rebuild positions from all transactions up to as_of
    trades = await db.fetch("""
        SELECT date, instrument, units, net_amount,
               COALESCE(asset_class, 'Securities [H]') AS asset_class
        FROM transactions
        WHERE date <= $1
        ORDER BY date ASC, created_at ASC
    """, as_of)

    for t in trades:
        units = D(t['units'])
        net   = D(t['net_amount'])
        ac    = t['asset_class']
        instr = t['instrument']
        if units > 0:
            # BUY — net_amount is negative (cash out), abs = cost paid
            state.apply_buy(instr, units, abs(net), ac)
        elif units < 0:
            state.apply_sell(instr, abs(units))

    return state


# ─────────────────────────────────────────────────────────────
# One day: apply changes, compute balance sheet, write historical
# ─────────────────────────────────────────────────────────────
async def _process_day(db, d: date, state: PortfolioState, total_units: Decimal) -> Optional[dict]:

    # ── a. Apply trades on d ─────────────────────────────────
    trades = await db.fetch("""
        SELECT instrument, units, net_amount,
               COALESCE(asset_class, 'Securities [H]') AS asset_class
        FROM transactions WHERE date = $1
        ORDER BY created_at ASC
    """, d)
    for t in trades:
        units = D(t['units'])
        net   = D(t['net_amount'])
        ac    = t['asset_class']
        if units > 0:
            state.apply_buy(t['instrument'], units, abs(net), ac)
        elif units < 0:
            state.apply_sell(t['instrument'], abs(units))

    # ── b. Cash movements on d ───────────────────────────────
    # Principal cashflows (subscriptions +, redemptions -)
    cf = await db.fetchrow(
        "SELECT COALESCE(SUM(amount),0) AS n FROM principal_cashflows WHERE date=$1", d)
    state.cash += D(cf['n'])

    # Dividends received (pmt_date = d)
    dp = await db.fetchrow(
        "SELECT COALESCE(SUM(amount),0) AS n FROM dividends WHERE pmt_date=$1", d)
    state.cash += D(dp['n'])

    # Others income/expense
    try:
        ot = await db.fetchrow(
            "SELECT COALESCE(SUM(amount),0) AS n FROM others WHERE record_date=$1", d)
        state.cash += D(ot['n'])
    except Exception:
        pass

    # Fee withdrawals (cash out)
    mgmt_w = Decimal('0')
    perf_w = Decimal('0')
    try:
        fw = await db.fetch("""
            SELECT fee_type, COALESCE(SUM(amount),0) AS n
            FROM fee_withdrawals WHERE date=$1 GROUP BY fee_type
        """, d)
        for row in fw:
            if row['fee_type'] == 'management': mgmt_w = D(row['n'])
            else:                               perf_w = D(row['n'])
        state.cash -= (mgmt_w + perf_w)
    except Exception:
        pass

    # Fund distributions paid to investors (cash out)
    try:
        dist = await db.fetchrow(
            "SELECT COALESCE(SUM(total_dividend),0) AS n FROM distributions WHERE pmt_date=$1", d)
        state.cash -= D(dist['n'])
    except Exception:
        pass

    # ── c. Receivables ───────────────────────────────────────
    # Add: dividends that went ex today
    ex_today = await db.fetchrow(
        "SELECT COALESCE(SUM(amount),0) AS n FROM dividends WHERE ex_date=$1", d)
    state.receivables += D(ex_today['n'])
    # Remove: dividends paid today (now cash)
    state.receivables -= D(dp['n'])
    if state.receivables < 0:
        state.receivables = Decimal('0')

    # ── d. Asset values ──────────────────────────────────────
    derivatives  = Decimal('0')
    securities   = Decimal('0')
    reits        = Decimal('0')
    bonds        = Decimal('0')
    money_market = Decimal('0')

    for instr, pos in state.positions.items():
        if pos['units'] <= Decimal('0.0001'):
            continue
        ac = pos['asset_class']
        if 'Money Market' in ac:
            money_market += pos['total_cost']  # at cost, no live price
        else:
            price = await _price(db, instr, d)
            val   = pos['units'] * price
            if   'Derivative' in ac or 'Warrant' in ac: derivatives += val
            elif 'Securities'  in ac:                    securities  += val
            elif 'Real Estate' in ac or 'REIT' in ac:   reits       += val
            elif 'Bond'        in ac:                    bonds       += val
            else:                                        securities  += val

    total_assets = derivatives + securities + reits + bonds + \
                   money_market + state.receivables + state.cash

    # ── e. Fees & liabilities ────────────────────────────────
    gross_nta    = total_assets / total_units if total_units > 0 else Decimal('0')
    daily_return = (gross_nta / state.prev_nta) - 1 if state.prev_nta > 0 else Decimal('0')

    base_sched = await db.fetchrow("""
        SELECT rate FROM fee_schedules
        WHERE fee_type='base' AND valid_from<=$1 AND (valid_to IS NULL OR valid_to>=$1)
        ORDER BY valid_from DESC LIMIT 1
    """, d)
    perf_sched = await db.fetchrow("""
        SELECT rate, hurdle_rate FROM fee_schedules
        WHERE fee_type='performance' AND valid_from<=$1 AND (valid_to IS NULL OR valid_to>=$1)
        ORDER BY valid_from DESC LIMIT 1
    """, d)

    base_fee = total_assets * D(base_sched['rate']) / 365 if base_sched else Decimal('0')
    perf_fee = Decimal('0')
    if perf_sched:
        hurdle   = D(perf_sched['hurdle_rate'] or 0)
        d_hurdle = (1 + hurdle) ** (Decimal('1')/365) - 1
        excess   = max(Decimal('0'), daily_return - d_hurdle)
        perf_fee = total_assets * excess * D(perf_sched['rate'])

    acc_mng   = max(Decimal('0'), state.prev_mng  + base_fee - mgmt_w)
    acc_perf  = max(Decimal('0'), state.prev_perf + perf_fee - perf_w)
    total_liab = acc_mng + acc_perf + state.prev_i_fees + state.prev_loans + state.prev_i_lns

    # Capital = SUM of all principal cashflows up to d
    cap_row  = await db.fetchrow(
        "SELECT COALESCE(SUM(amount),0) AS n FROM principal_cashflows WHERE date<=$1", d)
    capital  = D(cap_row['n'])
    nav      = total_assets - total_liab
    earnings = nav - capital
    net_nta  = nav / total_units if total_units > 0 else Decimal('0')

    # ── Write historical ─────────────────────────────────────
    def r2(v): return float(v.quantize(Decimal('0.01')))
    def r6(v): return float(v.quantize(Decimal('0.000001')))

    await db.execute("""
        INSERT INTO historical (
            date, derivatives, securities, reits, bonds, money_market,
            receivables, cash, mng_fees, perf_fees, ints_on_fees,
            loans, ints_on_loans, capital, earnings, total_units, nta,
            is_locked, source
        ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,FALSE,'system')
        ON CONFLICT (date) DO UPDATE SET
            derivatives=EXCLUDED.derivatives, securities=EXCLUDED.securities,
            reits=EXCLUDED.reits, bonds=EXCLUDED.bonds,
            money_market=EXCLUDED.money_market, receivables=EXCLUDED.receivables,
            cash=EXCLUDED.cash, mng_fees=EXCLUDED.mng_fees,
            perf_fees=EXCLUDED.perf_fees, ints_on_fees=EXCLUDED.ints_on_fees,
            loans=EXCLUDED.loans, ints_on_loans=EXCLUDED.ints_on_loans,
            capital=EXCLUDED.capital, earnings=EXCLUDED.earnings,
            total_units=EXCLUDED.total_units, nta=EXCLUDED.nta, source='system'
        WHERE historical.is_locked=FALSE
    """,
        d,
        r2(derivatives), r2(securities), r2(reits), r2(bonds),
        r2(money_market), r2(state.receivables), r2(state.cash),
        r2(acc_mng), r2(acc_perf), r2(state.prev_i_fees),
        r2(state.prev_loans), r2(state.prev_i_lns),
        r2(capital), r2(earnings), float(total_units), r6(net_nta))

    await db.execute("""
        UPDATE fund_settings
        SET current_nta=$1, aum=$2, last_nta_date=$3, updated_at=NOW()
        WHERE id=1
    """, r6(net_nta), r2(nav), d)

    # ── f. Mark records on d as computed ─────────────────────
    for tbl, col in [
        ('transactions',        'date'),
        ('principal_cashflows', 'date'),
        ('dividends',           'ex_date'),
        ('others',              'record_date'),
        ('fee_withdrawals',     'date'),
        ('distributions',       'pmt_date'),
    ]:
        try:
            await db.execute(
                f"UPDATE {tbl} SET is_computed=TRUE WHERE {col}=$1", d)
        except Exception:
            pass

    # Update state for next iteration
    state.prev_mng   = acc_mng
    state.prev_perf  = acc_perf
    state.prev_nta   = net_nta if net_nta > 0 else state.prev_nta

    logger.info(
        f"{d}: nta={r6(net_nta)} sec={r2(securities)} mm={r2(money_market)} "
        f"recv={r2(state.receivables)} cash={r2(state.cash)} "
        f"cap={r2(capital)} earn={r2(earnings)}")

    return {"date": str(d), "net_nta": r6(net_nta),
            "cash": r2(state.cash), "securities": r2(securities)}


# ─────────────────────────────────────────────────────────────
# Update holdings table with current snapshot
# ─────────────────────────────────────────────────────────────
async def _save_holdings(db, state: PortfolioState, as_of: date):
    # Clear non-locked holdings
    await db.execute("DELETE FROM holdings WHERE units > 0")

    for instr, pos in state.positions.items():
        if pos['units'] <= Decimal('0.0001'):
            continue
        price = await _price(db, instr, as_of)
        units = float(pos['units'])
        tc    = float(pos['total_cost'])
        avg   = float(pos['avg_cost'])
        mv    = units * float(price) if price > 0 else tc
        upl   = mv - tc

        await db.execute("""
            INSERT INTO holdings
                (instrument, asset_class, units, vwap, total_costs, total_cost,
                 last_price, market_value, unrealized_pl,
                 return_pct, last_trade_date, updated_at)
            VALUES ($1,$2,$3,$4,$5,$5,$6,$7,$8,
                    CASE WHEN $5>0 THEN ($7-$5)/$5 ELSE 0 END,
                    $9, NOW())
            ON CONFLICT (instrument) DO UPDATE SET
                asset_class=EXCLUDED.asset_class, units=EXCLUDED.units,
                vwap=EXCLUDED.vwap, total_costs=EXCLUDED.total_costs,
                total_cost=EXCLUDED.total_cost, last_price=EXCLUDED.last_price,
                market_value=EXCLUDED.market_value, unrealized_pl=EXCLUDED.unrealized_pl,
                return_pct=EXCLUDED.return_pct, updated_at=NOW()
        """, instr, pos['asset_class'], units, avg, tc,
             float(price), mv, upl, as_of)


# ─────────────────────────────────────────────────────────────
# MAIN ENTRY POINT
# ─────────────────────────────────────────────────────────────
async def compute_portfolio_and_nta(db, force_from: date = None) -> dict:
    """
    Single super-function: finds dirty data, rebuilds holdings+NTA forward.
    """
    today = date.today()

    if force_from:
        earliest_uncomputed = force_from
    else:
        # Find earliest is_computed=FALSE across all 6 input tables
        queries = [
            "SELECT MIN(date)        AS d FROM transactions        WHERE is_computed=FALSE",
            "SELECT MIN(date)        AS d FROM principal_cashflows WHERE is_computed=FALSE",
            "SELECT MIN(ex_date)     AS d FROM dividends           WHERE is_computed=FALSE",
            "SELECT MIN(record_date) AS d FROM others              WHERE is_computed=FALSE",
            "SELECT MIN(date)        AS d FROM fee_withdrawals     WHERE is_computed=FALSE",
            "SELECT MIN(pmt_date)    AS d FROM distributions       WHERE is_computed=FALSE",
        ]
        candidates = []
        for q in queries:
            try:
                row = await db.fetchrow(q)
                if row and row['d']:
                    candidates.append(row['d'])
            except Exception:
                pass

        earliest_uncomputed = min(candidates) if candidates else None

    # Find last historical date (b)
    last_hist = await db.fetchrow(
        "SELECT MAX(date) AS d FROM historical")
    last_hist_date = last_hist['d'] if last_hist and last_hist['d'] else None

    if not earliest_uncomputed and last_hist_date:
        # Nothing uncomputed — fill any gap to today
        start_date = last_hist_date + timedelta(days=1)
    elif earliest_uncomputed:
        # c = min(earliest_uncomputed - 1, last_hist_date)
        a_minus_1 = earliest_uncomputed - timedelta(days=1)
        if last_hist_date:
            c = min(a_minus_1, last_hist_date)
        else:
            c = a_minus_1
        start_date = c + timedelta(days=1)
    else:
        return {"computed": 0, "message": "No historical base data found"}

    if start_date > today:
        return {"computed": 0, "message": f"Already up to date (last: {last_hist_date})"}

    # Get historical row at c (the day before start) as base
    base_hist = await db.fetchrow(
        "SELECT * FROM historical WHERE date < $1 ORDER BY date DESC LIMIT 1",
        start_date)
    if not base_hist:
        return {"computed": 0, "message": f"No historical row before {start_date}"}

    base_date = base_hist['date']
    logger.info(f"Starting from {start_date} (base: {base_date}) → {today}")

    # Build state as-of base_date
    state = await _build_state_as_of(db, base_date, base_hist)

    # Total units (from investors, always current)
    ur = await db.fetchrow(
        "SELECT COALESCE(SUM(units),0) AS t FROM investors WHERE is_active=TRUE")
    total_units = D(ur['t']) if ur and D(ur['t']) > 0 else D(base_hist['total_units'])

    # Loop day by day
    computed = 0
    errors   = 0
    current  = start_date

    while current <= today:
        # Refresh total_units daily (handles new subscriptions mid-loop)
        ur = await db.fetchrow(
            "SELECT COALESCE(SUM(units),0) AS t FROM investors WHERE is_active=TRUE")
        tu = D(ur['t']) if ur and D(ur['t']) > 0 else total_units

        try:
            result = await _process_day(db, current, state, tu)
            if result:
                computed += 1
        except Exception as e:
            logger.error(f"  ✗ {current}: {e}")
            errors += 1

        current += timedelta(days=1)

    # Save final holdings snapshot
    try:
        await _save_holdings(db, state, today)
    except Exception as e:
        logger.error(f"Save holdings failed: {e}")

    logger.info(f"Done: {computed} days computed, {errors} errors")
    return {
        "computed":  computed,
        "errors":    errors,
        "from":      str(start_date),
        "to":        str(today),
        "message":   f"Computed {computed} days from {start_date} to {today}",
    }
