"""
Holdings + NTA Computation Engine  v6.0.0
==========================================
Super-function recap:
  1. Scan 6 input DBs → earliest is_computed=FALSE → date (a)
  2. c = min(a - 1, last_historical_date b)
  3. Reconstruct holdings as-of c:
       - positions: replay ALL transactions WHERE date <= c (VWAP)
       - cash:      read directly from historical[c].cash
       - everything else carried from historical[c]
  4. Loop d = c+1 → today (ignore is_computed, always process):
       a. Apply trades on d:
            BUY  (units > 0): add to position, deduct cash (net_amount < 0 → cash -= abs)
            SELL (units < 0): reduce position VWAP, add cash (net_amount > 0 → cash += net)
                              write settlement record
       b. Cash movements on d:
            + principal_cashflows.amount  (subscriptions +, redemptions -)
            + dividends.pmt_date = d      (stock dividends received)
            + others.record_date = d      (interest, misc income)
            - fee_withdrawals.date = d    (management/perf fee payments)
            - distributions.pmt_date = d  (fund payouts to members)
       c. Receivables:
            + dividends.ex_date = d       (accrued, not yet cash)
            - dividends.pmt_date = d      (now received as cash)
       d. Get prices from price_history (latest on or before d)
       e. Compute full balance sheet, write to historical
       f. total_units = SUM(principal_cashflows.units WHERE date <= d)
  5. After loop: save holdings snapshot, mark ALL 6 DBs is_computed=TRUE
"""
from datetime import date, timedelta
from decimal import Decimal, getcontext, ROUND_HALF_UP
from typing import Optional, Dict
import logging

logger = logging.getLogger(__name__)
getcontext().prec = 28

def D(v) -> Decimal:
    return Decimal('0') if v is None else Decimal(str(v))

def r2(v: Decimal) -> float:
    return float(v.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP))

def r6(v: Decimal) -> float:
    return float(v.quantize(Decimal('0.000001'), rounding=ROUND_HALF_UP))


# ─────────────────────────────────────────────────────────────
# Portfolio state carried through the loop
# ─────────────────────────────────────────────────────────────
class PortfolioState:
    def __init__(self):
        self.positions: Dict[str, dict] = {}  # instrument → {units, total_cost, avg_cost, asset_class}
        self.cash        = Decimal('0')
        self.receivables = Decimal('0')
        self.prev_mng    = Decimal('0')
        self.prev_perf   = Decimal('0')
        self.prev_i_fees = Decimal('0')
        self.prev_loans  = Decimal('0')
        self.prev_i_lns  = Decimal('0')
        self.prev_nta    = Decimal('1')

    def buy(self, instrument: str, units: Decimal, net_cost: Decimal, asset_class: str):
        """Add units at net_cost. Recalculate VWAP."""
        if instrument not in self.positions:
            self.positions[instrument] = {
                'units': Decimal('0'), 'total_cost': Decimal('0'),
                'avg_cost': Decimal('0'), 'asset_class': asset_class}
        p = self.positions[instrument]
        p['total_cost'] += net_cost
        p['units']      += units
        p['avg_cost']    = p['total_cost'] / p['units'] if p['units'] > 0 else net_cost / units
        p['asset_class'] = asset_class

    def sell(self, instrument: str, units_sold: Decimal) -> Decimal:
        """
        Reduce position by units_sold using VWAP.
        Returns cost_basis of units sold (for P&L calculation).
        """
        if instrument not in self.positions:
            return Decimal('0')
        p = self.positions[instrument]
        if p['units'] <= 0:
            return Decimal('0')
        ratio      = min(units_sold / p['units'], Decimal('1'))
        cost_basis = p['total_cost'] * ratio
        p['total_cost'] = p['total_cost'] - cost_basis
        p['units']     -= units_sold
        if p['units'] < Decimal('0.0001'):
            p['units']      = Decimal('0')
            p['total_cost'] = Decimal('0')
        return cost_basis


# ─────────────────────────────────────────────────────────────
# Price helper
# ─────────────────────────────────────────────────────────────
async def _price(db, instrument: str, as_of: date) -> Decimal:
    row = await db.fetchrow("""
        SELECT price FROM price_history
        WHERE instrument = $1 AND date <= $2
        ORDER BY date DESC LIMIT 1
    """, instrument, as_of)
    return D(row['price']) if row else Decimal('0')


# ─────────────────────────────────────────────────────────────
# Step 3: Build portfolio state as-of date c
# ─────────────────────────────────────────────────────────────
async def _build_state_as_of(db, base_date: date, hist_row) -> PortfolioState:
    """
    Reconstruct positions from ALL transactions up to base_date using VWAP.
    Cash and all carried values taken directly from hist_row (historical DB).
    Trades do NOT touch cash here — hist_row.cash already reflects everything.
    """
    state = PortfolioState()
    state.cash        = D(hist_row['cash'])
    state.receivables = D(hist_row['receivables'])
    state.prev_mng    = D(hist_row['mng_fees'])
    state.prev_perf   = D(hist_row['perf_fees'])
    state.prev_i_fees = D(hist_row['ints_on_fees'])
    state.prev_loans  = D(hist_row['loans'])
    state.prev_i_lns  = D(hist_row['ints_on_loans'])
    state.prev_nta    = D(hist_row['nta'])

    trades = await db.fetch("""
        SELECT instrument, units, net_amount,
               COALESCE(asset_class, 'Securities [H]') AS asset_class
        FROM transactions
        WHERE date <= $1
        ORDER BY date ASC, created_at ASC
    """, base_date)

    for t in trades:
        units = D(t['units'])
        ac    = t['asset_class']
        instr = t['instrument']
        if units > 0:
            # BUY — net_cost = abs(net_amount) since net_amount is negative
            state.buy(instr, units, abs(D(t['net_amount'])), ac)
        elif units < 0:
            # SELL — just reduce position (cash already in hist_row)
            state.sell(instr, abs(units))

    return state


# ─────────────────────────────────────────────────────────────
# Step 4: Process one day
# ─────────────────────────────────────────────────────────────
async def _process_day(db, d: date, state: PortfolioState, total_units: Decimal) -> Optional[dict]:

    # ── 4a. Apply trades on d ────────────────────────────────
    trades = await db.fetch("""
        SELECT id, instrument, units, price, net_amount,
               COALESCE(asset_class, 'Securities [H]') AS asset_class
        FROM transactions WHERE date = $1
        ORDER BY created_at ASC
    """, d)

    for t in trades:
        units     = D(t['units'])
        net_amt   = D(t['net_amount'])
        instr     = t['instrument']
        ac        = t['asset_class']

        if units > 0:
            # BUY: add to position, deduct cash
            net_cost = abs(net_amt)   # net_amount is negative for buys
            state.buy(instr, units, net_cost, ac)
            state.cash -= net_cost

        elif units < 0:
            # SELL: reduce position, add cash, write settlement
            units_sold = abs(units)
            avg_cost   = state.positions.get(instr, {}).get('avg_cost', Decimal('0'))
            cost_basis = state.sell(instr, units_sold)
            proceeds   = net_amt   # net_amount is positive for sells
            state.cash += proceeds

            # P&L = proceeds - cost_basis
            pl         = proceeds - cost_basis
            sale_price = D(t['price'])
            ret_pct    = pl / cost_basis if cost_basis > 0 else Decimal('0')

            # Write settlement record
            try:
                await db.execute("""
                    INSERT INTO settlement
                        (date, instrument, asset_class, units,
                         bought_price, sale_price, profit_loss, return_pct, remark)
                    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,'system-computed')
                    ON CONFLICT DO NOTHING
                """, d, instr, ac, float(units_sold),
                     float(avg_cost), float(sale_price),
                     r2(pl), r6(ret_pct))
            except Exception as e:
                logger.warning(f"Settlement write failed {d} {instr}: {e}")

    # ── 4b. Cash movements on d ──────────────────────────────
    # Principal cashflows (subscriptions +, redemptions -)
    cf = await db.fetchrow(
        "SELECT COALESCE(SUM(amount),0) AS n FROM principal_cashflows WHERE date=$1", d)
    state.cash += D(cf['n'])

    # Stock dividends received (pmt_date = d) → cash in, receivable out
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

    # Fee withdrawals (management + performance)
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

    # ── 4c. Receivables ──────────────────────────────────────
    # Dividend went ex today → accrue as receivable
    ex_today = await db.fetchrow(
        "SELECT COALESCE(SUM(amount),0) AS n FROM dividends WHERE ex_date=$1", d)
    state.receivables += D(ex_today['n'])
    # Dividend paid today → remove from receivables (now in cash)
    state.receivables -= D(dp['n'])
    if state.receivables < 0:
        state.receivables = Decimal('0')

    # ── 4d. Asset values ─────────────────────────────────────
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
            money_market += pos['total_cost']       # at cost — no live price
        else:
            price = await _price(db, instr, d)
            val   = pos['units'] * price
            if   'Derivative' in ac or 'Warrant' in ac: derivatives += val
            elif 'Securities'  in ac:                    securities  += val
            elif 'Real Estate' in ac or 'REIT'    in ac: reits       += val
            elif 'Bond'        in ac:                    bonds       += val
            else:                                        securities  += val  # fallback

    total_assets = derivatives + securities + reits + bonds + \
                   money_market + state.receivables + state.cash

    # ── 4e. Fees & liabilities ───────────────────────────────
    gross_nta    = total_assets / total_units if total_units > 0 else Decimal('0')
    daily_return = float(gross_nta / state.prev_nta) - 1 if state.prev_nta > 0 else 0.0

    base_sched = await db.fetchrow("""
        SELECT rate FROM fee_schedules
        WHERE fee_type='base'
          AND valid_from <= $1 AND (valid_to IS NULL OR valid_to >= $1)
        ORDER BY valid_from DESC LIMIT 1
    """, d)
    perf_sched = await db.fetchrow("""
        SELECT rate, hurdle_rate FROM fee_schedules
        WHERE fee_type='performance'
          AND valid_from <= $1 AND (valid_to IS NULL OR valid_to >= $1)
        ORDER BY valid_from DESC LIMIT 1
    """, d)

    base_fee = total_assets * D(base_sched['rate']) / 365 if base_sched else Decimal('0')
    perf_fee = Decimal('0')
    if perf_sched:
        hurdle   = float(D(perf_sched['hurdle_rate'] or 0))
        d_hurdle = (1 + hurdle) ** (1 / 365) - 1
        excess   = max(0.0, daily_return - d_hurdle)
        perf_fee = total_assets * D(str(excess)) * D(perf_sched['rate'])

    acc_mng    = max(Decimal('0'), state.prev_mng  + base_fee - mgmt_w)
    acc_perf   = max(Decimal('0'), state.prev_perf + perf_fee - perf_w)
    total_liab = acc_mng + acc_perf + state.prev_i_fees + state.prev_loans + state.prev_i_lns

    # ── 4f. Capital & Earnings ───────────────────────────────
    # Capital = cumulative SUM of all principal cashflows up to d
    cap_row = await db.fetchrow(
        "SELECT COALESCE(SUM(amount),0) AS n FROM principal_cashflows WHERE date <= $1", d)
    capital  = D(cap_row['n'])
    nav      = total_assets - total_liab
    earnings = nav - capital
    net_nta  = nav / total_units if total_units > 0 else Decimal('0')

    # ── Write historical ─────────────────────────────────────
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
        WHERE historical.is_locked = FALSE
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

    # Update carried state
    state.prev_mng  = acc_mng
    state.prev_perf = acc_perf
    state.prev_nta  = net_nta if net_nta > 0 else state.prev_nta

    logger.info(
        f"{d}: nta={r6(net_nta)} | sec={r2(securities)} mm={r2(money_market)} "
        f"recv={r2(state.receivables)} cash={r2(state.cash)} "
        f"cap={r2(capital)} earn={r2(earnings)} units={float(total_units):.4f}")

    return {"date": str(d), "net_nta": r6(net_nta),
            "cash": r2(state.cash), "securities": r2(securities)}


# ─────────────────────────────────────────────────────────────
# Step 5: Save holdings snapshot to holdings table
# ─────────────────────────────────────────────────────────────
async def _save_holdings(db, state: PortfolioState, as_of: date):
    await db.execute("DELETE FROM holdings")

    for instr, pos in state.positions.items():
        if pos['units'] <= Decimal('0.0001'):
            continue
        price = await _price(db, instr, as_of)
        units = float(pos['units'])
        tc    = float(pos['total_cost'])
        avg   = float(pos['avg_cost'])
        mv    = units * float(price) if price > 0 else tc
        upl   = mv - tc
        rp    = upl / tc if tc > 0 else 0.0

        await db.execute("""
            INSERT INTO holdings
                (instrument, asset_class, units, vwap, total_costs, total_cost,
                 last_price, market_value, unrealized_pl, return_pct,
                 last_trade_date, updated_at)
            VALUES ($1,$2,$3,$4,$5,$5,$6,$7,$8,$9,$10,NOW())
            ON CONFLICT (instrument) DO UPDATE SET
                asset_class=EXCLUDED.asset_class,
                units=EXCLUDED.units,
                vwap=EXCLUDED.vwap,
                total_costs=EXCLUDED.total_costs,
                total_cost=EXCLUDED.total_cost,
                last_price=EXCLUDED.last_price,
                market_value=EXCLUDED.market_value,
                unrealized_pl=EXCLUDED.unrealized_pl,
                return_pct=EXCLUDED.return_pct,
                updated_at=NOW()
        """, instr, pos['asset_class'], units, avg, tc,
             float(price), mv, upl, rp, as_of)


# ─────────────────────────────────────────────────────────────
# Mark all 6 input DBs as computed
# ─────────────────────────────────────────────────────────────
async def _mark_all_computed(db):
    for tbl in ['transactions', 'principal_cashflows', 'dividends',
                'others', 'fee_withdrawals', 'distributions']:
        try:
            await db.execute(f"UPDATE {tbl} SET is_computed=TRUE")
        except Exception as e:
            logger.warning(f"Could not mark {tbl} computed: {e}")


# ─────────────────────────────────────────────────────────────
# MAIN ENTRY POINT
# ─────────────────────────────────────────────────────────────
async def compute_portfolio_and_nta(db, force_from: date = None) -> dict:
    """
    Single super-function.
    Finds earliest uncomputed data, rebuilds holdings + NTA from there to today.
    """
    today = date.today()

    # ── Step 1: Find earliest uncomputed date (a) ────────────
    if force_from:
        earliest_uncomputed = force_from
    else:
        candidates = []
        checks = [
            "SELECT MIN(date)        AS d FROM transactions        WHERE is_computed=FALSE",
            "SELECT MIN(date)        AS d FROM principal_cashflows WHERE is_computed=FALSE",
            "SELECT MIN(ex_date)     AS d FROM dividends           WHERE is_computed=FALSE",
            "SELECT MIN(record_date) AS d FROM others              WHERE is_computed=FALSE",
            "SELECT MIN(date)        AS d FROM fee_withdrawals     WHERE is_computed=FALSE",
            "SELECT MIN(pmt_date)    AS d FROM distributions       WHERE is_computed=FALSE",
        ]
        for q in checks:
            try:
                row = await db.fetchrow(q)
                if row and row['d']:
                    candidates.append(row['d'])
            except Exception:
                pass
        earliest_uncomputed = min(candidates) if candidates else None

    # ── Step 2: Find c = min(a-1, b) ────────────────────────
    last_hist = await db.fetchrow("SELECT MAX(date) AS d FROM historical")
    b = last_hist['d'] if last_hist and last_hist['d'] else None

    if not earliest_uncomputed and b:
        # No dirty data — fill gap from last historical to today
        c = b
    elif earliest_uncomputed and b:
        a_minus_1 = earliest_uncomputed - timedelta(days=1)
        c = min(a_minus_1, b)
    elif earliest_uncomputed:
        c = earliest_uncomputed - timedelta(days=1)
    else:
        return {"computed": 0, "message": "No data to compute"}

    start_date = c + timedelta(days=1)
    if start_date > today:
        return {"computed": 0, "message": f"Already up to date (last: {b})"}

    # ── Step 3: Get base historical row at c ─────────────────
    base_hist = await db.fetchrow(
        "SELECT * FROM historical WHERE date <= $1 ORDER BY date DESC LIMIT 1", c)
    if not base_hist:
        return {"computed": 0, "message": f"No historical row on or before {c}"}

    base_date = base_hist['date']
    logger.info(f"Base: {base_date} | Start: {start_date} → {today}")

    # ── Step 3: Reconstruct portfolio state as-of base_date ──
    state = await _build_state_as_of(db, base_date, base_hist)

    # ── Step 4: Loop day by day ──────────────────────────────
    computed = 0
    errors   = 0
    current  = start_date

    while current <= today:
        # total_units: cumulative from principal_cashflows up to current date
        # (historical per-day value, not current investors.units)
        tu_row = await db.fetchrow(
            "SELECT COALESCE(SUM(units),0) AS t FROM principal_cashflows WHERE date <= $1",
            current)
        total_units = D(tu_row['t']) if tu_row and D(tu_row['t']) > 0 \
                      else D(base_hist['total_units'])

        try:
            result = await _process_day(db, current, state, total_units)
            if result:
                computed += 1
        except Exception as e:
            import traceback
            logger.error(f"  ✗ {current}: {e}\n{traceback.format_exc()}")
            errors += 1

        current += timedelta(days=1)

    # ── Step 5: Save holdings snapshot ───────────────────────
    try:
        await _save_holdings(db, state, today)
        logger.info("Holdings snapshot saved")
    except Exception as e:
        logger.error(f"Save holdings failed: {e}")

    # ── Step 5: Mark ALL 6 input DBs is_computed=TRUE ────────
    await _mark_all_computed(db)
    logger.info("All input DBs marked is_computed=TRUE")

    logger.info(f"Compute done: {computed} days OK, {errors} errors")
    return {
        "computed": computed,
        "errors":   errors,
        "from":     str(start_date),
        "to":       str(today),
        "message":  f"Computed {computed} days ({start_date} → {today})",
    }
