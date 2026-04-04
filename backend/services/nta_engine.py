"""
Holdings + NTA Computation Engine  v8.0.0
==========================================
Key design: PRE-LOAD all input data before loop.
Loop does pure Python arithmetic — only 2 DB writes per day.

DB queries:
  Before loop: ~10 bulk SELECTs covering full date range
  During loop: 2 writes per day (historical + fund_settings)
  After loop:  save holdings + mark computed

vs v7: 11 queries/day × 800 days = 8,880  →  10 + 800×2 = 1,610
"""
from datetime import date, timedelta
from decimal import Decimal, getcontext, ROUND_HALF_UP
from collections import defaultdict
from typing import Dict, List, Optional
import logging

logger = logging.getLogger(__name__)
getcontext().prec = 28

def D(v) -> Decimal:
    return Decimal('0') if v is None else Decimal(str(v))
def r2(v: Decimal) -> float:
    return float(v.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP))
def r6(v: Decimal) -> float:
    return float(v.quantize(Decimal('0.000001'), rounding=ROUND_HALF_UP))


# ── Job status ────────────────────────────────────────────────
async def _job_start(db, from_date, to_date):
    try:
        await db.execute("""
            INSERT INTO compute_job
                (id,status,started_at,finished_at,from_date,to_date,
                 processing_date,computed,errors,message,updated_at)
            VALUES (1,'running',NOW(),NULL,$1,$2,NULL,0,0,'Loading data...',NOW())
            ON CONFLICT (id) DO UPDATE SET
                status='running',started_at=NOW(),finished_at=NULL,
                from_date=$1,to_date=$2,processing_date=NULL,
                computed=0,errors=0,message='Loading data...',updated_at=NOW()
        """, from_date, to_date)
    except Exception: pass

async def _job_progress(db, current, computed, errors):
    try:
        await db.execute("""
            UPDATE compute_job
            SET processing_date=$1,computed=$2,errors=$3,message=$4,updated_at=NOW()
            WHERE id=1
        """, current, computed, errors, f"Computing {current} — {computed} done")
    except Exception: pass

async def _job_done(db, computed, errors, from_date, to_date):
    try:
        await db.execute("""
            UPDATE compute_job
            SET status=$1,finished_at=NOW(),computed=$2,errors=$3,message=$4,updated_at=NOW()
            WHERE id=1
        """, 'done' if errors==0 else 'error', computed, errors,
            f"Done — {computed} days ({from_date} → {to_date})"
            + (f", {errors} errors" if errors else ""))
    except Exception: pass


# ── Portfolio state ───────────────────────────────────────────
class PortfolioState:
    def __init__(self):
        self.positions: Dict[str, dict] = {}
        self.cash        = Decimal('0')
        self.receivables = Decimal('0')
        self.prev_mng    = Decimal('0')
        self.prev_perf   = Decimal('0')
        self.prev_i_fees = Decimal('0')
        self.prev_loans  = Decimal('0')
        self.prev_i_lns  = Decimal('0')
        self.prev_nta    = Decimal('1')

    def buy(self, instrument, units, net_cost, asset_class):
        if instrument not in self.positions:
            self.positions[instrument] = {
                'units': Decimal('0'), 'total_cost': Decimal('0'),
                'avg_cost': Decimal('0'), 'asset_class': asset_class}
        p = self.positions[instrument]
        p['total_cost'] += net_cost
        p['units']      += units
        p['avg_cost']    = p['total_cost'] / p['units'] if p['units'] > 0 else net_cost/units
        p['asset_class'] = asset_class

    def sell(self, instrument, units_sold) -> Decimal:
        if instrument not in self.positions:
            return Decimal('0')
        p = self.positions[instrument]
        if p['units'] <= 0:
            return Decimal('0')
        ratio      = min(units_sold / p['units'], Decimal('1'))
        cost_basis = p['total_cost'] * ratio
        p['total_cost'] -= cost_basis
        p['units']      -= units_sold
        if p['units'] < Decimal('0.001'):
            p['units'] = Decimal('0'); p['total_cost'] = Decimal('0')
        return cost_basis


# ── PRE-LOAD all input data ───────────────────────────────────
async def _load_all_data(db, start_date: date, end_date: date,
                         base_date: date) -> dict:
    """
    One query per input table covering the full compute range.
    Returns dicts keyed by date for O(1) lookup in the loop.
    """

    # Transactions: base_date for reconstruction + start_date→end_date for loop
    all_trades = await db.fetch("""
        SELECT date, instrument, units, price, net_amount, is_computed,
               COALESCE(asset_class,'Securities [H]') AS asset_class
        FROM transactions
        WHERE date <= $1
        ORDER BY date ASC, created_at ASC
    """, end_date)

    trades_all  = [dict(r) for r in all_trades]   # all for reconstruction
    trades_map  = defaultdict(list)                # keyed by date for loop
    for t in all_trades:
        if t['date'] >= start_date:
            trades_map[t['date']].append(dict(t))

    # Principal cashflows
    cf_rows = await db.fetch("""
        SELECT date, units, amount FROM principal_cashflows
        WHERE date <= $1 ORDER BY date ASC
    """, end_date)
    cf_cash_map  = defaultdict(Decimal)   # date → net cash
    cf_units_map = defaultdict(Decimal)   # date → net units
    cf_cum_units = Decimal('0')           # running total
    cf_cum_cash  = Decimal('0')
    units_by_date = {}                    # date → cumulative units
    for r in cf_rows:
        cf_cum_units += D(r['units'])
        cf_cum_cash  += D(r['amount'])
        units_by_date[r['date']] = cf_cum_units
        if r['date'] >= start_date:
            cf_cash_map[r['date']]  += D(r['amount'])
            cf_units_map[r['date']] += D(r['units'])

    # Capital up to base_date
    base_capital = Decimal('0')
    for r in cf_rows:
        if r['date'] <= base_date:
            base_capital += D(r['amount'])

    # Dividends (ex_date and pmt_date both matter)
    div_rows = await db.fetch("""
        SELECT ex_date, pmt_date, amount FROM dividends
        WHERE ex_date <= $1 OR pmt_date <= $1
    """, end_date)
    div_ex_map  = defaultdict(Decimal)   # ex_date  → total accrued
    div_pmt_map = defaultdict(Decimal)   # pmt_date → total received
    for r in div_rows:
        if r['ex_date']  and r['ex_date']  <= end_date: div_ex_map[r['ex_date']]   += D(r['amount'])
        if r['pmt_date'] and r['pmt_date'] <= end_date: div_pmt_map[r['pmt_date']] += D(r['amount'])

    # Others
    oth_map = defaultdict(Decimal)
    try:
        oth_rows = await db.fetch(
            "SELECT record_date, amount FROM others WHERE record_date <= $1", end_date)
        for r in oth_rows:
            if r['record_date'] >= start_date:
                oth_map[r['record_date']] += D(r['amount'])
    except Exception: pass

    # Fee withdrawals
    fw_map = defaultdict(lambda: {'management': Decimal('0'), 'performance': Decimal('0')})
    try:
        fw_rows = await db.fetch(
            "SELECT date, fee_type, amount FROM fee_withdrawals WHERE date <= $1", end_date)
        for r in fw_rows:
            if r['date'] >= start_date:
                fw_map[r['date']][r['fee_type']] += D(r['amount'])
    except Exception: pass

    # Distributions
    dist_map = defaultdict(Decimal)
    try:
        dist_rows = await db.fetch(
            "SELECT pmt_date, total_dividend FROM distributions WHERE pmt_date <= $1", end_date)
        for r in dist_rows:
            if r['pmt_date'] and r['pmt_date'] >= start_date:
                dist_map[r['pmt_date']] += D(r['total_dividend'] or 0)
    except Exception: pass

    # Price history: all prices for all instruments up to end_date
    # price_map[instrument][date] = price
    ph_rows = await db.fetch("""
        SELECT instrument, date, price FROM price_history
        WHERE date <= $1 ORDER BY instrument, date ASC
    """, end_date)
    price_by_instr = defaultdict(dict)
    for r in ph_rows:
        price_by_instr[r['instrument']][r['date']] = D(r['price'])

    # Build a "latest price on or before d" lookup
    # For each instrument, sorted dates → can binary search
    price_sorted = {}  # instrument → sorted list of (date, price)
    for instr, dmap in price_by_instr.items():
        price_sorted[instr] = sorted(dmap.items())  # list of (date, price)

    # Asset class map from ticker_map
    tm_rows = await db.fetch("SELECT instrument, asset_class FROM ticker_map")
    asset_class_map = {r['instrument']: r['asset_class'] for r in tm_rows}

    return {
        'trades_all':      trades_all,
        'trades_map':      trades_map,
        'cf_cash_map':     cf_cash_map,
        'cf_units_map':    cf_units_map,
        'units_by_date':   units_by_date,
        'base_capital':    base_capital,
        'div_ex_map':      div_ex_map,
        'div_pmt_map':     div_pmt_map,
        'oth_map':         oth_map,
        'fw_map':          fw_map,
        'dist_map':        dist_map,
        'price_sorted':    price_sorted,
        'asset_class_map': asset_class_map,
    }


def _get_price(price_sorted: dict, instrument: str, as_of: date) -> Decimal:
    """O(log n) price lookup — latest price on or before as_of."""
    lst = price_sorted.get(instrument)
    if not lst:
        return Decimal('0')
    # Binary search for rightmost date <= as_of
    lo, hi = 0, len(lst) - 1
    result = Decimal('0')
    while lo <= hi:
        mid = (lo + hi) // 2
        if lst[mid][0] <= as_of:
            result = lst[mid][1]
            lo = mid + 1
        else:
            hi = mid - 1
    return result


# ── Build state as-of base_date ───────────────────────────────
def _build_state_as_of(base_date: date, hist_row,
                       trades_all: list, asset_class_map: dict) -> PortfolioState:
    """Pure Python — no DB calls."""
    state = PortfolioState()
    state.cash        = D(hist_row['cash'])
    state.receivables = D(hist_row['receivables'])
    state.prev_mng    = D(hist_row['mng_fees'])
    state.prev_perf   = D(hist_row['perf_fees'])
    state.prev_i_fees = D(hist_row['ints_on_fees'])
    state.prev_loans  = D(hist_row['loans'])
    state.prev_i_lns  = D(hist_row['ints_on_loans'])
    state.prev_nta    = D(hist_row['nta'])

    for t in trades_all:
        if t['date'] > base_date:
            break
        units = D(t['units'])
        instr = t['instrument']
        ac    = asset_class_map.get(instr) or t['asset_class']
        if units > 0:
            state.buy(instr, units, abs(D(t['net_amount'])), ac)
        elif units < 0:
            state.sell(instr, abs(units))
    return state


# ── Process one day (pure Python + 2 DB writes) ───────────────
async def _process_day(db, d: date, state: PortfolioState,
                       total_units: Decimal, capital: Decimal,
                       data: dict, base_sched, perf_sched,
                       inception_date: date,
                       new_sells: list) -> dict:

    asset_class_map = data['asset_class_map']
    price_sorted    = data['price_sorted']

    # 4a. Trades
    for t in data['trades_map'].get(d, []):
        units   = D(t['units'])
        net_amt = D(t['net_amount'])
        instr   = t['instrument']
        ac      = asset_class_map.get(instr) or t['asset_class']

        if units > 0:
            state.buy(instr, units, abs(net_amt), ac)
            state.cash -= abs(net_amt)
        elif units < 0:
            units_sold = abs(units)
            avg_cost   = state.positions.get(instr, {}).get('avg_cost', Decimal('0'))
            cost_basis = state.sell(instr, units_sold)
            state.cash += net_amt
            if not t['is_computed']:
                pl      = net_amt - cost_basis
                ret_pct = pl / cost_basis if cost_basis > 0 else Decimal('0')
                new_sells.append({
                    'date': d, 'instrument': instr, 'asset_class': ac,
                    'units': float(units_sold), 'bought_price': float(avg_cost),
                    'sale_price': float(D(t['price'])),
                    'profit_loss': r2(pl), 'return_pct': r6(ret_pct),
                })

    # 4b. Cash
    cf_amt = data['cf_cash_map'].get(d, Decimal('0'))
    state.cash += cf_amt
    capital    += cf_amt

    dp = data['div_pmt_map'].get(d, Decimal('0'))
    state.cash += dp
    state.cash += data['oth_map'].get(d, Decimal('0'))

    fw = data['fw_map'].get(d, {})
    mgmt_w = fw.get('management', Decimal('0'))
    perf_w = fw.get('performance', Decimal('0'))
    state.cash -= (mgmt_w + perf_w)
    state.cash -= data['dist_map'].get(d, Decimal('0'))

    # 4c. Receivables
    state.receivables += data['div_ex_map'].get(d, Decimal('0'))
    state.receivables -= dp
    if state.receivables < 0:
        state.receivables = Decimal('0')

    # 4d. Asset values
    derivatives = securities = reits = bonds = money_market = Decimal('0')
    for instr, pos in state.positions.items():
        if pos['units'] < Decimal('0.001'):
            continue
        ac = pos['asset_class']
        if 'Money Market' in ac:
            money_market += pos['total_cost']
        else:
            price = _get_price(price_sorted, instr, d)
            val   = pos['units'] * price
            if   'Derivative' in ac or 'Warrant' in ac: derivatives += val
            elif 'Securities'  in ac:                    securities  += val
            elif 'Real Estate' in ac or 'REIT'    in ac: reits       += val
            elif 'Bond'        in ac:                    bonds       += val
            else:                                        securities  += val

    total_assets = (derivatives + securities + reits + bonds +
                    money_market + state.receivables + state.cash)

    # 4e. Fees
    gross_nta    = total_assets / total_units if total_units > 0 else Decimal('0')
    daily_return = float(gross_nta / state.prev_nta) - 1 if state.prev_nta > 0 else 0.0

    base_fee = total_assets * D(base_sched['rate']) / 365 if base_sched else Decimal('0')
    perf_fee = Decimal('0')
    if perf_sched and inception_date:
        hurdle       = float(D(perf_sched['hurdle_rate'] or 0))
        days_elapsed = max(1, (d - inception_date).days)
        annualised   = float(gross_nta) ** (365.0 / days_elapsed) - 1
        if annualised > hurdle:
            excess   = annualised - hurdle
            perf_fee = total_units * D(str(excess / 365)) * D(perf_sched['rate'])

    acc_mng   = max(Decimal('0'), state.prev_mng  + base_fee - mgmt_w)
    acc_perf  = max(Decimal('0'), state.prev_perf + perf_fee - perf_w)
    total_liab = acc_mng + acc_perf + state.prev_i_fees + state.prev_loans + state.prev_i_lns

    # 4f. Balance sheet
    nav      = total_assets - total_liab
    earnings = nav - capital
    net_nta  = nav / total_units if total_units > 0 else Decimal('0')

    # 2 DB writes only
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
        d, r2(derivatives), r2(securities), r2(reits), r2(bonds),
        r2(money_market), r2(state.receivables), r2(state.cash),
        r2(acc_mng), r2(acc_perf), r2(state.prev_i_fees),
        r2(state.prev_loans), r2(state.prev_i_lns),
        r2(capital), r2(earnings), float(total_units), r6(net_nta))

    await db.execute("""
        UPDATE fund_settings
        SET current_nta=$1, aum=$2, last_nta_date=$3, updated_at=NOW()
        WHERE id=1
    """, r6(net_nta), r2(nav), d)

    state.prev_mng  = acc_mng
    state.prev_perf = acc_perf
    state.prev_nta  = net_nta if net_nta > 0 else state.prev_nta

    return {"date": str(d), "net_nta": r6(net_nta), "new_capital": capital}


# ── Save holdings ─────────────────────────────────────────────
async def _save_holdings(db, state: PortfolioState,
                         as_of: date, price_sorted: dict):
    await db.execute("DELETE FROM holdings")
    rows = []
    for instr, pos in state.positions.items():
        if pos['units'] < Decimal('0.001'): continue
        price = _get_price(price_sorted, instr, as_of)
        units = float(pos['units']); tc = float(pos['total_cost'])
        avg   = float(pos['avg_cost'])
        mv    = units * float(price) if price > 0 else tc
        upl   = mv - tc; rp = upl / tc if tc > 0 else 0.0
        rows.append((instr, pos['asset_class'], units, avg, tc,
                     float(price), mv, upl, rp, as_of))
    for r in rows:
        await db.execute("""
            INSERT INTO holdings
                (instrument,asset_class,units,vwap,total_costs,total_cost,
                 last_price,market_value,unrealized_pl,return_pct,
                 last_trade_date,updated_at)
            VALUES ($1,$2,$3,$4,$5,$5,$6,$7,$8,$9,$10,NOW())
            ON CONFLICT (instrument) DO UPDATE SET
                asset_class=EXCLUDED.asset_class, units=EXCLUDED.units,
                vwap=EXCLUDED.vwap, total_costs=EXCLUDED.total_costs,
                total_cost=EXCLUDED.total_cost, last_price=EXCLUDED.last_price,
                market_value=EXCLUDED.market_value,
                unrealized_pl=EXCLUDED.unrealized_pl,
                return_pct=EXCLUDED.return_pct, updated_at=NOW()
        """, *r)
    await db.execute("""
        INSERT INTO holdings
            (instrument,asset_class,units,vwap,total_costs,total_cost,
             last_price,market_value,unrealized_pl,return_pct,
             last_trade_date,updated_at)
        VALUES ('__CASH__','Cash',1,0,$1,$1,0,$1,0,0,$2,NOW())
        ON CONFLICT (instrument) DO UPDATE SET
            total_costs=EXCLUDED.total_costs, total_cost=EXCLUDED.total_cost,
            market_value=EXCLUDED.market_value, updated_at=NOW()
    """, float(state.cash), as_of)


async def _mark_all_computed(db):
    for tbl in ['transactions','principal_cashflows','dividends',
                'others','fee_withdrawals','distributions']:
        try:
            await db.execute(f"UPDATE {tbl} SET is_computed=TRUE")
        except Exception as e:
            logger.warning(f"Mark {tbl}: {e}")


# ── MAIN ──────────────────────────────────────────────────────
async def compute_portfolio_and_nta(db, force_from: date = None) -> dict:
    today = date.today()

    # Step 1: earliest uncomputed
    if force_from:
        earliest_uncomputed = force_from
    else:
        candidates = []
        for q in [
            "SELECT MIN(date)        AS d FROM transactions        WHERE is_computed=FALSE",
            "SELECT MIN(date)        AS d FROM principal_cashflows WHERE is_computed=FALSE",
            "SELECT MIN(ex_date)     AS d FROM dividends           WHERE is_computed=FALSE",
            "SELECT MIN(record_date) AS d FROM others              WHERE is_computed=FALSE",
            "SELECT MIN(date)        AS d FROM fee_withdrawals     WHERE is_computed=FALSE",
            "SELECT MIN(pmt_date)    AS d FROM distributions       WHERE is_computed=FALSE",
        ]:
            try:
                row = await db.fetchrow(q)
                if row and row['d']: candidates.append(row['d'])
            except Exception: pass
        earliest_uncomputed = min(candidates) if candidates else None

    # Step 2: c = min(a-1, b)
    last_hist = await db.fetchrow("SELECT MAX(date) AS d FROM historical")
    b = last_hist['d'] if last_hist and last_hist['d'] else None

    if earliest_uncomputed and b:
        c = min(earliest_uncomputed - timedelta(days=1), b)
    elif earliest_uncomputed:
        c = earliest_uncomputed - timedelta(days=1)
    elif b:
        c = b
    else:
        return {"computed": 0, "message": "No historical base data"}

    start_date = c + timedelta(days=1)
    if start_date > today:
        return {"computed": 0, "message": f"Already up to date (last: {b})"}

    base_hist = await db.fetchrow(
        "SELECT * FROM historical WHERE date <= $1 ORDER BY date DESC LIMIT 1", c)
    if not base_hist:
        return {"computed": 0, "message": f"No historical row on or before {c}"}

    base_date = base_hist['date']
    logger.info(f"Base: {base_date} | Start: {start_date} → {today}")
    await _job_start(db, start_date, today)

    # Step 3: Pre-load ALL data (bulk queries, no per-day queries)
    data = await _load_all_data(db, start_date, today, base_date)

    # Cache fee schedules + inception date
    base_sched = await db.fetchrow("""
        SELECT rate FROM fee_schedules
        WHERE fee_type='base' AND valid_from<=$1 AND (valid_to IS NULL OR valid_to>=$1)
        ORDER BY valid_from DESC LIMIT 1
    """, today)
    perf_sched = await db.fetchrow("""
        SELECT rate, hurdle_rate FROM fee_schedules
        WHERE fee_type='performance' AND valid_from<=$1 AND (valid_to IS NULL OR valid_to>=$1)
        ORDER BY valid_from DESC LIMIT 1
    """, today)
    inc_row = await db.fetchrow("SELECT date FROM historical ORDER BY date ASC LIMIT 1")
    inception_date = inc_row['date'] if inc_row else start_date

    # Step 4: Reconstruct state (pure Python)
    state = _build_state_as_of(
        base_date, base_hist, data['trades_all'], data['asset_class_map'])
    capital = data['base_capital']

    # Step 5: Loop
    computed   = 0
    errors     = 0
    new_sells  = []    # batch settlement inserts
    current    = start_date

    # Build running total_units by date
    units_by_date = data['units_by_date']
    last_units    = D(base_hist['total_units'])

    while current <= today:
        # total_units: pick latest known value up to current
        if current in units_by_date:
            last_units = units_by_date[current]
        total_units = last_units if last_units > 0 else D(base_hist['total_units'])

        try:
            result = await _process_day(
                db, current, state, total_units, capital,
                data, base_sched, perf_sched, inception_date, new_sells)
            if result:
                computed += 1
                capital  = result['new_capital']
        except Exception as e:
            import traceback
            logger.error(f"  ✗ {current}: {e}\n{traceback.format_exc()}")
            errors += 1

        if computed % 10 == 0 or current == today:
            await _job_progress(db, current, computed, errors)

        current += timedelta(days=1)

    # Batch write settlements
    for s in new_sells:
        try:
            await db.execute("""
                INSERT INTO settlement
                    (date,instrument,asset_class,units,
                     bought_price,sale_price,profit_loss,return_pct,remark)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,'system-computed')
            """, s['date'], s['instrument'], s['asset_class'], s['units'],
                 s['bought_price'], s['sale_price'], s['profit_loss'], s['return_pct'])
        except Exception as e:
            logger.warning(f"Settlement insert: {e}")

    # Save holdings + mark computed
    try:
        await _save_holdings(db, state, today, data['price_sorted'])
    except Exception as e:
        logger.error(f"Save holdings: {e}")

    await _mark_all_computed(db)
    await _job_done(db, computed, errors, start_date, today)
    logger.info(f"Done: {computed} computed, {errors} errors")

    return {
        "computed": computed, "errors": errors,
        "from": str(start_date), "to": str(today),
        "message": f"Computed {computed} days ({start_date} → {today})",
    }
