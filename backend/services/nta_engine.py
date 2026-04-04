"""
NTA + Holdings Engine  v9.0  —  written against LIVE schema after migration 14.

Schema facts used:
  transactions:        date, instrument, units(+buy/-sell), price, net_amount, asset_class, is_computed
  principal_cashflows: date, units, amount, is_computed
  dividends:           ex_date, pmt_date, amount, is_computed
  others:              record_date, amount, is_computed
  fee_withdrawals:     date, fee_type, amount, is_computed
  distributions:       pmt_date, total_dividend, is_computed
  holdings:            instrument, asset_class, units, vwap, total_costs, last_price (no investor_id)
  historical:          date, securities, cash, mng_fees, perf_fees, total_units, nta, is_locked
  price_history:       instrument, date, price
  ticker_map:          instrument, asset_class
  fund_settings:       total_units, current_nta, aum
  investors:           units, is_active

Flow:
  Step 1  Find earliest is_computed=FALSE across all 6 tables → date a
  Step 2  c = min(a-1 , last_historical_date b)
  Step 3  Load base state from historical[c]: cash, receivables, fee accruals, prev_nta
  Step 4  Reconstruct positions as-of c by replaying ALL transactions <= c (VWAP)
  Step 5  Load ALL input data for range [c+1 .. today] into memory  (bulk queries)
  Step 6  Loop d = c+1 → today  (pure Python, 2 DB writes per day):
            a) Apply trades on d  →  update positions + cash
            b) Cash movements on d  (+subscriptions +div_pmt +others -fee_w -distributions)
            c) Receivables  (+ex_date, -pmt_date)
            d) Value positions using price_history
            e) Compute balance sheet
            f) Write historical row  (ONE write per day)
  Step 7  Save holdings snapshot
  Step 8  Mark all 6 input tables is_computed=TRUE
"""
from datetime import date, timedelta
from decimal import Decimal, getcontext, ROUND_HALF_UP
from collections import defaultdict
from typing import Dict, List, Optional
import logging, traceback

logger = logging.getLogger(__name__)
getcontext().prec = 28

def D(v):
    return Decimal('0') if v is None else Decimal(str(v))
def r2(v):
    return float(D(v).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP))
def r6(v):
    return float(D(v).quantize(Decimal('0.000001'), rounding=ROUND_HALF_UP))


# ── Job progress ──────────────────────────────────────────────
async def _job(db, status, from_d=None, to_d=None, proc=None, comp=0, err=0, msg=''):
    try:
        await db.execute("""
            INSERT INTO compute_job
              (id,status,started_at,finished_at,from_date,to_date,
               processing_date,computed,errors,message,updated_at)
            VALUES (1,$1,
              CASE WHEN $1='running' THEN NOW() ELSE (SELECT started_at FROM compute_job WHERE id=1) END,
              CASE WHEN $1 IN ('done','error') THEN NOW() ELSE NULL END,
              $2,$3,$4,$5,$6,$7,NOW())
            ON CONFLICT (id) DO UPDATE SET
              status=$1,
              started_at=CASE WHEN $1='running' THEN NOW() ELSE compute_job.started_at END,
              finished_at=CASE WHEN $1 IN ('done','error') THEN NOW() ELSE NULL END,
              from_date=$2, to_date=$3, processing_date=$4,
              computed=$5, errors=$6, message=$7, updated_at=NOW()
        """, status, from_d, to_d, proc, comp, err, msg)
    except Exception as e:
        logger.warning(f"job status write failed: {e}")


# ── Portfolio positions ───────────────────────────────────────
class Positions:
    def __init__(self):
        self.data: Dict[str, dict] = {}   # instrument → {units, total_cost, avg_cost, ac}

    def buy(self, instr, units, net_cost, ac):
        if instr not in self.data:
            self.data[instr] = {'units': D(0), 'total_cost': D(0), 'avg_cost': D(0), 'ac': ac}
        p = self.data[instr]
        p['total_cost'] += net_cost
        p['units']      += units
        p['avg_cost']    = p['total_cost'] / p['units'] if p['units'] > 0 else D(0)
        p['ac']          = ac

    def sell(self, instr, units_sold):
        """Returns cost_basis removed."""
        p = self.data.get(instr)
        if not p or p['units'] <= 0:
            return D(0)
        ratio      = min(units_sold / p['units'], D(1))
        cost_basis = p['total_cost'] * ratio
        p['total_cost'] -= cost_basis
        p['units']      -= units_sold
        if p['units'] < D('0.001'):
            p['units'] = D(0); p['total_cost'] = D(0)
        return cost_basis


def _latest_price(price_map, instr, d):
    """Return latest price on or before d from sorted list."""
    lst = price_map.get(instr)
    if not lst: return D(0)
    lo, hi, result = 0, len(lst)-1, D(0)
    while lo <= hi:
        mid = (lo+hi)//2
        if lst[mid][0] <= d: result = lst[mid][1]; lo = mid+1
        else: hi = mid-1
    return result


async def compute_portfolio_and_nta(db, force_from: date = None) -> dict:
    today = date.today()

    # ── Step 1: earliest is_computed=FALSE ───────────────────
    if force_from:
        a = force_from
    else:
        candidates = []
        for q in [
            "SELECT MIN(date)        FROM transactions        WHERE is_computed=FALSE",
            "SELECT MIN(date)        FROM principal_cashflows WHERE is_computed=FALSE",
            "SELECT MIN(ex_date)     FROM dividends           WHERE is_computed=FALSE",
            "SELECT MIN(record_date) FROM others              WHERE is_computed=FALSE",
            "SELECT MIN(date)        FROM fee_withdrawals     WHERE is_computed=FALSE",
            "SELECT MIN(pmt_date)    FROM distributions       WHERE is_computed=FALSE",
        ]:
            try:
                row = await db.fetchrow(q)
                v = row[0] if row else None
                if v: candidates.append(v)
            except Exception: pass
        a = min(candidates) if candidates else None

    # ── Step 2: c = min(a-1, b) ──────────────────────────────
    row_b = await db.fetchrow("SELECT MAX(date) FROM historical")
    b = row_b[0] if row_b and row_b[0] else None

    if a and b:   c = min(a - timedelta(days=1), b)
    elif a:       c = a - timedelta(days=1)
    elif b:       c = b
    else:
        return {"computed": 0, "message": "No base data found"}

    start = c + timedelta(days=1)
    if start > today:
        return {"computed": 0, "message": f"Up to date (last: {b})"}

    # ── Step 3: Load base historical row at c ─────────────────
    base = await db.fetchrow(
        "SELECT * FROM historical WHERE date <= $1 ORDER BY date DESC LIMIT 1", c)
    if not base:
        return {"computed": 0, "message": f"No historical row on or before {c}"}

    base_date = base['date']
    logger.info(f"Base: {base_date}  Start: {start} → {today}")
    await _job(db, 'running', start, today, start, 0, 0, 'Loading...')

    # ── Step 4: Reconstruct positions as-of base_date ────────
    pos = Positions()
    all_trades = await db.fetch("""
        SELECT date, instrument, units, net_amount,
               COALESCE(asset_class,'Securities [H]') AS ac
        FROM transactions WHERE date <= $1
        ORDER BY date ASC, created_at ASC
    """, base_date)
    # Asset class from ticker_map (authoritative)
    tm = await db.fetch("SELECT instrument, asset_class FROM ticker_map")
    ac_map = {r['instrument']: r['asset_class'] for r in tm}

    for t in all_trades:
        units = D(t['units']); instr = t['instrument']
        ac    = ac_map.get(instr) or t['ac']
        if units > 0:
            pos.buy(instr, units, abs(D(t['net_amount'])), ac)
        elif units < 0:
            pos.sell(instr, abs(units))

    # State carried from base historical row
    cash        = D(base['cash'])
    receivables = D(base['receivables'])
    prev_mng    = D(base['mng_fees'])
    prev_perf   = D(base['perf_fees'])
    prev_i_fees = D(base['ints_on_fees'])
    prev_loans  = D(base['loans'])
    prev_i_lns  = D(base['ints_on_loans'])
    prev_nta    = D(base['nta'])

    # ── Step 5: Bulk-load all input data for [start..today] ──
    # Transactions
    loop_trades = await db.fetch("""
        SELECT date, instrument, units, price, net_amount, is_computed,
               COALESCE(asset_class,'Securities [H]') AS ac
        FROM transactions WHERE date >= $1 AND date <= $2
        ORDER BY date ASC, created_at ASC
    """, start, today)
    trade_map = defaultdict(list)
    for t in loop_trades:
        trade_map[t['date']].append(dict(t))

    # Principal cashflows
    cf_rows = await db.fetch("""
        SELECT date, units, amount FROM principal_cashflows
        WHERE date >= $1 AND date <= $2
    """, start, today)
    cf_cash_map  = defaultdict(Decimal)
    cf_units_map = defaultdict(Decimal)
    for r in cf_rows:
        cf_cash_map[r['date']]  += D(r['amount'])
        cf_units_map[r['date']] += D(r['units'])

    # total_units per day (cumulative from inception)
    all_cf = await db.fetch(
        "SELECT date, units FROM principal_cashflows WHERE date <= $1 ORDER BY date ASC", today)
    cumul_units = D(0); units_timeline = {}
    for r in all_cf:
        cumul_units += D(r['units'])
        units_timeline[r['date']] = cumul_units
    # Get base total_units (last known before start)
    base_units = D(base['total_units'])
    last_units = base_units
    for d2 in sorted(units_timeline):
        if d2 <= base_date: last_units = units_timeline[d2]

    # Dividends
    div_rows = await db.fetch("""
        SELECT ex_date, pmt_date, amount FROM dividends
        WHERE (ex_date >= $1 OR pmt_date >= $1) AND
              (ex_date <= $2 OR pmt_date <= $2)
    """, start, today)
    div_ex_map  = defaultdict(Decimal)
    div_pmt_map = defaultdict(Decimal)
    for r in div_rows:
        if r['ex_date']  and start <= r['ex_date']  <= today: div_ex_map[r['ex_date']]   += D(r['amount'])
        if r['pmt_date'] and start <= r['pmt_date'] <= today: div_pmt_map[r['pmt_date']] += D(r['amount'])

    # Others
    oth_map = defaultdict(Decimal)
    try:
        oth = await db.fetch(
            "SELECT record_date, amount FROM others WHERE record_date >= $1 AND record_date <= $2",
            start, today)
        for r in oth: oth_map[r['record_date']] += D(r['amount'])
    except Exception: pass

    # Fee withdrawals
    fw_map = defaultdict(lambda: {'management': D(0), 'performance': D(0)})
    try:
        fw = await db.fetch(
            "SELECT date, fee_type, amount FROM fee_withdrawals WHERE date >= $1 AND date <= $2",
            start, today)
        for r in fw: fw_map[r['date']][r['fee_type']] += D(r['amount'])
    except Exception: pass

    # Distributions
    dist_map = defaultdict(Decimal)
    try:
        dist = await db.fetch(
            "SELECT pmt_date, total_dividend FROM distributions WHERE pmt_date >= $1 AND pmt_date <= $2",
            start, today)
        for r in dist: dist_map[r['pmt_date']] += D(r['total_dividend'] or 0)
    except Exception: pass

    # Prices — sorted list per instrument for binary search
    ph = await db.fetch(
        "SELECT instrument, date, price FROM price_history WHERE date <= $1 ORDER BY instrument, date ASC",
        today)
    price_map = defaultdict(list)
    for r in ph:
        price_map[r['instrument']].append((r['date'], D(r['price'])))

    # Fee schedules (cached)
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
    inception_date = inc_row['date'] if inc_row else start

    # Capital cumulative
    cap_row = await db.fetchrow(
        "SELECT COALESCE(SUM(amount),0) AS n FROM principal_cashflows WHERE date <= $1", base_date)
    capital = D(cap_row['n'])

    # ── Step 6: Loop ──────────────────────────────────────────
    computed = 0; errors = 0; settlements = []
    current = start

    while current <= today:
        try:
            # total_units on this day
            if current in units_timeline:
                last_units = units_timeline[current]
            total_units = last_units if last_units > 0 else base_units

            # 6a. Trades
            for t in trade_map.get(current, []):
                units  = D(t['units']); net = D(t['net_amount'])
                instr  = t['instrument']
                ac     = ac_map.get(instr) or t['ac']
                if units > 0:
                    pos.buy(instr, units, abs(net), ac)
                    cash -= abs(net)
                elif units < 0:
                    avg_c      = pos.data.get(instr, {}).get('avg_cost', D(0))
                    cost_basis = pos.sell(instr, abs(units))
                    cash      += net
                    if not t['is_computed']:
                        pl = net - cost_basis
                        settlements.append((
                            current, instr, ac, float(abs(units)),
                            float(avg_c), float(D(t['price'])),
                            r2(pl), r6(pl/cost_basis if cost_basis>0 else D(0))
                        ))

            # 6b. Cash movements
            cf_amt  = cf_cash_map.get(current, D(0))
            capital += cf_amt
            cash    += cf_amt
            dp       = div_pmt_map.get(current, D(0))
            cash    += dp
            cash    += oth_map.get(current, D(0))
            fw       = fw_map.get(current, {})
            mgmt_w   = fw.get('management', D(0))
            perf_w   = fw.get('performance', D(0))
            cash    -= mgmt_w + perf_w
            cash    -= dist_map.get(current, D(0))

            # 6c. Receivables
            receivables += div_ex_map.get(current, D(0))
            receivables -= dp
            if receivables < 0: receivables = D(0)

            # 6d. Value positions
            derivatives = securities = reits = bonds = money_market = D(0)
            for instr, p in pos.data.items():
                if p['units'] < D('0.001'): continue
                ac = p['ac']
                if 'Money Market' in ac:
                    money_market += p['total_cost']
                else:
                    price = _latest_price(price_map, instr, current)
                    val   = p['units'] * price
                    if   'Derivative' in ac or 'Warrant' in ac: derivatives += val
                    elif 'Securities'  in ac:                    securities  += val
                    elif 'Real Estate' in ac or 'REIT'    in ac: reits       += val
                    elif 'Bond'        in ac:                    bonds       += val
                    else:                                        securities  += val

            total_assets = derivatives + securities + reits + bonds + money_market + receivables + cash

            # 6e. Fees
            gross_nta    = total_assets / total_units if total_units > 0 else D(0)
            daily_return = float(gross_nta / prev_nta) - 1 if prev_nta > 0 else 0.0

            base_fee = total_assets * D(base_sched['rate']) / 365 if base_sched else D(0)
            perf_fee = D(0)
            if perf_sched:
                try:
                    days_el    = max(1, (current - inception_date).days)
                    annualised = max(0.0, float(gross_nta)) ** (365.0 / days_el) - 1
                    hurdle     = float(D(perf_sched['hurdle_rate'] or 0))
                    if annualised > hurdle:
                        perf_fee = total_units * D(str((annualised-hurdle)/365)) * D(perf_sched['rate'])
                except Exception: pass

            acc_mng   = max(D(0), prev_mng  + base_fee - mgmt_w)
            acc_perf  = max(D(0), prev_perf + perf_fee - perf_w)
            total_liab = acc_mng + acc_perf + prev_i_fees + prev_loans + prev_i_lns

            # 6f. Balance sheet
            nav      = total_assets - total_liab
            earnings = nav - capital
            net_nta  = nav / total_units if total_units > 0 else D(0)

            # Write historical (ONE write per day)
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
            """, current,
                r2(derivatives), r2(securities), r2(reits), r2(bonds),
                r2(money_market), r2(receivables), r2(cash),
                r2(acc_mng), r2(acc_perf), r2(prev_i_fees),
                r2(prev_loans), r2(prev_i_lns),
                r2(capital), r2(earnings), float(total_units), r6(net_nta))

            # Update fund_settings once per day
            await db.execute("""
                UPDATE fund_settings SET current_nta=$1, aum=$2, last_nta_date=$3, updated_at=NOW()
                WHERE id=1
            """, r6(net_nta), r2(nav), current)

            prev_mng = acc_mng; prev_perf = acc_perf
            prev_nta = net_nta if net_nta > 0 else prev_nta
            computed += 1
            logger.info(f"  {current}: nta={r6(net_nta)} sec={r2(securities)} cash={r2(cash)}")

        except Exception as e:
            logger.error(f"  ✗ {current}: {e}\n{traceback.format_exc()}")
            errors += 1

        if computed % 10 == 0:
            await _job(db, 'running', start, today, current, computed, errors,
                      f"Computing {current} — {computed} done")
        current += timedelta(days=1)

    # ── Step 7: Save holdings ────────────────────────────────
    try:
        await db.execute("DELETE FROM holdings")
        for instr, p in pos.data.items():
            if p['units'] < D('0.001'): continue
            price = _latest_price(price_map, instr, today)
            units = float(p['units']); tc = float(p['total_cost']); avg = float(p['avg_cost'])
            mv = units * float(price) if price > 0 else tc
            upl = mv - tc; rp = upl / tc if tc > 0 else 0.0
            await db.execute("""
                INSERT INTO holdings (instrument, asset_class, units, vwap, total_costs,
                    last_price, market_value, unrealized_pl, return_pct, last_trade_date, updated_at)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,NOW())
                ON CONFLICT (instrument) DO UPDATE SET
                    asset_class=EXCLUDED.asset_class, units=EXCLUDED.units,
                    vwap=EXCLUDED.vwap, total_costs=EXCLUDED.total_costs,
                    last_price=EXCLUDED.last_price, market_value=EXCLUDED.market_value,
                    unrealized_pl=EXCLUDED.unrealized_pl, return_pct=EXCLUDED.return_pct,
                    updated_at=NOW()
            """, instr, p['ac'], units, avg, float(p['total_cost']),
                 float(price), mv, upl, rp, today)
        # Cash row
        await db.execute("""
            INSERT INTO holdings (instrument, asset_class, units, vwap, total_costs,
                last_price, market_value, unrealized_pl, return_pct, last_trade_date, updated_at)
            VALUES ('__CASH__','Cash',1,0,$1,0,$1,0,0,$2,NOW())
            ON CONFLICT (instrument) DO UPDATE SET
                total_costs=EXCLUDED.total_costs, market_value=EXCLUDED.market_value, updated_at=NOW()
        """, float(cash), today)
    except Exception as e:
        logger.error(f"Save holdings: {e}")

    # ── Step 8: Write settlements + mark computed ────────────
    for s in settlements:
        try:
            await db.execute("""
                INSERT INTO settlement
                    (date,instrument,asset_class,units,bought_price,sale_price,profit_loss,return_pct,remark)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,'system-computed')
            """, *s)
        except Exception as e:
            logger.warning(f"Settlement: {e}")

    for tbl in ['transactions','principal_cashflows','dividends','others','fee_withdrawals','distributions']:
        try:
            await db.execute(f"UPDATE {tbl} SET is_computed=TRUE")
        except Exception as e:
            logger.warning(f"Mark {tbl}: {e}")

    await _job(db, 'done' if errors==0 else 'error', start, today, today,
               computed, errors, f"Done — {computed} days ({start} → {today})")
    logger.info(f"Compute done: {computed} OK, {errors} errors")
    return {"computed": computed, "errors": errors,
            "from": str(start), "to": str(today)}


# Keep for backward compat
async def compute_daily_nta(db, target_date=None):
    return await compute_portfolio_and_nta(db, target_date)

async def compute_nta_range(db, from_date=None, to_date=None):
    return await compute_portfolio_and_nta(db, from_date)
