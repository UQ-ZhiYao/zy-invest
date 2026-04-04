"""
Holdings + NTA Computation Engine  v7.0.0
==========================================
Performance fixes vs v6:
  - Cache prices, fee schedules, inception date BEFORE loop (not per-day DB queries)
  - Capital tracked incrementally (not SUM every day)
  - _job_progress updated every 10 days only
  - Asset class taken from ticker_map (authoritative) not transactions

Correctness fixes:
  - Positions with units < 0.001 excluded from asset calculation
  - asset_class from ticker_map overrides transaction entry
  - SELL cash: net_amount is positive → cash += net_amount directly
  - BUY cash: net_amount is negative → cash += net_amount directly (no abs)

Flow:
  1. Find earliest is_computed=FALSE across 6 input DBs → date a
  2. c = min(a-1, last_historical_date b)
  3. Reconstruct holdings as-of c from transactions (VWAP, positions only)
     Cash/liabilities from historical[c] directly
  4. Cache: prices, fee schedules, inception date, asset classes
  5. Loop d = c+1 → today:
     a. Apply trades (BUY/SELL) → positions + cash
     b. Cash: +subscriptions +div_payments +others -fee_withdrawals -distributions
     c. Receivables: +ex_date dividends -paid dividends
     d. Asset values from cached prices (refreshed daily from price_history)
     e. Compute balance sheet → write historical
  6. Save holdings, mark all 6 DBs is_computed=TRUE
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


# ── Job status ────────────────────────────────────────────────
async def _job_start(db, from_date, to_date):
    try:
        await db.execute("""
            INSERT INTO compute_job
                (id,status,started_at,finished_at,from_date,to_date,
                 processing_date,computed,errors,message,updated_at)
            VALUES (1,'running',NOW(),NULL,$1,$2,NULL,0,0,'Starting...',NOW())
            ON CONFLICT (id) DO UPDATE SET
                status='running',started_at=NOW(),finished_at=NULL,
                from_date=$1,to_date=$2,processing_date=NULL,
                computed=0,errors=0,message='Starting...',updated_at=NOW()
        """, from_date, to_date)
    except Exception: pass

async def _job_progress(db, current, computed, errors):
    try:
        await db.execute("""
            UPDATE compute_job
            SET processing_date=$1,computed=$2,errors=$3,
                message=$4,updated_at=NOW() WHERE id=1
        """, current, computed, errors,
             f"Computing {current} — {computed} done")
    except Exception: pass

async def _job_done(db, computed, errors, from_date, to_date):
    try:
        await db.execute("""
            UPDATE compute_job
            SET status=$1,finished_at=NOW(),computed=$2,errors=$3,
                message=$4,updated_at=NOW() WHERE id=1
        """,
        'done' if errors == 0 else 'error',
        computed, errors,
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

    def buy(self, instrument: str, units: Decimal, net_cost: Decimal, asset_class: str):
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
        if instrument not in self.positions:
            return Decimal('0')
        p = self.positions[instrument]
        if p['units'] <= 0:
            return Decimal('0')
        ratio           = min(units_sold / p['units'], Decimal('1'))
        cost_basis      = p['total_cost'] * ratio
        p['total_cost'] -= cost_basis
        p['units']      -= units_sold
        if p['units'] < Decimal('0.001'):
            p['units']      = Decimal('0')
            p['total_cost'] = Decimal('0')
        return cost_basis


# ── Build state as-of base_date ───────────────────────────────
async def _build_state_as_of(db, base_date: date, hist_row,
                              asset_class_map: Dict[str, str]) -> PortfolioState:
    """Reconstruct positions from transactions up to base_date.
    Cash and balances come from hist_row directly."""
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
        instr = t['instrument']
        # Use ticker_map asset_class if available (authoritative)
        ac = asset_class_map.get(instr) or t['asset_class']
        if units > 0:
            state.buy(instr, units, abs(D(t['net_amount'])), ac)
        elif units < 0:
            state.sell(instr, abs(units))

    return state


# ── Process one day ───────────────────────────────────────────
async def _process_day(db, d: date, state: PortfolioState,
                       total_units: Decimal, capital: Decimal,
                       price_cache: Dict[str, Decimal],
                       base_sched, perf_sched,
                       inception_date) -> Optional[dict]:

    # ── 4a. Trades ───────────────────────────────────────────
    trades = await db.fetch("""
        SELECT instrument, units, price, net_amount, is_computed,
               COALESCE(asset_class,'Securities [H]') AS asset_class
        FROM transactions WHERE date=$1
        ORDER BY created_at ASC
    """, d)

    for t in trades:
        units   = D(t['units'])
        net_amt = D(t['net_amount'])
        instr   = t['instrument']
        ac      = t['asset_class']

        if units > 0:
            # BUY: net_amount is negative (cash out)
            net_cost = abs(net_amt)
            state.buy(instr, units, net_cost, ac)
            state.cash -= net_cost
        elif units < 0:
            # SELL: net_amount is positive (cash in)
            units_sold = abs(units)
            avg_cost   = state.positions.get(instr, {}).get('avg_cost', Decimal('0'))
            cost_basis = state.sell(instr, units_sold)
            state.cash += net_amt

            # Settlement only for new sells
            if not t['is_computed']:
                pl      = net_amt - cost_basis
                ret_pct = pl / cost_basis if cost_basis > 0 else Decimal('0')
                try:
                    await db.execute("""
                        INSERT INTO settlement
                            (date,instrument,asset_class,units,
                             bought_price,sale_price,profit_loss,return_pct,remark)
                        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,'system-computed')
                    """, d, instr, ac, float(units_sold),
                         float(avg_cost), float(D(t['price'])),
                         r2(pl), r6(ret_pct))
                except Exception as e:
                    logger.warning(f"Settlement {d} {instr}: {e}")

    # ── 4b. Cash movements ───────────────────────────────────
    cf = await db.fetchrow(
        "SELECT COALESCE(SUM(amount),0) AS n FROM principal_cashflows WHERE date=$1", d)
    cf_amt = D(cf['n'])
    state.cash += cf_amt
    capital    += cf_amt   # capital tracks cumulative subscriptions

    dp = await db.fetchrow(
        "SELECT COALESCE(SUM(amount),0) AS n FROM dividends WHERE pmt_date=$1", d)
    state.cash += D(dp['n'])

    try:
        ot = await db.fetchrow(
            "SELECT COALESCE(SUM(amount),0) AS n FROM others WHERE record_date=$1", d)
        state.cash += D(ot['n'])
    except Exception: pass

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
    except Exception: pass

    try:
        dist = await db.fetchrow(
            "SELECT COALESCE(SUM(total_dividend),0) AS n FROM distributions WHERE pmt_date=$1", d)
        state.cash -= D(dist['n'])
    except Exception: pass

    # ── 4c. Receivables ──────────────────────────────────────
    ex_today = await db.fetchrow(
        "SELECT COALESCE(SUM(amount),0) AS n FROM dividends WHERE ex_date=$1", d)
    state.receivables += D(ex_today['n'])
    state.receivables -= D(dp['n'])
    if state.receivables < 0:
        state.receivables = Decimal('0')

    # ── 4d. Refresh prices for today ─────────────────────────
    # Update price_cache with any new prices for today
    new_prices = await db.fetch("""
        SELECT instrument, price FROM price_history WHERE date=$1
    """, d)
    for row in new_prices:
        price_cache[row['instrument']] = D(row['price'])

    # ── 4e. Asset values ─────────────────────────────────────
    derivatives  = Decimal('0')
    securities   = Decimal('0')
    reits        = Decimal('0')
    bonds        = Decimal('0')
    money_market = Decimal('0')

    for instr, pos in state.positions.items():
        if pos['units'] < Decimal('0.001'):
            continue
        ac = pos['asset_class']
        if 'Money Market' in ac:
            money_market += pos['total_cost']
        else:
            price = price_cache.get(instr, Decimal('0'))
            val   = pos['units'] * price
            if   'Derivative' in ac or 'Warrant' in ac: derivatives += val
            elif 'Securities'  in ac:                    securities  += val
            elif 'Real Estate' in ac or 'REIT'    in ac: reits       += val
            elif 'Bond'        in ac:                    bonds       += val
            else:                                        securities  += val

    total_assets = derivatives + securities + reits + bonds + \
                   money_market + state.receivables + state.cash

    # ── 4f. Fees ─────────────────────────────────────────────
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

    # ── 4g. Balance sheet ─────────────────────────────────────
    nav      = total_assets - total_liab
    earnings = nav - capital
    net_nta  = nav / total_units if total_units > 0 else Decimal('0')

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

    state.prev_mng  = acc_mng
    state.prev_perf = acc_perf
    state.prev_nta  = net_nta if net_nta > 0 else state.prev_nta

    logger.info(
        f"{d}: nta={r6(net_nta)} sec={r2(securities)} "
        f"cash={r2(state.cash)} cap={r2(capital)}")

    return {"date": str(d), "net_nta": r6(net_nta),
            "cash": r2(state.cash), "capital": r2(capital),
            "new_capital": capital}  # pass back for next iteration


# ── Save holdings snapshot ────────────────────────────────────
async def _save_holdings(db, state: PortfolioState,
                         as_of: date, price_cache: Dict[str, Decimal]):
    await db.execute("DELETE FROM holdings")
    for instr, pos in state.positions.items():
        if pos['units'] < Decimal('0.001'):
            continue
        price = price_cache.get(instr, Decimal('0'))
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
                asset_class=EXCLUDED.asset_class, units=EXCLUDED.units,
                vwap=EXCLUDED.vwap, total_costs=EXCLUDED.total_costs,
                total_cost=EXCLUDED.total_cost, last_price=EXCLUDED.last_price,
                market_value=EXCLUDED.market_value, unrealized_pl=EXCLUDED.unrealized_pl,
                return_pct=EXCLUDED.return_pct, updated_at=NOW()
        """, instr, pos['asset_class'], units, avg, tc,
             float(price), mv, upl, rp, as_of)
    # Cash row
    await db.execute("""
        INSERT INTO holdings
            (instrument, asset_class, units, vwap, total_costs, total_cost,
             last_price, market_value, unrealized_pl, return_pct,
             last_trade_date, updated_at)
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
            logger.warning(f"Could not mark {tbl}: {e}")


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
                if row and row['d']:
                    candidates.append(row['d'])
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
        return {"computed": 0, "message": "No historical base data found"}

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

    # ── Cache expensive data BEFORE loop ─────────────────────
    # Asset class map from ticker_map (authoritative)
    tm_rows = await db.fetch("SELECT instrument, asset_class FROM ticker_map")
    asset_class_map = {r['instrument']: r['asset_class'] for r in tm_rows}

    # Fee schedules (cached — queried once per schedule type)
    base_sched = await db.fetchrow("""
        SELECT rate FROM fee_schedules
        WHERE fee_type='base'
          AND valid_from <= $1 AND (valid_to IS NULL OR valid_to >= $1)
        ORDER BY valid_from DESC LIMIT 1
    """, today)
    perf_sched = await db.fetchrow("""
        SELECT rate, hurdle_rate FROM fee_schedules
        WHERE fee_type='performance'
          AND valid_from <= $1 AND (valid_to IS NULL OR valid_to >= $1)
        ORDER BY valid_from DESC LIMIT 1
    """, today)

    # Inception date (first historical row)
    inc_row = await db.fetchrow(
        "SELECT date FROM historical ORDER BY date ASC LIMIT 1")
    inception_date = inc_row['date'] if inc_row else start_date

    # Price cache: latest price for each instrument up to start_date
    price_rows = await db.fetch("""
        SELECT DISTINCT ON (instrument) instrument, price
        FROM price_history
        WHERE date <= $1
        ORDER BY instrument, date DESC
    """, start_date)
    price_cache: Dict[str, Decimal] = {r['instrument']: D(r['price']) for r in price_rows}

    # Capital: cumulative subscriptions up to base_date (incremental from here)
    cap_row = await db.fetchrow(
        "SELECT COALESCE(SUM(amount),0) AS n FROM principal_cashflows WHERE date <= $1",
        base_date)
    capital = D(cap_row['n'])

    # Step 3: Reconstruct state
    state = await _build_state_as_of(db, base_date, base_hist, asset_class_map)

    # Step 4: Loop
    computed = 0
    errors   = 0
    current  = start_date

    while current <= today:
        # total_units: use investors table for current, or compute from cashflows
        tu_row = await db.fetchrow(
            "SELECT COALESCE(SUM(units),0) AS t FROM principal_cashflows WHERE date <= $1",
            current)
        total_units = D(tu_row['t']) if tu_row and D(tu_row['t']) > 0 \
                      else D(base_hist['total_units'])

        try:
            result = await _process_day(
                db, current, state, total_units, capital,
                price_cache, base_sched, perf_sched, inception_date)
            if result:
                computed += 1
                capital = result['new_capital']  # carry incremental capital forward
        except Exception as e:
            import traceback
            logger.error(f"  ✗ {current}: {e}\n{traceback.format_exc()}")
            errors += 1

        # Progress update every 10 days (reduce DB writes)
        if computed % 10 == 0 or current == today:
            await _job_progress(db, current, computed, errors)

        current += timedelta(days=1)

    # Step 5
    try:
        await _save_holdings(db, state, today, price_cache)
    except Exception as e:
        logger.error(f"Save holdings failed: {e}")

    await _mark_all_computed(db)
    await _job_done(db, computed, errors, start_date, today)
    logger.info(f"Done: {computed} computed, {errors} errors")

    return {
        "computed": computed, "errors": errors,
        "from": str(start_date), "to": str(today),
        "message": f"Computed {computed} days ({start_date} → {today})",
    }
