"""
NTA Engine v10  —  clean rebuild
"""
from datetime import date, timedelta
from decimal import Decimal, getcontext, ROUND_HALF_UP
from collections import defaultdict
import logging, traceback

logger = logging.getLogger(__name__)
getcontext().prec = 28

def D(v):
    return Decimal('0') if v is None else Decimal(str(v))
def r2(v):
    return float(D(v).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP))
def r6(v):
    return float(D(v).quantize(Decimal('0.000001'), rounding=ROUND_HALF_UP))


# ── Fetch price from yahooquery for a specific date ───────────
def _fetch_price_sync(ticker: str, for_date: date) -> float:
    """
    Get closing price for ticker on for_date.
    If no trading on that date, yahooquery returns last available close.
    """
    try:
        from yahooquery import Ticker
        t = Ticker(ticker, timeout=15)
        # Request history up to for_date + 5 days buffer, take last close <= for_date
        end = for_date + timedelta(days=5)
        hist = t.history(start=str(for_date - timedelta(days=7)), end=str(end))
        if hist is None or (hasattr(hist, 'empty') and hist.empty):
            return 0.0
        import pandas as pd
        if isinstance(hist, pd.DataFrame):
            hist = hist.reset_index()
            # Filter rows where date <= for_date
            date_col = 'date' if 'date' in hist.columns else hist.columns[0]
            hist[date_col] = pd.to_datetime(hist[date_col]).dt.date
            hist = hist[hist[date_col] <= for_date]
            if hist.empty:
                return 0.0
            return float(hist['close'].iloc[-1])
        return 0.0
    except Exception as e:
        logger.warning(f"yahooquery {ticker} {for_date}: {e}")
        return 0.0


async def _get_price(db, instrument: str, for_date: date, ticker_map: dict) -> Decimal:
    """Get price: yahooquery if ticker exists, else price_history fallback."""
    import asyncio
    ticker = ticker_map.get(instrument, {}).get('yahoo_ticker')
    if ticker:
        loop = asyncio.get_event_loop()
        price = await loop.run_in_executor(None, _fetch_price_sync, ticker, for_date)
        if price > 0:
            # Save to price_history for record
            try:
                await db.execute("""
                    INSERT INTO price_history (instrument, date, price, source)
                    VALUES ($1,$2,$3,'yahoo')
                    ON CONFLICT (instrument, date) DO UPDATE SET price=EXCLUDED.price
                """, instrument, for_date, price)
            except Exception:
                pass
            return D(str(price))
    # Fallback: latest from price_history
    row = await db.fetchrow("""
        SELECT price FROM price_history
        WHERE instrument=$1 AND date<=$2
        ORDER BY date DESC LIMIT 1
    """, instrument, for_date)
    return D(row['price']) if row else D(0)


# ── Job status ────────────────────────────────────────────────
async def _job(db, status, from_d=None, to_d=None, proc=None, comp=0, err=0, msg=''):
    try:
        await db.execute("""
            INSERT INTO compute_job
                (id,status,started_at,finished_at,from_date,to_date,
                 processing_date,computed,errors,message,updated_at)
            VALUES (1,$1,
                CASE WHEN $1='running' THEN NOW()
                     ELSE (SELECT started_at FROM compute_job WHERE id=1) END,
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
        logger.warning(f"job status: {e}")


# ── Positions ─────────────────────────────────────────────────
class Positions:
    def __init__(self):
        self.data = {}  # instrument → {units, total_cost, avg_cost, ac}

    def buy(self, instr, units, net_cost, ac):
        if instr not in self.data:
            self.data[instr] = {'units': D(0), 'total_cost': D(0),
                                'avg_cost': D(0), 'ac': ac}
        p = self.data[instr]
        p['total_cost'] += net_cost
        p['units']      += units
        p['avg_cost']    = p['total_cost'] / p['units'] if p['units'] > 0 else D(0)
        p['ac']          = ac

    def sell(self, instr, units_sold):
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


# ── MAIN ──────────────────────────────────────────────────────
async def compute_portfolio_and_nta(db, force_from: date = None) -> dict:
    today = date.today()

    # ── Step 1: Find earliest is_computed=FALSE (date a) ─────
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
                if row and row[0]: candidates.append(row[0])
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

    # ── Step 3: Load historical[c] as base ───────────────────
    base = await db.fetchrow(
        "SELECT * FROM historical WHERE date <= $1 ORDER BY date DESC LIMIT 1", c)
    if not base:
        return {"computed": 0, "message": f"No historical row on or before {c}"}

    base_date = base['date']
    logger.info(f"Base: {base_date} | Start: {start} → {today}")
    await _job(db, 'running', start, today, start, 0, 0, 'Building holdings...')

    # ── Load ticker_map (yahoo_ticker + asset_class) ──────────
    tm_rows = await db.fetch("SELECT instrument, yahoo_ticker, asset_class FROM ticker_map")
    ticker_map = {r['instrument']: {'yahoo_ticker': r['yahoo_ticker'],
                                    'ac': r['asset_class']} for r in tm_rows}

    # ── Step 4: Build holdings as-of base_date from transactions
    pos = Positions()
    all_trades = await db.fetch("""
        SELECT date, instrument, units, net_amount,
               COALESCE(asset_class,'Securities [H]') AS ac
        FROM transactions WHERE date <= $1
        ORDER BY date ASC, created_at ASC
    """, base_date)
    for t in all_trades:
        units = D(t['units']); instr = t['instrument']
        ac    = ticker_map.get(instr, {}).get('ac') or t['ac']
        if units > 0:
            pos.buy(instr, units, abs(D(t['net_amount'])), ac)
        elif units < 0:
            pos.sell(instr, abs(units))

    # Cash and carried values from historical[base_date]
    cash        = D(base['cash'])
    receivables = D(base['receivables'])
    prev_mng    = D(base['mng_fees'])
    prev_perf   = D(base['perf_fees'])
    prev_i_fees = D(base['ints_on_fees'])
    prev_loans  = D(base['loans'])
    prev_i_lns  = D(base['ints_on_loans'])
    prev_nta    = D(base['nta'])

    # Capital cumulative up to base_date
    cap_row = await db.fetchrow(
        "SELECT COALESCE(SUM(amount),0) AS n FROM principal_cashflows WHERE date<=$1", base_date)
    capital = D(cap_row['n'])

    # Total units up to base_date
    tu_row = await db.fetchrow(
        "SELECT COALESCE(SUM(units),0) AS n FROM principal_cashflows WHERE date<=$1", base_date)
    last_units = D(tu_row['n']) if tu_row and D(tu_row['n']) > 0 else D(base['total_units'])

    # Fee schedules
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

    # ── Step 5: Loop d = start → today ───────────────────────
    computed = 0; errors = 0; settlements = []
    current = start

    while current <= today:
        try:
            # 5a. Apply trades on d
            trades_today = await db.fetch("""
                SELECT instrument, units, price, net_amount, is_computed,
                       COALESCE(asset_class,'Securities [H]') AS ac
                FROM transactions WHERE date=$1
                ORDER BY created_at ASC
            """, current)

            for t in trades_today:
                units  = D(t['units']); net = D(t['net_amount'])
                instr  = t['instrument']
                ac     = ticker_map.get(instr, {}).get('ac') or t['ac']
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
                            r2(pl),
                            r6(pl / cost_basis if cost_basis > 0 else D(0))
                        ))

            # 5b. Cash movements
            cf = await db.fetchrow(
                "SELECT COALESCE(SUM(amount),0) AS n, COALESCE(SUM(units),0) AS u "
                "FROM principal_cashflows WHERE date=$1", current)
            cf_amt = D(cf['n']); cf_units = D(cf['u'])
            cash    += cf_amt
            capital += cf_amt
            last_units += cf_units

            dp = await db.fetchrow(
                "SELECT COALESCE(SUM(amount),0) AS n FROM dividends WHERE pmt_date=$1", current)
            cash += D(dp['n'])

            try:
                ot = await db.fetchrow(
                    "SELECT COALESCE(SUM(amount),0) AS n FROM others WHERE record_date=$1", current)
                cash += D(ot['n'])
            except Exception: pass

            mgmt_w = D(0); perf_w = D(0)
            try:
                fw = await db.fetch(
                    "SELECT fee_type, COALESCE(SUM(amount),0) AS n "
                    "FROM fee_withdrawals WHERE date=$1 GROUP BY fee_type", current)
                for row in fw:
                    if row['fee_type'] == 'management': mgmt_w = D(row['n'])
                    else:                               perf_w = D(row['n'])
                cash -= mgmt_w + perf_w
            except Exception: pass

            try:
                dist = await db.fetchrow(
                    "SELECT COALESCE(SUM(total_dividend),0) AS n FROM distributions WHERE pmt_date=$1", current)
                cash -= D(dist['n'])
            except Exception: pass

            # 5c. Receivables
            ex_row = await db.fetchrow(
                "SELECT COALESCE(SUM(amount),0) AS n FROM dividends WHERE ex_date=$1", current)
            receivables += D(ex_row['n'])
            receivables -= D(dp['n'])
            if receivables < 0: receivables = D(0)

            # 5d. Get prices from yahooquery for date d, compute market values
            total_units  = last_units if last_units > 0 else D(base['total_units'])
            derivatives  = securities = reits = bonds = money_market = D(0)

            for instr, p in pos.data.items():
                if p['units'] < D('0.001'): continue
                ac = p['ac']
                if 'Money Market' in ac:
                    money_market += p['total_cost']
                else:
                    price = await _get_price(db, instr, current, ticker_map)
                    val   = p['units'] * price
                    if   'Derivative' in ac or 'Warrant' in ac: derivatives += val
                    elif 'Securities'  in ac:                    securities  += val
                    elif 'Real Estate' in ac or 'REIT'    in ac: reits       += val
                    elif 'Bond'        in ac:                    bonds       += val
                    else:                                        securities  += val

            total_assets = derivatives + securities + reits + bonds + \
                           money_market + receivables + cash

            # 5e. Liabilities
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

            # 5f. NTA
            nav      = total_assets - total_liab
            earnings = nav - capital
            net_nta  = nav / total_units if total_units > 0 else D(0)

            # 5g. Write historical
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

        await _job(db, 'running', start, today, current, computed, errors,
                   f"{current} — {computed} done")
        current += timedelta(days=1)

    # ── Step 6: Save holdings snapshot ───────────────────────
    try:
        await db.execute("DELETE FROM holdings")
        for instr, p in pos.data.items():
            if p['units'] < D('0.001'): continue
            price = await _get_price(db, instr, today, ticker_map)
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
            """, instr, p['ac'], units, avg, tc, float(price), mv, upl, rp, today)
        # Cash row
        await db.execute("""
            INSERT INTO holdings (instrument, asset_class, units, vwap, total_costs,
                last_price, market_value, unrealized_pl, return_pct, last_trade_date, updated_at)
            VALUES ('__CASH__','Cash',1,0,$1,0,$1,0,0,$2,NOW())
            ON CONFLICT (instrument) DO UPDATE SET
                total_costs=EXCLUDED.total_costs, market_value=EXCLUDED.market_value, updated_at=NOW()
        """, float(cash), today)
    except Exception as e:
        logger.error(f"Save holdings: {e}\n{traceback.format_exc()}")

    # ── Step 7: Settlements + mark computed ──────────────────
    for s in settlements:
        try:
            await db.execute("""
                INSERT INTO settlement
                    (date,instrument,asset_class,units,bought_price,sale_price,
                     profit_loss,return_pct,remark)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,'system-computed')
            """, *s)
        except Exception as e:
            logger.warning(f"Settlement: {e}")

    for tbl in ['transactions','principal_cashflows','dividends',
                'others','fee_withdrawals','distributions']:
        try:
            await db.execute(f"UPDATE {tbl} SET is_computed=TRUE")
        except Exception as e:
            logger.warning(f"Mark {tbl}: {e}")

    await _job(db, 'done' if errors==0 else 'error', start, today, today,
               computed, errors, f"Done — {computed} days ({start} → {today})")
    logger.info(f"Done: {computed} OK, {errors} errors")
    return {"computed": computed, "errors": errors,
            "from": str(start), "to": str(today)}


# Aliases
async def compute_daily_nta(db, target_date=None):
    return await compute_portfolio_and_nta(db, target_date)

async def compute_nta_range(db, from_date=None, to_date=None):
    return await compute_portfolio_and_nta(db, from_date)
