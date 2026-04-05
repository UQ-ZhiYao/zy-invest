"""
NTA Engine v11  —  clean rebuild against live schema
=====================================================
Flow:
  1. Find latest date (d) in historical table
  2. Load holdings from DB (positions already computed)
  3. Load cash from historical[d].cash
  4. Load all 6 input DBs for the date range [d+1 .. today]
  5. Loop date by date from d+1 to today:
       a. Apply transactions on date (BUYs first, then SELLs) → positions + cash
       b. Cash movements: +principal +others +div_pmt -distributions -fee_withdrawals
       c. Receivables: +div on ex_date, -div on pmt_date (move to cash)
       d. Fetch prices from yahooquery for date (latest available)
       e. Compute assets, liabilities, NAV, NTA
       f. Write to historical
  6. Update holdings table with final positions + prices
"""
from datetime import date, timedelta
from decimal import Decimal, getcontext, ROUND_HALF_UP
from collections import defaultdict
import logging, traceback

logger = logging.getLogger(__name__)
getcontext().prec = 28

def D(v):
    if v is None: return Decimal('0')
    return Decimal(str(v))
def r2(v): return float(D(v).quantize(Decimal('0.01'),       rounding=ROUND_HALF_UP))
def r4(v): return float(D(v).quantize(Decimal('0.0001'),     rounding=ROUND_HALF_UP))
def r6(v): return float(D(v).quantize(Decimal('0.000001'),   rounding=ROUND_HALF_UP))
def r8(v): return float(D(v).quantize(Decimal('0.00000001'), rounding=ROUND_HALF_UP))


# ── Job status ────────────────────────────────────────────────
async def _job(db, status, from_d=None, to_d=None, proc=None,
               comp=0, err=0, msg=''):
    try:
        await db.execute("""
            INSERT INTO compute_job
                (id, status, started_at, finished_at, from_date, to_date,
                 processing_date, computed, errors, message, updated_at)
            VALUES (1, $1,
                CASE WHEN $1='running' THEN NOW()
                     ELSE (SELECT started_at FROM compute_job WHERE id=1) END,
                CASE WHEN $1 IN ('done','error') THEN NOW() ELSE NULL END,
                $2,$3,$4,$5,$6,$7, NOW())
            ON CONFLICT (id) DO UPDATE SET
                status=$1,
                started_at = CASE WHEN $1='running' THEN NOW()
                                  ELSE compute_job.started_at END,
                finished_at = CASE WHEN $1 IN ('done','error') THEN NOW()
                                   ELSE NULL END,
                from_date=$2, to_date=$3, processing_date=$4,
                computed=$5, errors=$6, message=$7, updated_at=NOW()
        """, status, from_d, to_d, proc, comp, err, msg)
    except Exception as e:
        logger.warning(f"Job status write: {e}")


# ── Fetch price from yahooquery for a specific date ───────────
def _fetch_price(ticker: str, for_date: date) -> float:
    """
    Get latest available closing price on or before for_date.
    Works on weekends/holidays by returning last trading day close.
    """
    try:
        from yahooquery import Ticker
        import pandas as pd
        t    = Ticker(ticker, timeout=15)
        # Fetch a window ending at for_date
        start = for_date - timedelta(days=10)
        hist  = t.history(start=str(start), end=str(for_date + timedelta(days=1)))
        if hist is None or (hasattr(hist, 'empty') and hist.empty):
            return 0.0
        if isinstance(hist, pd.DataFrame):
            hist = hist.reset_index()
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


async def _get_price(db, instrument: str, for_date: date,
                     ticker_map: dict, price_cache: dict) -> Decimal:
    """
    Get price for instrument on for_date.
    Cache: check in-memory first, then DB price_history, then yahooquery.
    """
    # In-memory cache hit
    cache_key = (instrument, for_date)
    if cache_key in price_cache:
        return price_cache[cache_key]

    ticker = ticker_map.get(instrument, {}).get('yahoo_ticker')

    if ticker:
        import asyncio
        loop  = asyncio.get_event_loop()
        price = await loop.run_in_executor(None, _fetch_price, ticker, for_date)
        if price > 0:
            price_cache[cache_key] = D(str(price))
            # Save to price_history
            try:
                await db.execute("""
                    INSERT INTO price_history (instrument, date, price, source)
                    VALUES ($1,$2,$3,'yahoo')
                    ON CONFLICT (instrument, date) DO UPDATE
                        SET price=EXCLUDED.price
                """, instrument, for_date, price)
            except Exception:
                pass
            return price_cache[cache_key]

    # Fallback: latest from price_history
    row = await db.fetchrow("""
        SELECT price FROM price_history
        WHERE instrument=$1 AND date<=$2
        ORDER BY date DESC LIMIT 1
    """, instrument, for_date)
    p = D(row['price']) if row else D(0)
    price_cache[cache_key] = p
    return p


# ── MAIN ──────────────────────────────────────────────────────
async def compute_portfolio_and_nta(db, force_from: date = None) -> dict:
    today = date.today()

    # ── Step 1: Latest historical date ───────────────────────
    row_d = await db.fetchrow("SELECT MAX(date) AS d FROM historical")
    last_hist_date = row_d['d'] if row_d and row_d['d'] else None
    if not last_hist_date:
        return {"computed": 0, "message": "No historical base data found"}

    start = force_from if force_from else last_hist_date + timedelta(days=1)
    if start > today:
        return {"computed": 0, "message": f"Already up to date (last: {last_hist_date})"}

    # Load base historical row
    base = await db.fetchrow(
        "SELECT * FROM historical WHERE date=$1", last_hist_date)
    if not base:
        return {"computed": 0, "message": f"No historical row for {last_hist_date}"}

    logger.info(f"NTA compute: {last_hist_date} → {start} to {today}")
    await _job(db, 'running', start, today, start, 0, 0, 'Loading...')

    # ── Step 2: Load holdings from DB ────────────────────────
    # positions[instr] = {units, total_cost, ac}
    positions = {}
    h_rows = await db.fetch("""
        SELECT instrument, asset_class, units, avg_cost, total_cost
        FROM holdings
        WHERE instrument != '__CASH__'
    """)
    for h in h_rows:
        if D(h['units']) > D('0.0001'):
            positions[h['instrument']] = {
                'units':      D(h['units']),
                'total_cost': D(h['total_cost']),
                'ac':         h['asset_class'] or 'Securities [H]',
            }

    # ── Step 3: Load state from historical[last_hist_date] ───
    cash        = D(base['cash'])
    receivables = D(base['receivables'])
    prev_mng    = D(base['mng_fees'])
    prev_perf   = D(base['perf_fees'])
    prev_i_fees = D(base['ints_on_fees'])
    prev_loans  = D(base['loans'])
    prev_i_lns  = D(base['ints_on_loans'])
    prev_nta    = D(base['nta'])

    # Cumulative capital and total_units up to last_hist_date
    cap_row = await db.fetchrow(
        "SELECT COALESCE(SUM(amount),0) AS n FROM principal_cashflows WHERE date<=$1",
        last_hist_date)
    capital = D(cap_row['n'])

    tu_row = await db.fetchrow(
        "SELECT COALESCE(SUM(units),0) AS n FROM principal_cashflows WHERE date<=$1",
        last_hist_date)
    total_units = D(tu_row['n']) if D(tu_row['n']) > 0 else D(base['total_units'])

    # ── Load ticker_map ───────────────────────────────────────
    tm_rows = await db.fetch(
        "SELECT instrument, yahoo_ticker, asset_class FROM ticker_map")
    ticker_map = {r['instrument']: {
        'yahoo_ticker': r['yahoo_ticker'],
        'ac':           r['asset_class']
    } for r in tm_rows}

    # ── Fee schedules (cached) ────────────────────────────────
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
    inception_date = inc_row['date'] if inc_row else start

    # ── Step 4: Loop ──────────────────────────────────────────
    computed   = 0
    errors     = 0
    price_cache = {}
    settlements = []
    current    = start

    while current <= today:
        try:
            # ── 4a. Transactions on date (BUYs first, SELLs after) ──
            trades = await db.fetch("""
                SELECT instrument, asset_class, units, price, net_amount
                FROM transactions
                WHERE date = $1
                ORDER BY CASE WHEN units > 0 THEN 0 ELSE 1 END ASC,
                         created_at ASC
            """, current)

            for t in trades:
                instr = t['instrument']
                units = D(t['units'])
                net   = D(t['net_amount'])
                ac    = ticker_map.get(instr, {}).get('ac') or t['asset_class'] or 'Securities [H]'

                if instr not in positions:
                    positions[instr] = {
                        'units': D(0), 'total_cost': D(0), 'ac': ac}
                p = positions[instr]

                if units > D(0):
                    # BUY
                    cost            = abs(net)
                    p['units']      += units
                    p['total_cost'] += cost
                    cash            -= cost

                elif units < D(0):
                    # SELL
                    sell_u   = abs(units)
                    proceeds = abs(net)
                    cash    += proceeds

                    if p['units'] > D(0):
                        sell_u     = min(sell_u, p['units'])
                        avg_cost   = p['total_cost'] / p['units']
                        cost_basis = avg_cost * sell_u
                        pl         = proceeds - cost_basis
                        sale_price = proceeds / sell_u
                        ret_pct    = (pl / cost_basis * 100) if cost_basis > D(0) else D(0)
                        proportion       = sell_u / p['units']
                        p['total_cost'] -= p['total_cost'] * proportion
                        p['units']      -= sell_u
                        if p['units'] < D('0.0001'):
                            p['units'] = D(0); p['total_cost'] = D(0)
                    else:
                        avg_cost = cost_basis = D(0)
                        pl = proceeds
                        sale_price = proceeds / sell_u if sell_u > D(0) else D(0)
                        ret_pct    = D(0)

                    settlements.append((
                        current, 'MY', ac, '', instr,
                        r8(sell_u), r8(avg_cost), r8(sale_price),
                        r6(cost_basis), r6(proceeds),
                        r4(pl), r4(ret_pct)
                    ))

            # ── 4b. Cash movements on date ───────────────────
            # Principal cashflows
            cf = await db.fetchrow(
                "SELECT COALESCE(SUM(amount),0) n, COALESCE(SUM(units),0) u "
                "FROM principal_cashflows WHERE date=$1", current)
            cash        += D(cf['n'])
            capital     += D(cf['n'])
            total_units += D(cf['u'])

            # Others
            try:
                ot = await db.fetchrow(
                    "SELECT COALESCE(SUM(amount),0) n FROM others WHERE record_date=$1",
                    current)
                cash += D(ot['n'])
            except Exception: pass

            # Dividends paid (pmt_date) → move from receivables to cash
            dp = await db.fetchrow(
                "SELECT COALESCE(SUM(amount),0) n FROM dividends WHERE pmt_date=$1",
                current)
            div_paid = D(dp['n'])
            cash        += div_paid
            receivables -= div_paid
            if receivables < D(0): receivables = D(0)

            # Distributions paid out
            try:
                dist = await db.fetchrow(
                    "SELECT COALESCE(SUM(total_dividend),0) n FROM distributions WHERE pmt_date=$1",
                    current)
                cash -= D(dist['n'])
            except Exception: pass

            # Fee withdrawals
            mgmt_w = D(0); perf_w = D(0)
            try:
                fw = await db.fetch(
                    "SELECT fee_type, COALESCE(SUM(amount),0) n "
                    "FROM fee_withdrawals WHERE date=$1 GROUP BY fee_type", current)
                for row in fw:
                    if row['fee_type'] == 'management': mgmt_w = D(row['n'])
                    else:                               perf_w = D(row['n'])
                cash -= (mgmt_w + perf_w)
            except Exception: pass

            # ── 4c. Receivables ──────────────────────────────
            # Dividends going ex today → entitled, add to receivables
            ex = await db.fetchrow(
                "SELECT COALESCE(SUM(amount),0) n FROM dividends WHERE ex_date=$1",
                current)
            receivables += D(ex['n'])

            # ── 4d. Asset values via yahooquery ──────────────
            derivatives = securities = reits = bonds = money_market = D(0)

            for instr, p in positions.items():
                if p['units'] < D('0.0001'): continue
                ac = p['ac']
                if 'Money Market' in ac:
                    money_market += p['total_cost']
                else:
                    price = await _get_price(db, instr, current, ticker_map, price_cache)
                    val   = p['units'] * price
                    if   'Derivative' in ac or 'Warrant' in ac: derivatives += val
                    elif 'Securities'  in ac:                    securities  += val
                    elif 'Real Estate' in ac or 'REIT'    in ac: reits       += val
                    elif 'Bond'        in ac:                    bonds       += val
                    else:                                        securities  += val

            total_assets = (derivatives + securities + reits + bonds +
                            money_market + receivables + cash)

            # ── 4e. Liabilities ──────────────────────────────
            gross_nta    = total_assets / total_units if total_units > D(0) else D(0)
            daily_return = float(gross_nta / prev_nta) - 1 if prev_nta > D(0) else 0.0

            # Base fee: AUM × rate / 365
            base_fee = total_assets * D(base_sched['rate']) / 365 if base_sched else D(0)

            # Performance fee: total_units × excess × (rate/365)
            perf_fee = D(0)
            if perf_sched:
                try:
                    days_el    = max(1, (current - inception_date).days)
                    annualised = max(0.0, float(gross_nta)) ** (365.0 / days_el) - 1
                    hurdle     = float(D(perf_sched['hurdle_rate'] or 0))
                    if annualised > hurdle:
                        excess   = annualised - hurdle
                        perf_fee = total_units * D(str(excess)) * (D(perf_sched['rate']) / 365)
                except Exception as e:
                    logger.warning(f"Perf fee {current}: {e}")

            acc_mng    = max(D(0), prev_mng  + base_fee - mgmt_w)
            acc_perf   = max(D(0), prev_perf + perf_fee - perf_w)
            total_liab = acc_mng + acc_perf + prev_i_fees + prev_loans + prev_i_lns

            # ── Balance sheet ─────────────────────────────────
            nav      = total_assets - total_liab
            earnings = nav - capital
            net_nta  = nav / total_units if total_units > D(0) else D(0)

            # ── 4f. Write historical (one row per day) ────────
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
                current,
                r2(derivatives), r2(securities), r2(reits), r2(bonds),
                r2(money_market), r2(receivables), r2(cash),
                r2(acc_mng), r2(acc_perf), r2(prev_i_fees),
                r2(prev_loans), r2(prev_i_lns),
                r2(capital), r2(earnings), float(total_units), r6(net_nta))

            # Update fund_settings
            await db.execute("""
                UPDATE fund_settings
                SET current_nta=$1, aum=$2, last_nta_date=$3, updated_at=NOW()
                WHERE id=1
            """, r6(net_nta), r2(nav), current)

            # Carry forward
            prev_mng  = acc_mng
            prev_perf = acc_perf
            prev_nta  = net_nta if net_nta > D(0) else prev_nta
            computed += 1
            logger.info(
                f"  {current}: nta={r6(net_nta)} "
                f"sec={r2(securities)} cash={r2(cash)} "
                f"cap={r2(capital)} units={float(total_units):.4f}")

        except Exception as e:
            logger.error(f"  ✗ {current}: {e}\n{traceback.format_exc()}")
            errors += 1

        await _job(db, 'running', start, today, current,
                   computed, errors, f"{current} — {computed} done")
        current += timedelta(days=1)

    # ── Step 5: Write settlements ─────────────────────────────
    for s in settlements:
        try:
            await db.execute("""
                INSERT INTO settlement
                    (date, region, asset_class, sector, instrument, units,
                     bought_price, sale_price, cost_basis, proceeds,
                     profit_loss, return_pct, remark)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,'nta-computed')
            """, *s)
        except Exception as e:
            logger.warning(f"Settlement {s[4]}: {e}")

    # ── Step 5: Update holdings ───────────────────────────────
    try:
        await db.execute("DELETE FROM holdings")
        for instr, p in positions.items():
            if p['units'] < D('0.0001'): continue
            price = await _get_price(db, instr, today, ticker_map, price_cache)
            units = float(p['units'])
            tc    = float(p['total_cost'])
            avg_c = r8(p['total_cost'] / p['units'])
            mv    = units * float(price) if price > D(0) else tc
            upl   = mv - tc
            rp    = upl / tc if tc > 0 else 0.0
            await db.execute("""
                INSERT INTO holdings
                    (instrument, asset_class, units, avg_cost, total_cost,
                     last_price, market_value, unrealized_pl, return_pct,
                     last_trade_date, last_updated)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,NOW())
                ON CONFLICT (instrument) DO UPDATE SET
                    asset_class=EXCLUDED.asset_class,
                    units=EXCLUDED.units, avg_cost=EXCLUDED.avg_cost,
                    total_cost=EXCLUDED.total_cost,
                    last_price=EXCLUDED.last_price,
                    market_value=EXCLUDED.market_value,
                    unrealized_pl=EXCLUDED.unrealized_pl,
                    return_pct=EXCLUDED.return_pct,
                    last_trade_date=EXCLUDED.last_trade_date,
                    last_updated=NOW()
            """, instr, p['ac'], units, avg_c, tc,
                 float(price), mv, upl, rp, today)

        # Cash row
        await db.execute("""
            INSERT INTO holdings
                (instrument, asset_class, sector, region,
                 units, avg_cost, total_cost, cash, last_updated)
            VALUES ('__CASH__','Cash','','MY',1,0,$1,$1,NOW())
            ON CONFLICT (instrument) DO UPDATE SET
                total_cost=EXCLUDED.total_cost,
                cash=EXCLUDED.cash, last_updated=NOW()
        """, r2(cash))
        logger.info("Holdings updated")
    except Exception as e:
        logger.error(f"Update holdings: {e}\n{traceback.format_exc()}")

    await _job(db, 'done' if errors == 0 else 'error',
               start, today, today, computed, errors,
               f"Done — {computed} days ({start} → {today})")
    logger.info(f"NTA compute done: {computed} OK, {errors} errors")

    return {
        "computed": computed,
        "errors":   errors,
        "from":     str(start),
        "to":       str(today),
        "message":  f"Computed {computed} days ({start} → {today})",
    }


# Aliases for backward compatibility
async def compute_daily_nta(db, target_date=None):
    return await compute_portfolio_and_nta(db, target_date)

async def compute_nta_range(db, from_date=None, to_date=None):
    return await compute_portfolio_and_nta(db, from_date)
