"""
Admin router  v1.0.0
All endpoints require valid JWT with role=admin.
Full CRUD access — bypasses RLS via service_role connection.

Endpoints included in v1.0.0:
  GET/POST/PUT/DELETE  /api/admin/investors
  GET/POST/PUT/DELETE  /api/admin/users
  GET/POST/PUT/DELETE  /api/admin/transactions
  GET/POST/PUT/DELETE  /api/admin/fee-schedules
  GET/POST/PUT/DELETE  /api/admin/ticker-map
  POST                 /api/admin/prices/override
  POST                 /api/admin/prices/fetch-now
  POST                 /api/admin/nta/compute
  POST                 /api/admin/upload/excel
  GET                  /api/admin/audit-log
"""
from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from pydantic import BaseModel
from typing import Optional, List
from datetime import date
import io

from database import Database, get_db, serialise
from routers.auth import require_admin
from services.price_fetcher import run_daily_price_fetch, update_manual_price
from services.nta_engine import compute_portfolio_and_nta, compute_nta_range, compute_daily_nta

router = APIRouter()


# ── Investor Management ───────────────────────────────────────
@router.get("/investors")
async def list_investors(
    admin: dict = Depends(require_admin),
    db: Database = Depends(get_db)
):
    # Direct query — includes ALL investors (active + inactive),
    # plus account_type and holder_count from investor_holders
    rows = await db.fetch("""
        SELECT i.id, i.name, i.units, i.vwap, i.total_costs,
               i.current_nta, i.market_value, i.unrealized_pl,
               i.realized_pl, i.irr, i.joined_date, i.is_active,
               i.notes, i.account_type,
               COALESCE(COUNT(ih.id), 0) AS holder_count,
               fs.current_nta AS fund_nta,
               ROUND((i.market_value - i.total_costs) /
                     NULLIF(i.total_costs, 0) * 100, 4) AS simple_return_pct
        FROM investors i
        CROSS JOIN fund_settings fs
        LEFT JOIN investor_holders ih ON ih.investor_id = i.id
        GROUP BY i.id, fs.current_nta
        ORDER BY i.name
    """)
    return [serialise(r) for r in rows]


@router.get("/investors/{investor_id}")
async def get_investor(
    investor_id: str,
    admin: dict = Depends(require_admin),
    db: Database = Depends(get_db)
):
    row = await db.fetchrow("SELECT * FROM v_investor_profile WHERE id = $1", investor_id)
    if not row:
        raise HTTPException(status_code=404, detail="Investor not found")
    return dict(row)


# ── User / Account Management ─────────────────────────────────
@router.get("/users")
async def list_users(
    admin: dict = Depends(require_admin),
    db: Database = Depends(get_db)
):
    rows = await db.fetch("""
        SELECT u.id, u.name, u.email, u.phone, u.role, u.is_active,
               u.investor_id, u.bank_name, u.bank_account_no,
               u.address_line1, u.address_line2, u.city, u.postcode,
               u.state, u.country, u.created_at,
               ih.role  AS holder_role,
               i.name   AS investor_name
        FROM users u
        LEFT JOIN investor_holders ih ON ih.user_id = u.id
        LEFT JOIN investors i         ON i.id = ih.investor_id
        ORDER BY u.name
    """)
    return [serialise(r) for r in rows]


# UserUpdate kept minimal for backward compat — full update handled below
class UserUpdate(BaseModel):
    name: Optional[str] = None
    email: Optional[str] = None
    phone: Optional[str] = None
    role: Optional[str] = None
    is_active: Optional[bool] = None
    investor_id: Optional[str] = None


# ── Fee Schedules ─────────────────────────────────────────────
@router.get("/fee-schedules")
async def list_fee_schedules(
    admin: dict = Depends(require_admin),
    db: Database = Depends(get_db)
):
    rows = await db.fetch("SELECT * FROM v_active_fee_schedule")
    return [dict(r) for r in rows]


class FeeScheduleCreate(BaseModel):
    fee_type: str       # 'base' or 'performance'
    rate: float         # decimal: 0.01 = 1%
    basis: str = "daily"
    hurdle_rate: Optional[float] = None
    valid_from: date
    valid_to: Optional[date] = None
    description: Optional[str] = None


@router.post("/fee-schedules")
async def create_fee_schedule(
    body: FeeScheduleCreate,
    admin: dict = Depends(require_admin),
    db: Database = Depends(get_db)
):
    # Validate no overlap for same fee_type
    overlap = await db.fetchrow(
        """
        SELECT id FROM fee_schedules
        WHERE fee_type = $1 AND valid_from <= $2
          AND (valid_to IS NULL OR valid_to >= $2)
        """,
        body.fee_type, body.valid_from
    )
    if overlap:
        raise HTTPException(status_code=400,
            detail=f"Overlapping {body.fee_type} fee schedule exists for this period")

    await db.execute(
        """
        INSERT INTO fee_schedules
            (fee_type, rate, basis, hurdle_rate, valid_from, valid_to, description, created_by)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
        """,
        body.fee_type, body.rate, body.basis, body.hurdle_rate,
        body.valid_from, body.valid_to, body.description, str(admin["id"])
    )
    return {"message": "Fee schedule created"}


# ── Price Override ────────────────────────────────────────────
class PriceOverride(BaseModel):
    instrument: str
    price: float


@router.post("/prices/override")
async def price_override(
    body: PriceOverride,
    admin: dict = Depends(require_admin),
    db: Database = Depends(get_db)
):
    await update_manual_price(db, body.instrument, body.price, str(admin["id"]))
    return {"message": f"Price for {body.instrument} set to {body.price}"}


@router.get("/prices/status")
async def price_status(
    admin: dict = Depends(require_admin),
    db: Database = Depends(get_db)
):
    rows = await db.fetch("SELECT * FROM v_price_status")
    return [dict(r) for r in rows]


@router.post("/prices/fetch-now")
async def trigger_price_fetch(
    admin: dict = Depends(require_admin),
    db: Database = Depends(get_db)
):
    result = await run_daily_price_fetch(db)
    return result


# ── NTA Computation ───────────────────────────────────────────
@router.post("/nta/compute")
async def trigger_nta_compute(
    background_tasks: BackgroundTasks,
    body: dict = None,
    admin: dict = Depends(require_admin),
    db: Database = Depends(get_db),
):
    """Compute NTA from last historical date to today. Runs in background."""
    from datetime import date as date_t
    body = body or {}
    force_from = None
    if body.get("force_from"):
        try: force_from = date_t.fromisoformat(str(body["force_from"]))
        except: raise HTTPException(400, "Invalid force_from")

    last = await db.fetchrow("SELECT MAX(date) AS d FROM historical")
    from_label = str(force_from or (last["d"] if last and last["d"] else "beginning"))

    background_tasks.add_task(compute_nta_range, db, force_from, None)
    return {
        "message": "Computation started in background",
        "from":    from_label,
        "to":      str(date_t.today()),
    }




@router.get("/nta/uncomputed-status")
async def get_uncomputed_status(
    admin: dict = Depends(require_admin),
    db: Database = Depends(get_db),
):
    """Show count of uncomputed records per input table."""
    result = {}
    checks = [
        ("transactions",        "SELECT COUNT(*) FROM transactions        WHERE is_computed=FALSE"),
        ("dividends",           "SELECT COUNT(*) FROM dividends           WHERE is_computed=FALSE"),
        ("distributions",       "SELECT COUNT(*) FROM distributions       WHERE is_computed=FALSE"),
        ("others",              "SELECT COUNT(*) FROM others              WHERE is_computed=FALSE"),
        ("principal_cashflows", "SELECT COUNT(*) FROM principal_cashflows WHERE is_computed=FALSE"),
        ("fee_withdrawals",     "SELECT COUNT(*) FROM fee_withdrawals     WHERE is_computed=FALSE"),
    ]
    total = 0
    for name, q in checks:
        try:
            n = await db.fetchval(q) or 0
            result[name] = int(n)
            total += int(n)
        except Exception:
            result[name] = "N/A (run migration 14)"
    result["total_uncomputed"] = total
    result["needs_compute"]    = total > 0
    return result




@router.get("/nta/job-status")
async def get_job_status(
    admin: dict = Depends(require_admin),
    db: Database = Depends(get_db),
):
    """Live progress of the background compute job."""
    try:
        row = await db.fetchrow("SELECT * FROM compute_job WHERE id=1")
        return serialise(row) if row else {"status": "idle", "note": "Run migration 15"}
    except Exception:
        return {"status": "idle", "note": "compute_job table not yet created"}


@router.get("/nta/latest")
async def get_latest_nta(
    admin: dict = Depends(require_admin),
    db:   Database = Depends(get_db)
):
    row = await db.fetchrow("SELECT * FROM historical ORDER BY date DESC LIMIT 1")
    return serialise(row) if row else {}


# ── Fee Withdrawal ────────────────────────────────────────────
@router.get("/fee-withdrawals")
async def list_fee_withdrawals(
    admin: dict = Depends(require_admin),
    db:    Database = Depends(get_db)
):
    rows = await db.fetch("SELECT * FROM fee_withdrawals ORDER BY date DESC")
    return [serialise(r) for r in rows]


@router.post("/fee-withdrawals")
async def record_fee_withdrawal(
    body: dict,
    admin: dict = Depends(require_admin),
    db:   Database = Depends(get_db)
):
    """Record a fee withdrawal. NTA engine picks this up on next compute."""
    from datetime import date as _dt
    fee_type = body.get('fee_type')
    amount   = float(body.get('amount', 0))
    w_date   = body.get('date')
    notes    = body.get('notes', '') or ''
    if fee_type not in ('management', 'performance'):
        raise HTTPException(400, "fee_type must be 'management' or 'performance'")
    if amount <= 0:
        raise HTTPException(400, "amount must be positive")
    if not w_date:
        raise HTTPException(400, "date required")
    try:
        date_obj = _dt.fromisoformat(str(w_date))
    except ValueError:
        raise HTTPException(400, f"Invalid date: {w_date}")
    rec_id = await db.fetchval("""
        INSERT INTO fee_withdrawals (fee_type, amount, date, notes, created_by)
        VALUES ($1, $2, $3, $4, $5::uuid) RETURNING id
    """, fee_type, amount, date_obj, notes, str(admin['id']))
    return {"message": "Recorded", "id": str(rec_id), "date": w_date,
            "fee_type": fee_type, "amount": amount}


@router.delete("/fee-withdrawals/{withdrawal_id}")
async def delete_fee_withdrawal(
    withdrawal_id: str,
    admin: dict = Depends(require_admin),
    db:   Database = Depends(get_db)
):
    row = await db.fetchrow(
        "SELECT id FROM fee_withdrawals WHERE id = $1::uuid", withdrawal_id)
    if not row:
        raise HTTPException(404, "Not found")
    await db.execute(
        "DELETE FROM fee_withdrawals WHERE id = $1::uuid", withdrawal_id)
    return {"message": "Deleted. Re-run Compute to update historical."}

# ── Audit Log ─────────────────────────────────────────────────
@router.get("/audit-log")
async def audit_log(
    admin: dict = Depends(require_admin),
    db: Database = Depends(get_db),
    limit: int = 100
):
    rows = await db.fetch(
        """
        SELECT al.*, u.name AS user_name
        FROM audit_log al
        LEFT JOIN users u ON u.id = al.user_id
        ORDER BY al.created_at DESC
        LIMIT $1
        """,
        limit
    )
    return [dict(r) for r in rows]


# ── Ticker Map ────────────────────────────────────────────────
@router.get("/ticker-map")
async def get_ticker_map(
    admin: dict = Depends(require_admin),
    db: Database = Depends(get_db)
):
    rows = await db.fetch("SELECT * FROM ticker_map ORDER BY instrument")
    return [dict(r) for r in rows]


class TickerUpdate(BaseModel):
    yahoo_ticker: Optional[str]
    is_manual: Optional[bool]
    asset_class: Optional[str]
    sector: Optional[str]


@router.put("/ticker-map/{instrument}")
async def update_ticker(
    instrument: str,
    body: TickerUpdate,
    admin: dict = Depends(require_admin),
    db: Database = Depends(get_db)
):
    fields, vals, idx = [], [], 1
    for field, val in body.model_dump(exclude_none=True).items():
        fields.append(f"{field} = ${idx}")
        vals.append(val)
        idx += 1
    if not fields:
        return {"message": "Nothing to update"}
    vals.append(instrument)
    await db.execute(
        f"UPDATE ticker_map SET {', '.join(fields)}, updated_at = NOW() WHERE instrument = ${idx}",
        *vals
    )
    return {"message": f"Ticker {instrument} updated"}


# ── Transactions (admin view all) ────────────────────────────
@router.get("/transactions")
async def list_all_transactions(
    admin: dict = Depends(require_admin),
    db: Database = Depends(get_db),
    page: int = 1,
    limit: int = 50
):
    offset = (page - 1) * limit
    rows = await db.fetch(
        """SELECT t.*
           FROM transactions t
           ORDER BY t.date DESC LIMIT $1 OFFSET $2""",
        limit, offset
    )
    total = await db.fetchval("SELECT COUNT(*) FROM transactions")
    return {"data": [dict(r) for r in rows], "total": total, "page": page}


@router.post("/transactions")
async def create_transaction(
    body: dict,
    admin: dict = Depends(require_admin),
    db: Database = Depends(get_db)
):
    from datetime import date as date_type
    await db.execute(
        """INSERT INTO transactions
           (date, investor_id, region, asset_class, sector, instrument,
            units, price, amount, total_fees, net_amount, theme, notes)
           VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)""",
        date_type.fromisoformat(body['date']),
        body.get('investor_id') or None,
        body.get('region', 'MY'),
        body.get('asset_class', 'Securities [H]'),
        body.get('sector'),
        body['instrument'],
        body['units'],
        body['price'],
        body['amount'],
        body.get('total_fees', 0),
        body['net_amount'],
        body.get('theme'),
        body.get('notes'),
    )
    await db.execute(
        "INSERT INTO audit_log (user_id, action, table_name) VALUES ($1,$2,$3)",
        str(admin["id"]), "INSERT", "transactions"
    )
    return {"message": "Transaction added"}


# ── Holdings ─────────────────────────────────────────────────
@router.get("/holdings")
async def get_holdings(
    admin: dict = Depends(require_admin),
    db: Database = Depends(get_db)
):
    try:
        rows = await db.fetch("""
            SELECT instrument, asset_class, sector, region,
                   units, avg_cost, total_cost,
                   last_price, market_value, unrealized_pl,
                   return_pct, last_trade_date, cash
            FROM holdings
            ORDER BY
                CASE WHEN instrument = '__CASH__' THEN 1 ELSE 0 END ASC,
                COALESCE(market_value, total_cost, 0) DESC NULLS LAST
        """)
        return [serialise(r) for r in rows]
    except Exception as e:
        import logging
        logging.getLogger(__name__).error(f"GET holdings error: {e}")
        raise HTTPException(500, f"Holdings query failed: {e}")


@router.post("/holdings/compute")
async def compute_holdings_and_settlement(
    body: dict = None,
    admin: dict = Depends(require_admin),
    db: Database = Depends(get_db)
):
    """
    Recompute holdings and settlements using AVCO VWAP method.

    VWAP (AVCO):
      BUY  → avg_cost = (old_total_cost + abs(net_amount)) / (old_units + units)
      SELL → avg_cost unchanged for remaining units

    Settlement:
      proceeds   = abs(net_amount)          ← direct from DB, never units × price
      cost_basis = avg_cost × units_sold    ← AVCO cost
      realised_pl = proceeds - cost_basis   ← exact, rounded only at DB write
      sale_price  = proceeds / units_sold   ← for display only

    Cash balance:
      = sum(transactions.net_amount)
      + sum(principal_cashflows.amount)
      + sum(others.amount)
      + sum(dividends.amount WHERE pmt_date IS NOT NULL)
      - sum(distributions.total_dividend)
      - sum(fee_withdrawals.amount)
    """
    from decimal import Decimal, ROUND_HALF_UP, getcontext
    from datetime import date as date_t
    getcontext().prec = 28

    body  = body or {}
    as_of = None
    if body.get('as_of'):
        try:    as_of = date_t.fromisoformat(str(body['as_of']))
        except: raise HTTPException(400, "Invalid as_of date")

    def D(v):
        if v is None: return Decimal('0')
        return Decimal(str(v))

    # Round only at DB write time
    def r8(v): return float(D(v).quantize(Decimal('0.00000001'), rounding=ROUND_HALF_UP))
    def r6(v): return float(D(v).quantize(Decimal('0.000001'),   rounding=ROUND_HALF_UP))
    def r4(v): return float(D(v).quantize(Decimal('0.0001'),     rounding=ROUND_HALF_UP))
    def r2(v): return float(D(v).quantize(Decimal('0.01'),       rounding=ROUND_HALF_UP))

    # ── 1. Load transactions in ascending date order ──────────
    if as_of:
        rows = await db.fetch("""
            SELECT date, instrument, asset_class, sector, region,
                   units, price, net_amount
            FROM transactions
            WHERE date <= $1
            ORDER BY date ASC,
                     CASE WHEN units > 0 THEN 0 ELSE 1 END ASC,
                     created_at ASC
        """, as_of)
    else:
        rows = await db.fetch("""
            SELECT date, instrument, asset_class, sector, region,
                   units, price, net_amount
            FROM transactions
            ORDER BY date ASC,
                     CASE WHEN units > 0 THEN 0 ELSE 1 END ASC,
                     created_at ASC
        """)

    # positions[instr] = {units, total_cost, avg_cost, ac, sector, region, last_date}
    positions       = {}
    settlements     = []   # collect all, write after loop
    errors          = []

    # Clear previous auto-computed settlements
    await db.execute("DELETE FROM settlement WHERE remark = 'auto-computed VWAP'")

    # ── 2. Replay transactions in sequence ────────────────────
    for row in rows:
        instr  = row['instrument']
        units  = D(row['units'])       # positive = BUY, negative = SELL
        net    = D(row['net_amount'])  # negative = BUY (outflow), positive = SELL (inflow)
        ac     = row['asset_class'] or 'Securities [H]'
        sector = row['sector']  or ''
        region = row['region']  or 'MY'
        d      = row['date']

        if instr not in positions:
            positions[instr] = {
                'units':      Decimal('0'),
                'total_cost': Decimal('0'),
                'ac': ac, 'sector': sector, 'region': region,
                'last_date': d,
            }
        p = positions[instr]

        if units > Decimal('0'):
            # ── BUY ──────────────────────────────────────────
            # cost = abs(net_amount) — real cash paid including all fees
            cost            = abs(net)
            p['units']      += units
            p['total_cost'] += cost
            p['ac']          = ac
            p['sector']      = sector
            p['region']      = region
            p['last_date']   = d
            # avg_cost always derived: never stored with rounding error
            # computed fresh at read time as total_cost / units

        elif units < Decimal('0'):
            # ── SELL ─────────────────────────────────────────
            units_sold = abs(units)

            # PROCEEDS: always abs(net_amount) — direct from DB, never units × price
            proceeds = abs(net)

            # COST BASIS: (total_cost / units) × units_sold — from holdings, full precision
            # If position is 0 (e.g. warrant with zero cost), cost_basis = 0
            if p['units'] > Decimal('0'):
                # Cap units_sold at held (handles tiny rounding differences)
                units_sold  = min(units_sold, p['units'])
                cost_basis  = (p['total_cost'] / p['units']) * units_sold  # no rounding
                avg_cost    = p['total_cost'] / p['units']                  # for display
            else:
                # Position is 0 — warrant/rights with zero cost basis
                cost_basis  = Decimal('0')
                avg_cost    = Decimal('0')

            # REALISED P&L: proceeds - cost_basis — exact, no intermediate rounding
            realised_pl = proceeds - cost_basis
            sale_price  = proceeds / units_sold if units_sold > Decimal('0') else Decimal('0')
            ret_pct     = (realised_pl / cost_basis * 100) if cost_basis > Decimal('0') else Decimal('0')

            # Append settlement — round ONLY here at DB write time
            # cost_basis and proceeds stored at full precision so
            # realised_pl = proceeds - cost_basis is always exact when read from DB
            settlements.append((
                d, region, ac, sector, instr,
                r8(units_sold),
                r8(avg_cost),       # bought_price = total_cost/units (AVCO)
                r8(sale_price),     # sale_price   = proceeds/units_sold
                r6(cost_basis),     # cost_basis   = (total_cost/units) × units_sold
                r6(proceeds),       # proceeds     = abs(net_amount)
                r4(realised_pl),    # realised_pl  = proceeds - cost_basis (exact)
                r4(ret_pct),
            ))

            # Reduce position — total_cost reduced proportionally
            if p['units'] > Decimal('0'):
                proportion      = units_sold / p['units']
                p['total_cost'] -= p['total_cost'] * proportion
                p['units']      -= units_sold
                p['last_date']   = d

                # Clear dust — residual < 0.0001 is rounding artifact
                if p['units'] < Decimal('0.0001'):
                    p['units']      = Decimal('0')
                    p['total_cost'] = Decimal('0')

    # ── 3. Write settlements ──────────────────────────────────
    sc = 0
    for s in settlements:
        try:
            await db.execute("""
                INSERT INTO settlement
                    (date, region, asset_class, sector, instrument, units,
                     bought_price, sale_price, cost_basis, proceeds,
                     profit_loss, return_pct, remark)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,'auto-computed VWAP')
            """, *s)
            sc += 1
        except Exception as e:
            errors.append(f"settlement {s[4]}: {e}")

    # ── 4. Compute cash balance ───────────────────────────────
    cash = Decimal('0')

    # Transactions: net_amount is negative for BUY, positive for SELL
    for row in rows:
        cash += D(row['net_amount'])

    # Principal cashflows (subscriptions +, redemptions -)
    try:
        q = "SELECT COALESCE(SUM(amount),0) n FROM principal_cashflows"
        r = await db.fetchrow(q + (" WHERE date<=$1" if as_of else ""), *([as_of] if as_of else []))
        cash += D(r['n'])
    except Exception as e: errors.append(f"principal_cashflows: {e}")

    # Others income/expense
    try:
        q = "SELECT COALESCE(SUM(amount),0) n FROM others"
        r = await db.fetchrow(q + (" WHERE record_date<=$1" if as_of else ""), *([as_of] if as_of else []))
        cash += D(r['n'])
    except Exception as e: errors.append(f"others: {e}")

    # Dividends received
    try:
        q = "SELECT COALESCE(SUM(amount),0) n FROM dividends WHERE pmt_date IS NOT NULL"
        r = await db.fetchrow(q + (" AND pmt_date<=$1" if as_of else ""), *([as_of] if as_of else []))
        cash += D(r['n'])
    except Exception as e: errors.append(f"dividends: {e}")

    # Distributions paid out
    try:
        q = "SELECT COALESCE(SUM(total_dividend),0) n FROM distributions"
        r = await db.fetchrow(q + (" WHERE pmt_date<=$1" if as_of else ""), *([as_of] if as_of else []))
        cash -= D(r['n'])
    except Exception as e: errors.append(f"distributions: {e}")

    # Fee withdrawals
    try:
        q = "SELECT COALESCE(SUM(amount),0) n FROM fee_withdrawals"
        r = await db.fetchrow(q + (" WHERE date<=$1" if as_of else ""), *([as_of] if as_of else []))
        cash -= D(r['n'])
    except Exception as e: errors.append(f"fee_withdrawals: {e}")

    # ── 5. Save holdings (units > 0.0001 only) ───────────────
    await db.execute("DELETE FROM holdings")
    saved = 0

    for instr, p in positions.items():
        if p['units'] <= Decimal('0.0001'):
            continue
        try:
            await db.execute("""
                INSERT INTO holdings
                    (instrument, asset_class, sector, region,
                     units, avg_cost, total_cost, last_trade_date, last_updated)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,NOW())
                ON CONFLICT (instrument) DO UPDATE SET
                    asset_class=EXCLUDED.asset_class,
                    sector=EXCLUDED.sector,
                    region=EXCLUDED.region,
                    units=EXCLUDED.units,
                    avg_cost=EXCLUDED.avg_cost,
                    total_cost=EXCLUDED.total_cost,
                    last_trade_date=EXCLUDED.last_trade_date,
                    last_updated=NOW()
            """,
                instr, p['ac'], p['sector'], p['region'],
                r8(p['units']),
                r8(p['total_cost'] / p['units']),   # avg_cost = total_cost/units, no rounding error
                r4(p['total_cost']),
                p['last_date'])
            saved += 1
        except Exception as e:
            errors.append(f"holdings {instr}: {e}")

    # ── 6. Cash row — always last ─────────────────────────────
    try:
        await db.execute("""
            INSERT INTO holdings
                (instrument, asset_class, sector, region,
                 units, avg_cost, total_cost, cash, last_updated)
            VALUES ('__CASH__','Cash','','MY',1,0,$1,$1,NOW())
            ON CONFLICT (instrument) DO UPDATE SET
                total_cost=EXCLUDED.total_cost,
                cash=EXCLUDED.cash,
                last_updated=NOW()
        """, r2(cash))
    except Exception as e:
        errors.append(f"cash row: {e}")

    return {
        "message":            "Holdings computed" + (f" as of {as_of}" if as_of else " (latest)"),
        "as_of":              str(as_of) if as_of else "latest",
        "positions":          saved,
        "cash_balance":       r2(cash),
        "settlement_records": sc,
        "errors":             errors[:20],
    }


# ── Financial Statements ──────────────────────────────────────
# ── Financial Statements ─────────────────────────────────────
async def _compute_financial_statements(db) -> list:
    """
    Core computation for all financial statements.
    Called by the GET endpoint (cache miss) and POST recompute.
    Returns list of FY dicts with IS, BS, CF, Ratio data.
    """
    from datetime import date as date_t, timedelta as td
    import json

    today = date_t.today()

    def fy_bounds(fy_year: int):
        return date_t(fy_year - 1, 12, 1), date_t(fy_year, 11, 30)

    start_fy = 2022
    end_fy   = today.year if today.month == 12 else today.year

    results       = []
    cumulative_np = 0.0

    for fy in range(start_fy, end_fy + 1):
        fyb, fye = fy_bounds(fy)
        if fyb > today: continue
        fye_cap  = min(fye, today)
        fy_label = f"FY{str(fy)[2:]}"
        fyb1     = fyb - td(days=1)

        # ── Revenue ──────────────────────────────────────────
        r = await db.fetchrow(
            "SELECT COALESCE(SUM(amount),0) n FROM dividends "
            "WHERE pmt_date>=$1 AND pmt_date<=$2", fyb, fye_cap)
        dividend_income = float(r['n'])

        r = await db.fetchrow(
            "SELECT COALESCE(SUM(amount),0) n FROM others "
            "WHERE record_date>=$1 AND record_date<=$2 "
            "AND LOWER(income_type) LIKE '%interest%'", fyb, fye_cap)
        r2 = await db.fetchrow(
            "SELECT COALESCE(SUM(profit_loss),0) n FROM settlement "
            "WHERE date>=$1 AND date<=$2 "
            "AND LOWER(asset_class) LIKE '%money market%'", fyb, fye_cap)
        interest_income = float(r['n']) + float(r2['n'])
        revenue = dividend_income + interest_income

        # ── Costs ─────────────────────────────────────────────
        h_open = await db.fetchrow(
            "SELECT mng_fees, perf_fees FROM historical "
            "WHERE date<=$1 ORDER BY date DESC LIMIT 1", fyb1)
        h_close = await db.fetchrow(
            "SELECT * FROM historical WHERE date<=$1 ORDER BY date DESC LIMIT 1",
            fye_cap)
        mng_open  = float(h_open['mng_fees'])  if h_open  else 0.0
        mng_close = float(h_close['mng_fees']) if h_close else 0.0
        prf_open  = float(h_open['perf_fees'])  if h_open  else 0.0
        prf_close = float(h_close['perf_fees']) if h_close else 0.0

        fw_m = await db.fetchrow(
            "SELECT COALESCE(SUM(amount),0) n FROM fee_withdrawals "
            "WHERE date>=$1 AND date<=$2 AND fee_type='management'", fyb, fye_cap)
        fw_p = await db.fetchrow(
            "SELECT COALESCE(SUM(amount),0) n FROM fee_withdrawals "
            "WHERE date>=$1 AND date<=$2 AND fee_type='performance'", fyb, fye_cap)
        fw_mng  = float(fw_m['n'])
        fw_perf = float(fw_p['n'])
        mng_cost    = max(0.0, (mng_close - mng_open) + fw_mng)
        perf_cost   = max(0.0, (prf_close - prf_open) + fw_perf)
        total_costs = mng_cost + perf_cost
        gross_profit = revenue - total_costs

        # ── Other Income ──────────────────────────────────────
        r = await db.fetchrow(
            "SELECT COALESCE(SUM(profit_loss),0) n FROM settlement "
            "WHERE date>=$1 AND date<=$2 "
            "AND LOWER(asset_class) NOT LIKE '%money market%'", fyb, fye_cap)
        realised = float(r['n'])

        r = await db.fetchrow(
            "SELECT COALESCE(SUM(amount),0) n FROM others "
            "WHERE record_date>=$1 AND record_date<=$2 "
            "AND LOWER(income_type) NOT LIKE '%interest%'", fyb, fye_cap)
        other_misc = float(r['n'])

        # ── Net Profit (adjusted) ─────────────────────────────
        r = await db.fetchrow(
            "SELECT COALESCE(SUM(total_dividend),0) n FROM distributions "
            "WHERE pmt_date<=$1", fye_cap)
        cum_dist   = float(r['n'])
        re_fye     = float(h_close['earnings']) if h_close else 0.0
        net_profit = re_fye + cum_dist - cumulative_np
        unrealised = net_profit - gross_profit - realised - other_misc
        other_total = realised + unrealised + other_misc
        cumulative_np += net_profit

        # ── Balance Sheet ─────────────────────────────────────
        bs    = h_close
        sec   = float(bs['securities']   or 0) if bs else 0.0
        deriv = float(bs['derivatives']  or 0) if bs else 0.0
        reit  = float(bs['reits']        or 0) if bs else 0.0
        bond  = float(bs['bonds']        or 0) if bs else 0.0
        mm    = float(bs['money_market'] or 0) if bs else 0.0
        recv  = float(bs['receivables']  or 0) if bs else 0.0
        cash_b= float(bs['cash']         or 0) if bs else 0.0
        mng_l = float(bs['mng_fees']     or 0) if bs else 0.0
        prf_l = float(bs['perf_fees']    or 0) if bs else 0.0
        iof_l = float(bs['ints_on_fees'] or 0) if bs else 0.0
        lns_l = float(bs['loans']        or 0) if bs else 0.0
        iln_l = float(bs['ints_on_loans']or 0) if bs else 0.0
        cap   = float(bs['capital']      or 0) if bs else 0.0
        earn  = float(bs['earnings']     or 0) if bs else 0.0
        total_units = float(bs['total_units'] or 0) if bs else 0.0
        nta   = float(bs['nta']          or 0) if bs else 0.0
        total_assets = sec + deriv + reit + bond + mm + recv + cash_b
        total_liab   = mng_l + prf_l + iof_l + lns_l + iln_l
        nav   = total_assets - total_liab

        # ── Cash Flow ─────────────────────────────────────────
        r = await db.fetchrow(
            "SELECT COALESCE(SUM(ABS(net_amount)),0) n FROM transactions "
            "WHERE date>=$1 AND date<=$2 AND units>0", fyb, fye_cap)
        buys_total = float(r['n'])
        r = await db.fetchrow(
            "SELECT COALESCE(SUM(net_amount),0) n FROM transactions "
            "WHERE date>=$1 AND date<=$2 AND units<0", fyb, fye_cap)
        sells_total = float(r['n'])
        r = await db.fetchrow(
            "SELECT COALESCE(SUM(CASE WHEN amount>0 THEN amount ELSE 0 END),0) n "
            "FROM principal_cashflows WHERE date>=$1 AND date<=$2", fyb, fye_cap)
        subscriptions = float(r['n'])
        r = await db.fetchrow(
            "SELECT COALESCE(SUM(ABS(CASE WHEN amount<0 THEN amount ELSE 0 END)),0) n "
            "FROM principal_cashflows WHERE date>=$1 AND date<=$2", fyb, fye_cap)
        redemptions = float(r['n'])
        r = await db.fetchrow(
            "SELECT COALESCE(SUM(total_dividend),0) n FROM distributions "
            "WHERE pmt_date>=$1 AND pmt_date<=$2", fyb, fye_cap)
        distributions_paid = float(r['n'])
        h_open_cash = await db.fetchrow(
            "SELECT cash FROM historical WHERE date<=$1 ORDER BY date DESC LIMIT 1", fyb1)
        cash_open       = float(h_open_cash['cash']) if h_open_cash else 0.0
        net_cash_change = cash_b - cash_open

        # ── Per-share ─────────────────────────────────────────
        gps = (gross_profit / total_units * 100) if total_units > 0 else 0.0
        eps = (net_profit   / total_units * 100) if total_units > 0 else 0.0

        # ── Ratios ────────────────────────────────────────────
        h_open_nta = await db.fetchrow(
            "SELECT nta FROM historical WHERE date<=$1 ORDER BY date DESC LIMIT 1", fyb1)
        nta_open      = float(h_open_nta['nta']) if h_open_nta else 1.0
        total_return  = ((nta / nta_open) - 1) * 100 if nta_open > 0 and nta > 0 else 0.0
        gross_margin  = (gross_profit / revenue * 100) if revenue != 0 else 0.0
        income_yield  = (revenue      / total_assets * 100) if total_assets > 0 else 0.0
        gross_yield   = (gross_profit / total_assets * 100) if total_assets > 0 else 0.0

        r = await db.fetchrow(
            "SELECT COALESCE(SUM(dps_sen),0) n FROM distributions "
            "WHERE pmt_date>=$1 AND pmt_date<=$2", fyb, fye_cap)
        total_dps = float(r['n'])
        div_yield = (total_dps / 100 / nta * 100) if nta > 0 else 0.0

        row = {
            "fy": fy_label, "fy_year": fy,
            "fyb": str(fyb), "fye": str(fye_cap),
            "is_current": fye > today,
            # IS
            "dividend_income": round(dividend_income, 2),
            "interest_income": round(interest_income, 2),
            "revenue":         round(revenue, 2),
            "mng_cost":        round(mng_cost, 2),
            "perf_cost":       round(perf_cost, 2),
            "total_costs":     round(total_costs, 2),
            "gross_profit":    round(gross_profit, 2),
            "realised":        round(realised, 2),
            "unrealised":      round(unrealised, 2),
            "other_misc":      round(other_misc, 2),
            "other_total":     round(other_total, 2),
            "net_profit":      round(net_profit, 2),
            "outstanding_shares": round(total_units, 4),
            "gps":             round(gps, 4),
            "eps":             round(eps, 4),
            # BS
            "securities":      round(sec,   2),
            "derivatives":     round(deriv, 2),
            "reits":           round(reit,  2),
            "bonds":           round(bond,  2),
            "money_market":    round(mm,    2),
            "receivables":     round(recv,  2),
            "cash_bal":        round(cash_b,2),
            "total_assets":    round(total_assets, 2),
            "mng_fees_liab":   round(mng_l,  2),
            "perf_fees_liab":  round(prf_l,  2),
            "other_liab":      round(iof_l + lns_l + iln_l, 2),
            "total_liab":      round(total_liab, 2),
            "capital":         round(cap,   2),
            "earnings":        round(earn,  2),
            "nav":             round(nav,   2),
            "nta":             round(nta,   6),
            # CF
            "buys_total":         round(buys_total, 2),
            "sells_total":        round(sells_total, 2),
            "subscriptions":      round(subscriptions, 2),
            "redemptions":        round(redemptions, 2),
            "distributions_paid": round(distributions_paid, 2),
            "fw_mng":             round(fw_mng,  2),
            "fw_perf":            round(fw_perf, 2),
            "net_cash_change":    round(net_cash_change, 2),
            # Ratios
            "total_return_pct":   round(total_return,  4),
            "income_yield_pct":   round(income_yield,  4),
            "gross_yield_pct":    round(gross_yield,   4),
            "div_yield_pct":      round(div_yield,     4),
            "gross_margin_pct":   round(gross_margin,  4),
            "total_dps":          round(total_dps,     4),
        }
        results.append(row)

    # YoY growth
    for i, d in enumerate(results):
        if i == 0:
            d['revenue_yoy'] = None
            d['gross_yoy']   = None
        else:
            prev = results[i-1]
            d['revenue_yoy'] = round(
                ((d['revenue']      - prev['revenue'])      / prev['revenue']      * 100)
                if prev['revenue'] != 0 else 0.0, 4)
            d['gross_yoy']   = round(
                ((d['gross_profit'] - prev['gross_profit']) / prev['gross_profit'] * 100)
                if prev['gross_profit'] != 0 else 0.0, 4)

    return results


async def _store_statements(db, results: list):
    """Upsert computed results into financial_statements table."""
    import json
    for row in results:
        await db.execute("""
            INSERT INTO financial_statements (fy, fy_year, is_current, data, computed_at)
            VALUES ($1, $2, $3, $4::jsonb, NOW())
            ON CONFLICT (fy) DO UPDATE SET
                data        = EXCLUDED.data,
                is_current  = EXCLUDED.is_current,
                computed_at = NOW()
        """, row['fy'], row['fy_year'], row['is_current'], json.dumps(row))


@router.get("/financials/income-statement")
async def get_income_statement(
    admin: dict = Depends(require_admin),
    db: Database = Depends(get_db)
):
    """
    Returns pre-computed financial statements from DB.
    Falls back to live computation + stores if DB is empty.
    Current FY (is_current=True) always recomputed fresh.
    """
    import json
    from datetime import date as date_t

    today = date_t.today()

    # Load cached rows — skip current FY (always recompute)
    cached = await db.fetch("""
        SELECT fy, fy_year, is_current, data, computed_at
        FROM financial_statements
        WHERE is_current = FALSE
        ORDER BY fy_year ASC
    """)

    # Load current FY fresh
    current_fy_year = today.year if today.month == 12 else today.year
    current_fy_label = f"FY{str(current_fy_year)[2:]}"

    if cached:
        # Deserialise cached past FYs
        results = []
        for row in cached:
            d = row['data'] if isinstance(row['data'], dict) else json.loads(row['data'])
            results.append(d)

        # Compute only the current FY
        all_computed = await _compute_financial_statements(db)
        current_rows = [r for r in all_computed if r['fy'] == current_fy_label]
        results.extend(current_rows)

        # Upsert current FY into DB
        if current_rows:
            await _store_statements(db, current_rows)

        return results
    else:
        # No cache at all — compute everything and store
        results = await _compute_financial_statements(db)
        await _store_statements(db, results)
        return results


@router.post("/financials/recompute")
async def recompute_statements(
    body: dict = {},
    admin: dict = Depends(require_admin),
    db: Database = Depends(get_db)
):
    """
    Admin triggers full recomputation of all financial statements.
    Pass {"fy": "FY25"} to recompute a single FY, or {} for all.
    Stores results in financial_statements table.
    """
    results = await _compute_financial_statements(db)

    target_fy = body.get('fy') if body else None
    if target_fy:
        to_store = [r for r in results if r['fy'] == target_fy]
    else:
        to_store = results

    await _store_statements(db, to_store)

    return {
        "message":   f"Recomputed {len(to_store)} FY records",
        "fys":       [r['fy'] for r in to_store],
        "computed_at": str(__import__('datetime').datetime.now()),
    }


@router.get("/financials/cache-status")
async def get_cache_status(
    admin: dict = Depends(require_admin),
    db: Database = Depends(get_db)
):
    """Returns metadata about what's cached and when it was last computed."""
    rows = await db.fetch("""
        SELECT fy, fy_year, is_current, computed_at
        FROM financial_statements
        ORDER BY fy_year ASC
    """)
    return [serialise(r) for r in rows]

# ── Principal cashflow ────────────────────────────────────────
@router.get("/principal")
async def get_principal(
    admin: dict = Depends(require_admin),
    db: Database = Depends(get_db)
):
    rows = await db.fetch("""
        SELECT p.*,
               COALESCE(p.flow_type, p.cashflow_type) as flow_type,
               i.name as investor_name
        FROM principal_cashflows p
        LEFT JOIN investors i ON i.id = p.investor_id
        ORDER BY p.date DESC
    """)
    return [dict(r) for r in rows]


@router.post("/principal")
async def add_principal(
    body: dict,
    admin: dict = Depends(require_admin),
    db: Database = Depends(get_db)
):
    from datetime import date as date_type
    await db.execute("""
        INSERT INTO principal_cashflows
        (date, investor_id, flow_type, amount, nta_at_date, units, notes)
        VALUES ($1,$2,$3,$4,$5,$6,$7)
    """,
        date_type.fromisoformat(body['date']),
        body.get('investor_id') or None,
        body['flow_type'],
        body['amount'],
        body['nta_at_date'],
        body['units'],
        body.get('notes'),
    )
    sign = 1 if body['flow_type'] == 'deposit' else -1
    await db.execute(
        "UPDATE investors SET units = units + $1 WHERE id = $2",
        sign * body['units'], body['investor_id']
    )
    return {"message": "Principal cashflow recorded"}


# ── NTA at date ───────────────────────────────────────────────
@router.get("/nta/at-date")
async def get_nta_at_date(
    date: str,
    admin: dict = Depends(require_admin),
    db: Database = Depends(get_db)
):
    from datetime import date as date_type
    d = date_type.fromisoformat(date)
    row = await db.fetchrow("""
        SELECT nta, date FROM historical
        WHERE date <= $1 AND nta > 0.5
        ORDER BY date DESC LIMIT 1
    """, d)
    if row:
        return {"nta": float(row['nta']), "date": str(row['date'])}
    # Fallback to fund settings
    setting = await db.fetchrow("SELECT current_nta FROM fund_settings LIMIT 1")
    if setting:
        return {"nta": float(setting['current_nta']), "date": date}
    return {"nta": 1.0, "date": date}


# ── Dividends admin ───────────────────────────────────────────
@router.get("/dividends")
async def get_dividends(
    admin: dict = Depends(require_admin),
    db: Database = Depends(get_db)
):
    rows = await db.fetch("SELECT * FROM dividends ORDER BY ex_date DESC")
    return [dict(r) for r in rows]


@router.post("/dividends")
async def add_dividend(
    body: dict,
    admin: dict = Depends(require_admin),
    db: Database = Depends(get_db)
):
    from datetime import date as date_type
    await db.execute("""
        INSERT INTO dividends
        (ann_date, ex_date, pmt_date, asset_class, instrument,
         units, dps_sen, amount, entitlement)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
    """,
        date_type.fromisoformat(body['ann_date']),
        date_type.fromisoformat(body['ex_date']),
        date_type.fromisoformat(body['pmt_date']) if body.get('pmt_date') else None,
        body.get('asset_class'),
        body['instrument'],
        body['units'],
        body['dps_sen'],
        body['amount'],
        body.get('entitlement', 'cash'),
    )
    return {"message": "Dividend recorded"}


# ── Others admin ──────────────────────────────────────────────
@router.get("/others")
async def get_others(
    admin: dict = Depends(require_admin),
    db: Database = Depends(get_db)
):
    rows = await db.fetch("SELECT * FROM others ORDER BY record_date DESC")
    return [dict(r) for r in rows]


@router.post("/others")
async def add_other(
    body: dict,
    admin: dict = Depends(require_admin),
    db: Database = Depends(get_db)
):
    from datetime import date as date_type
    await db.execute("""
        INSERT INTO others
        (record_date, title, income_type, amount, platform, description)
        VALUES ($1,$2,$3,$4,$5,$6)
    """,
        date_type.fromisoformat(body['record_date']),
        body['title'],
        body.get('income_type', 'Others'),
        body['amount'],
        body.get('platform'),
        body.get('description'),
    )
    return {"message": "Record added"}


# ── Distributions admin ───────────────────────────────────────
@router.get("/distributions")
async def get_distributions(
    admin: dict = Depends(require_admin),
    db: Database = Depends(get_db)
):
    rows = await db.fetch("SELECT * FROM distributions ORDER BY ex_date DESC")
    return [dict(r) for r in rows]


@router.post("/distributions")
async def declare_distribution(
    body: dict,
    admin: dict = Depends(require_admin),
    db: Database = Depends(get_db)
):
    from datetime import date as date_type
    ex_date = date_type.fromisoformat(body['ex_date'])
    dps     = float(body['dps_sen'])

    # Get all investors and their units
    investors = await db.fetch("SELECT id, name, units FROM investors WHERE units > 0")
    total_units = sum(float(r['units']) for r in investors)
    total_div   = total_units * dps / 100

    # Insert distribution header
    dist_id = await db.fetchval("""
        INSERT INTO distributions
        (ann_date, ex_date, pmt_date, financial_year, title, dist_type,
         dps_sen, total_units, total_dividend, payout_ratio)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
        RETURNING id
    """,
        date_type.fromisoformat(body['ann_date']),
        ex_date,
        date_type.fromisoformat(body['pmt_date']) if body.get('pmt_date') else None,
        body.get('financial_year', 'FY?'),
        body['title'],
        body['dist_type'],
        dps,
        total_units,
        total_div,
        body.get('payout_ratio'),
    )

    # Insert per-investor entitlements
    for inv in investors:
        amount = float(inv['units']) * dps / 100
        await db.execute("""
            INSERT INTO distribution_investors
            (distribution_id, investor_id, units_at_ex_date, dps_sen, amount, paid)
            VALUES ($1,$2,$3,$4,$5,FALSE)
            ON CONFLICT DO NOTHING
        """, dist_id, str(inv['id']), float(inv['units']), dps, amount)

    return {
        "message": "Distribution declared",
        "investors_count": len(investors),
        "total_dividend": total_div
    }


@router.get("/distributions/{dist_id}/breakdown")
async def get_distribution_breakdown(
    dist_id: str,
    admin: dict = Depends(require_admin),
    db: Database = Depends(get_db)
):
    from fastapi import HTTPException
    try:
        # First check if pre-computed breakdown exists
        rows = await db.fetch("""
            SELECT di.*, i.name as investor_name
            FROM distribution_investors di
            LEFT JOIN investors i ON i.id = di.investor_id
            WHERE di.distribution_id = $1
            ORDER BY di.amount DESC
        """, dist_id)

        if rows:
            return [dict(r) for r in rows]

        # No pre-computed data — compute on-the-fly from principal_cashflows
        dist = await db.fetchrow("SELECT * FROM distributions WHERE id = $1", dist_id)
        if not dist:
            return []

        ex_date = dist['ex_date']
        dps     = float(dist['dps_sen'])

        # Get each investor's cumulative units at ex-date from principal_cashflows
        # NOTE: units column is already signed — negative for withdrawals, positive for deposits
        investor_units = await db.fetch("""
            SELECT
                p.investor_id,
                i.name as investor_name,
                SUM(p.units) as units_at_ex_date
            FROM principal_cashflows p
            LEFT JOIN investors i ON i.id = p.investor_id
            WHERE p.date <= $1
              AND p.investor_id IS NOT NULL
            GROUP BY p.investor_id, i.name
            HAVING SUM(p.units) > 0
            ORDER BY SUM(p.units) DESC
        """, ex_date)

        if not investor_units:
            raise HTTPException(status_code=404, detail="No investor units found at ex-date. Check principal_cashflows has investor_id linked.")

        result = []
        total_units = sum(float(r['units_at_ex_date']) for r in investor_units)

        for inv in investor_units:
            units  = float(inv['units_at_ex_date'])
            amount = units * dps / 100
            result.append({
                'investor_id':       str(inv['investor_id']),
                'investor_name':     inv['investor_name'] or '—',
                'units_at_ex_date':  units,
                'dps_sen':           dps,
                'amount':            round(amount, 4),
                'paid':              False,
            })
            try:
                await db.execute("""
                    INSERT INTO distribution_investors
                    (distribution_id, investor_id, units_at_ex_date, dps_sen, amount, paid)
                    VALUES ($1,$2,$3,$4,$5,FALSE)
                    ON CONFLICT (distribution_id, investor_id) DO UPDATE SET
                        units_at_ex_date = EXCLUDED.units_at_ex_date,
                        amount = EXCLUDED.amount
                """, dist_id, str(inv['investor_id']), units, dps, round(amount, 4))
            except Exception as e:
                pass  # Don't fail if save fails

        # Update distribution total only if it was 0 (never set)
        try:
            existing = await db.fetchrow(
                "SELECT total_units, total_dividend FROM distributions WHERE id = $1", dist_id
            )
            if not existing['total_units'] or float(existing['total_units']) == 0:
                total_div = sum(r['amount'] for r in result)
                await db.execute("""
                    UPDATE distributions
                    SET total_units = $1, total_dividend = $2
                    WHERE id = $3
                """, total_units, round(total_div, 4), dist_id)
        except Exception:
            pass

        return result

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Breakdown error: {str(e)}")


@router.post("/distributions/{dist_id}/compute-breakdown")
async def compute_distribution_breakdown(
    dist_id: str,
    admin: dict = Depends(require_admin),
    db: Database = Depends(get_db)
):
    """Force recompute breakdown from principal_cashflows."""
    await db.execute("DELETE FROM distribution_investors WHERE distribution_id = $1", dist_id)
    return await get_distribution_breakdown(dist_id, admin, db)


# ── Settlement admin ──────────────────────────────────────────
@router.get("/settlement")
async def get_settlement(
    admin: dict = Depends(require_admin),
    db: Database = Depends(get_db)
):
    rows = await db.fetch("SELECT * FROM settlement ORDER BY date DESC")
    return [dict(r) for r in rows]


# ── Users admin ───────────────────────────────────────────────
@router.post("/users")
async def create_user(
    body: dict,
    admin: dict = Depends(require_admin),
    db: Database = Depends(get_db)
):
    import bcrypt
    pw_hash = bcrypt.hashpw(body['new_password'].encode(), bcrypt.gensalt()).decode()
    await db.execute("""
        INSERT INTO users (name, email, phone, role, password_hash, is_active, investor_id)
        VALUES ($1,$2,$3,$4,$5,$6,$7)
    """,
        body['name'], body['email'], body.get('phone'),
        body.get('role','member'), pw_hash,
        body.get('is_active', True),
        body.get('investor_id'),
    )
    return {"message": "User created"}


@router.put("/users/{user_id}")
async def update_user(
    user_id: str,
    body: dict,
    admin: dict = Depends(require_admin),
    db: Database = Depends(get_db)
):
    import bcrypt as _bcrypt
    # All updatable fields
    allowed = {
        "name", "email", "phone", "role", "is_active", "investor_id",
        "bank_name", "bank_account_no",
        "address_line1", "address_line2", "city", "postcode", "state", "country",
    }
    fields, vals, idx = [], [], 1
    for k, v in body.items():
        if k in allowed:
            fields.append(f"{k}=${idx}")
            vals.append(v)
            idx += 1
    if fields:
        vals.append(user_id)
        await db.execute(
            f"UPDATE users SET {','.join(fields)}, updated_at=NOW() WHERE id=${idx}",
            *vals
        )
    if body.get("new_password"):
        pw_hash = _bcrypt.hashpw(
            body["new_password"].encode(), _bcrypt.gensalt()).decode()
        await db.execute(
            "UPDATE users SET password_hash=$1 WHERE id=$2", pw_hash, user_id)
    await db.execute(
        "INSERT INTO audit_log (user_id,action,table_name,record_id) VALUES ($1,$2,$3,$4)",
        str(admin["id"]), "UPDATE", "users", user_id)
    return {"message": "User updated"}

@router.post("/statements/generate")
async def generate_statement(
    body: dict,
    admin: dict = Depends(require_admin),
    db: Database = Depends(get_db)
):
    """Generate PDF statement and store in documents table."""
    import io, base64, traceback as _tb
    from services.pdf_statements import (
        generate_factsheet, generate_subscription, generate_redemption,
        generate_dividend_statement, generate_account_statement
    )

    stmt_type   = body.get('statement_type')
    _inv_id_raw = body.get('investor_id')
    fin_year    = body.get('financial_year', '')
    period      = body.get('period', '')

    import uuid as _uuid_mod
    from datetime import date as _date_type

    async def _enrich_inv(inv_row, investor_uuid):
        """Add holders + their nominees to investor dict for PDF display."""
        d = dict(inv_row)
        # Holders (primary first)
        holders = await db.fetch("""
            SELECT u.name, u.email, u.phone, ih.role, ih.share_ratio
            FROM investor_holders ih JOIN users u ON u.id=ih.user_id
            WHERE ih.investor_id=$1
            ORDER BY CASE ih.role WHEN 'primary' THEN 0 ELSE 1 END
        """, investor_uuid)
        d['holders'] = [{"name":  h["name"],  "email": h["email"],
                         "phone": h["phone"],  "role":  h["role"],
                         "share_ratio": float(h["share_ratio"] or 100)}
                        for h in holders]
        acc = await db.fetchval(
            "SELECT account_type FROM investors WHERE id=$1", investor_uuid)
        d['account_type'] = (acc or 'individual').capitalize()
        return d
    try:
        investor_id = _uuid_mod.UUID(str(_inv_id_raw)) if _inv_id_raw else None
    except (ValueError, AttributeError):
        raise HTTPException(400, f"Invalid investor_id format: {_inv_id_raw}")

    # Convert selected_date string to datetime.date (asyncpg requires date object)
    _sel_date_raw = body.get('selected_date')
    selected_date_obj = None
    if _sel_date_raw:
        try:
            selected_date_obj = _date_type.fromisoformat(str(_sel_date_raw))
        except ValueError:
            raise HTTPException(400, f"Invalid selected_date format: {_sel_date_raw}")

    try:
        pdf_bytes = None
        title     = ''
        visibility = 'fund'
        inv_id_for_doc = None

        # ── Fetch common fund data ────────────────────────────
        overview = await db.fetchrow("SELECT * FROM v_fund_overview")
        fund_data = dict(overview) if overview else {}

        if stmt_type == 'factsheet':
            from datetime import date as date_type, timedelta as _td

            # (a) Selective date — all data filtered to this date
            as_of = selected_date_obj or date_type.today()

            # (b) Fund Details from historical at as_of
            hist_row = await db.fetchrow("""
                SELECT nta, total_units, cash,
                       securities, derivatives, reits, bonds, money_market,
                       receivables, mng_fees, perf_fees
                FROM historical WHERE date <= $1 ORDER BY date DESC LIMIT 1
            """, as_of)
            if hist_row:
                fund_data = dict(fund_data)
                nta_val   = float(hist_row['nta']          or 0)
                units_val = float(hist_row['total_units']  or 0)
                sec_val   = float(hist_row['securities']   or 0)
                der_val   = float(hist_row['derivatives']  or 0)
                rei_val   = float(hist_row['reits']        or 0)
                bon_val   = float(hist_row['bonds']        or 0)
                mm_val    = float(hist_row['money_market'] or 0)
                rec_val   = float(hist_row['receivables']  or 0)
                csh_val   = float(hist_row['cash']         or 0)
                total_assets = sec_val+der_val+rei_val+bon_val+mm_val+rec_val+csh_val
                total_liab   = float(hist_row['mng_fees']  or 0) + float(hist_row['perf_fees'] or 0)
                aum_val   = total_assets - total_liab
                fund_data['current_nta']  = nta_val
                fund_data['aum']          = aum_val
                fund_data['total_units']  = units_val
            else:
                nta_val = units_val = aum_val = 0.0
                total_assets = 0.0

            # (c) Sector breakdown from historical at as_of
            # Compute weights from the historical asset buckets at as_of
            sector_data = []
            if hist_row and total_assets > 0:
                buckets = [
                    ('Securities',    float(hist_row['securities']   or 0)),
                    ('Derivatives',   float(hist_row['derivatives']  or 0)),
                    ('REITs',         float(hist_row['reits']        or 0)),
                    ('Bonds',         float(hist_row['bonds']        or 0)),
                    ('Money Market',  float(hist_row['money_market'] or 0)),
                    ('Receivables',   float(hist_row['receivables']  or 0)),
                    ('Cash',          float(hist_row['cash']         or 0)),
                ]
                for name, val in buckets:
                    if val > 0:
                        sector_data.append({
                            'asset_class':         name,
                            'total_market_value':  val,
                            'weight_pct':          round(val / total_assets * 100, 2),
                        })

            # (d) NTA history from inception to as_of
            nta_hist = await db.fetch(
                "SELECT date, nta FROM historical WHERE date <= $1 ORDER BY date ASC", as_of)

            # Distributions up to as_of
            dists = await db.fetch(
                "SELECT * FROM distributions WHERE ex_date <= $1 ORDER BY ex_date DESC LIMIT 8", as_of)

            # (e) Period returns — relative to NTA at as_of date
            periods = {}
            if hist_row and nta_val > 0:
                inception_row = await db.fetchrow(
                    "SELECT nta FROM historical ORDER BY date ASC LIMIT 1")
                for label, days in [("1M",30),("3M",90),("6M",180),("1Y",365),("3Y",1095)]:
                    ref_date = as_of - _td(days=days)
                    ref = await db.fetchrow(
                        "SELECT nta FROM historical WHERE date <= $1 ORDER BY date DESC LIMIT 1",
                        ref_date)
                    if ref and ref["nta"] and float(ref["nta"]) > 0:
                        periods[label] = round(
                            (nta_val / float(ref["nta"]) - 1) * 100, 4)
                if inception_row and float(inception_row["nta"] or 0) > 0:
                    total_ret = round(
                        (nta_val / float(inception_row["nta"]) - 1) * 100, 4)
                else:
                    total_ret = 0.0
            else:
                total_ret = 0.0

            perf_data = {'period_returns': periods, 'total_return_pct': total_ret}

            # (f) Largest Holdings as-of: replay transactions up to as_of date
            positions = {}
            trades = await db.fetch("""
                SELECT instrument, asset_class, units, net_amount
                FROM transactions WHERE date <= $1
                ORDER BY date ASC,
                         CASE WHEN units > 0 THEN 0 ELSE 1 END ASC,
                         created_at ASC
            """, as_of)
            for t in trades:
                instr = t['instrument']
                u     = float(t['units'])
                net   = float(t['net_amount'])
                if instr not in positions:
                    positions[instr] = {'units': 0.0, 'total_cost': 0.0,
                                        'asset_class': t['asset_class'] or 'Securities [H]'}
                p = positions[instr]
                if u > 0:                          # BUY
                    p['units']      += u
                    p['total_cost'] += abs(net)
                elif u < 0 and p['units'] > 0:     # SELL
                    sell_u    = min(abs(u), p['units'])
                    proportion         = sell_u / p['units']
                    p['total_cost']   -= p['total_cost'] * proportion
                    p['units']        -= sell_u
                    if p['units'] < 0.0001:
                        p['units'] = 0.0; p['total_cost'] = 0.0

            total_cost_all = sum(
                p['total_cost'] for p in positions.values() if p['units'] > 0.0001)
            holdings = []
            for instr, p in sorted(
                    positions.items(),
                    key=lambda x: x[1]['total_cost'], reverse=True):
                if p['units'] > 0.0001 and total_cost_all > 0:
                    holdings.append({
                        'instrument': instr,
                        'asset_class': p['asset_class'],
                        'total_cost': round(p['total_cost'], 2),
                        'weight_pct': round(p['total_cost'] / total_cost_all * 100, 2),
                    })

            manager_comment = body.get('manager_comment', '')
            # (a) Title uses as_of date
            as_of_label = as_of.strftime('%d %B %Y')
            title = f"ZY-Invest Monthly Factsheet — {as_of_label}"

            pdf_bytes = generate_factsheet(
                fund_data=fund_data,
                holdings=holdings[:10],
                performance=perf_data,
                as_of_date=as_of,
                distributions=[dict(d) for d in dists],
                nta_history=[dict(r) for r in nta_hist],
                sector_data=sector_data,
                manager_comment=manager_comment,
            )
            visibility = 'fund'

        elif stmt_type == 'subscription':
            if not investor_id:
                raise HTTPException(400, "investor_id required")
            inv = await db.fetchrow("""
                SELECT i.*,
                       u.email, u.phone, u.bank_name, u.bank_account_no,
                       u.address_line1, u.address_line2, u.city, u.postcode, u.state, u.country,
                       COALESCE(
                           CASE WHEN i.joined_date IS NOT NULL
                                THEN (CURRENT_DATE - i.joined_date)
                                ELSE (SELECT CURRENT_DATE - MIN(date)
                                      FROM principal_cashflows
                                      WHERE investor_id=i.id)
                           END,
                           0
                       )::int AS days_held
                FROM investors i LEFT JOIN LATERAL (
                SELECT id,name,email,phone,bank_name,bank_account_no,
                       address_line1,address_line2,city,postcode,state,country
                FROM users u2
                WHERE u2.id = (
                    SELECT ih.user_id FROM investor_holders ih
                    WHERE ih.investor_id = i.id
                      AND ih.role = 'primary'
                    ORDER BY ih.created_at ASC LIMIT 1
                )
            ) u ON TRUE
                WHERE i.id = $1
            """, investor_id)
            if not inv: raise HTTPException(404, "Investor not found")
            inv = await _enrich_inv(inv, investor_id)
            # Use selected_date if provided, otherwise latest
            selected_date = selected_date_obj
            if selected_date:
                target = await db.fetchrow("""
                    SELECT * FROM principal_cashflows
                    WHERE investor_id=$1 AND amount>0 AND date=$2::date
                    ORDER BY created_at DESC LIMIT 1
                """, investor_id, selected_date)
            else:
                target = await db.fetchrow("""
                    SELECT * FROM principal_cashflows
                    WHERE investor_id=$1 AND amount>0 ORDER BY date DESC LIMIT 1
                """, investor_id)
            if not target: raise HTTPException(404, "No subscription record found")
            prior_cfs = await db.fetch("""
                SELECT * FROM principal_cashflows
                WHERE investor_id=$1
                  AND (date < $2 OR (date = $2 AND created_at < $3))
                ORDER BY date ASC
            """, investor_id, target['date'], target['created_at'])
            cf_rec = dict(target)
            cf_rec['prior_cashflows'] = [dict(c) for c in (prior_cfs or [])]
            pdf_bytes = generate_subscription(investor=dict(inv), cashflow_record=cf_rec)
            title = f"Subscription Statement — {inv['name']} ({str(cf_rec.get('date',''))[:10]})"
            visibility = 'member'
            inv_id_for_doc = str(investor_id)

        elif stmt_type == 'redemption':
            if not investor_id:
                raise HTTPException(400, "investor_id required")
            inv = await db.fetchrow("""
                SELECT i.*,
                       u.email, u.phone, u.bank_name, u.bank_account_no,
                       u.address_line1, u.address_line2, u.city, u.postcode, u.state, u.country,
                       COALESCE(
                           CASE WHEN i.joined_date IS NOT NULL
                                THEN (CURRENT_DATE - i.joined_date)
                                ELSE (SELECT CURRENT_DATE - MIN(date)
                                      FROM principal_cashflows
                                      WHERE investor_id=i.id)
                           END,
                           0
                       )::int AS days_held
                FROM investors i LEFT JOIN LATERAL (
                SELECT id,name,email,phone,bank_name,bank_account_no,
                       address_line1,address_line2,city,postcode,state,country
                FROM users u2
                WHERE u2.id = (
                    SELECT ih.user_id FROM investor_holders ih
                    WHERE ih.investor_id = i.id
                      AND ih.role = 'primary'
                    ORDER BY ih.created_at ASC LIMIT 1
                )
            ) u ON TRUE
                WHERE i.id = $1
            """, investor_id)
            if not inv: raise HTTPException(404, "Investor not found")
            inv = await _enrich_inv(inv, investor_id)
            selected_date = selected_date_obj
            if selected_date:
                target = await db.fetchrow("""
                    SELECT * FROM principal_cashflows
                    WHERE investor_id=$1 AND amount<0 AND date=$2::date
                    ORDER BY created_at DESC LIMIT 1
                """, investor_id, selected_date)
            else:
                target = await db.fetchrow("""
                    SELECT * FROM principal_cashflows
                    WHERE investor_id=$1 AND amount<0 ORDER BY date DESC LIMIT 1
                """, investor_id)
            if not target: raise HTTPException(404, "No redemption record found")
            prior_cfs = await db.fetch("""
                SELECT * FROM principal_cashflows
                WHERE investor_id=$1
                  AND (date < $2 OR (date = $2 AND created_at < $3))
                ORDER BY date ASC
            """, investor_id, target['date'], target['created_at'])
            red_rec = await db.fetchrow("""
                SELECT * FROM redemption_ledger
                WHERE cashflow_id=$1 ORDER BY created_at DESC LIMIT 1
            """, target['id'])
            cf_rec = dict(target)
            cf_rec['prior_cashflows'] = [dict(c) for c in (prior_cfs or [])]
            if red_rec:
                cf_rec.update({k: float(red_rec[k]) for k in
                    ['realized_pl','cost_basis','redemption_value','avg_cost_at_redemption']})
            pdf_bytes = generate_redemption(investor=dict(inv), cashflow_record=cf_rec)
            title = f"Redemption Statement — {inv['name']} ({str(cf_rec.get('date',''))[:10]})"
            visibility = 'member'
            inv_id_for_doc = str(investor_id)

        elif stmt_type == 'dividend':
            if not investor_id:
                raise HTTPException(400, "investor_id required")
            inv = await db.fetchrow("""
                SELECT i.*,
                       u.email, u.phone, u.bank_name, u.bank_account_no,
                       u.address_line1, u.address_line2, u.city, u.postcode, u.state, u.country,
                       COALESCE(
                           CASE WHEN i.joined_date IS NOT NULL
                                THEN (CURRENT_DATE - i.joined_date)
                                ELSE (SELECT CURRENT_DATE - MIN(date)
                                      FROM principal_cashflows
                                      WHERE investor_id=i.id)
                           END,
                           0
                       )::int AS days_held
                FROM investors i LEFT JOIN LATERAL (
                SELECT id,name,email,phone,bank_name,bank_account_no,
                       address_line1,address_line2,city,postcode,state,country
                FROM users u2
                WHERE u2.id = (
                    SELECT ih.user_id FROM investor_holders ih
                    WHERE ih.investor_id = i.id
                      AND ih.role = 'primary'
                    ORDER BY ih.created_at ASC LIMIT 1
                )
            ) u ON TRUE
                WHERE i.id = $1
            """, investor_id)
            if not inv: raise HTTPException(404, "Investor not found")
            inv = await _enrich_inv(inv, investor_id)
            # Get distribution ledger for this investor (most recent record for FY)
            dists = await db.fetch("""
                SELECT dl.units_at_ex_date, dl.amount, dl.paid,
                       d.title, d.dist_type, d.dps_sen, d.ex_date, d.pmt_date,
                       d.financial_year, d.payout_ratio
                FROM distribution_ledger dl
                JOIN distributions d ON d.id = dl.distribution_id
                WHERE dl.investor_id = $1
                  AND ($2 = '' OR d.financial_year = $2)
                ORDER BY d.ex_date DESC
            """, investor_id, fin_year)
            if not dists: raise HTTPException(404, "No dividend records found for this investor/FY")
            # Use selected distribution title if provided, else most recent
            selected_dist = body.get('selected_distribution')
            if selected_dist:
                matching = [d for d in dists if d.get('title') == selected_dist]
                dist_rec = dict(matching[0]) if matching else dict(dists[0])
            else:
                dist_rec = dict(dists[0])
            pdf_bytes = generate_dividend_statement(
                investor=dict(inv), dist_record=dist_rec, financial_year=fin_year or dist_rec.get('financial_year',''))
            title = f"Dividend Statement {fin_year} — {inv['name']}"
            visibility = 'member'
            inv_id_for_doc = str(investor_id)

        elif stmt_type == 'account':
            if not investor_id:
                raise HTTPException(400, "investor_id required")
            inv = await db.fetchrow("""
                SELECT i.*, u.email, u.phone, u.bank_name, u.bank_account_no,
                       u.address_line1, u.address_line2, u.city, u.postcode, u.state, u.country
                FROM investors i
                LEFT JOIN LATERAL (
                SELECT id,name,email,phone,bank_name,bank_account_no,
                       address_line1,address_line2,city,postcode,state,country
                FROM users u2
                WHERE u2.id = (
                    SELECT ih.user_id FROM investor_holders ih
                    WHERE ih.investor_id = i.id
                      AND ih.role = 'primary'
                    ORDER BY ih.created_at ASC LIMIT 1
                )
            ) u ON TRUE
                WHERE i.id = $1
            """, investor_id)
            if not inv:
                raise HTTPException(404, "Investor not found")
            inv = await _enrich_inv(inv, investor_id)
            summary = await db.fetchrow("SELECT * FROM v_investor_profile WHERE id = $1", investor_id)
            if not summary:
                raise HTTPException(404, "Investor profile not found")
            # Override realized_pl with sum from redemption_ledger (pure redemption P&L only)
            redemption_pl = await db.fetchval("""
                SELECT COALESCE(SUM(realized_pl), 0)
                FROM redemption_ledger
                WHERE investor_id = $1
            """, investor_id)
            summary = dict(summary)
            summary['realized_pl'] = float(redemption_pl or 0)

            # Parse FY period dates (Dec 1 - Nov 30)
            if fin_year and fin_year.startswith('FY'):
                yr = int(fin_year[2:]) + 2000
                fy_start = date(yr-1, 12, 1)
                fy_end   = date(yr,   11, 30)
                period   = period or f"{fy_start.strftime('%d/%m/%Y')} - {fy_end.strftime('%d/%m/%Y')}"
            else:
                fy_start, fy_end = date(2000,1,1), date.today()

            cfs = await db.fetch("""
                SELECT date, cashflow_type, amount, nta_at_date, units
                FROM principal_cashflows
                WHERE investor_id = $1 AND date BETWEEN $2 AND $3
                ORDER BY date
            """, investor_id, fy_start, fy_end)

            dists = await db.fetch("""
                SELECT dl.units_at_ex_date, dl.amount,
                       d.title, d.dps_sen, d.ex_date, d.pmt_date, d.financial_year
                FROM distribution_ledger dl
                JOIN distributions d ON d.id = dl.distribution_id
                WHERE dl.investor_id = $1
                  AND d.ex_date BETWEEN $2 AND $3
                ORDER BY d.ex_date
            """, investor_id, fy_start, fy_end)

            # Build cashflow rows with description
            cfs_list = []
            cum_units = 0
            for cf in cfs:
                u = float(cf['units'])
                cum_units += u
                cfs_list.append({
                    **dict(cf),
                    'description': ('Subscription' if float(cf['amount']) > 0 else 'Redemption')
                                  + (' @ Fund Switching' if cf.get('notes','').lower().find('switch') >= 0 else ''),
                })

            # Days held
            first_cf = await db.fetchrow("""
                SELECT date FROM principal_cashflows WHERE investor_id=$1 ORDER BY date ASC LIMIT 1
            """, investor_id)
            days_held = (date.today() - first_cf['date']).days if first_cf else 0

            # Dividend received total
            div_total = await db.fetchval("""
                SELECT COALESCE(SUM(dl.amount),0)
                FROM distribution_ledger dl
                JOIN distributions d ON d.id = dl.distribution_id
                WHERE dl.investor_id=$1 AND dl.paid=TRUE
            """, investor_id) or 0

            sum_data = {
                **dict(summary),
                'days_held': days_held,
                'dividends_received': float(div_total),
                'adjustment': 0,
                'account_type': 'Nominee Account',
            }

            pdf_bytes = generate_account_statement(
                investor=dict(inv),
                summary=sum_data,
                cashflows=cfs_list,
                dist_history=[dict(d) for d in dists],
                statement_period=period,
                financial_year=fin_year
            )
            title = f"Account Statement {fin_year} — {inv['name']}"
            visibility = 'member'
            inv_id_for_doc = str(investor_id)

        else:
            raise HTTPException(400, f"Unknown statement_type: {stmt_type}")

        if not pdf_bytes:
            raise HTTPException(500, "PDF generation failed")

        # Store in documents table
        import base64 as _b64mod
        file_b64  = _b64mod.b64encode(pdf_bytes).decode()
        # Store as data URI so it is self-contained
        file_url  = f"data:application/pdf;base64,{file_b64}"
        file_name = (title
            .replace(' ', '_').replace('—', '-').replace('/', '-')
            .replace(':', '').replace('*', '')) + '.pdf'
        file_size = len(pdf_bytes) // 1024

        # Map stmt_type to valid doc_type (schema CHECK constraint)
        _doc_type_map = {
            'factsheet':    'annual_report',
            'subscription': 'member_statement',
            'redemption':   'member_statement',
            'dividend':     'distribution_notice',
            'account':      'member_statement',
        }
        _doc_type = _doc_type_map.get(stmt_type, 'member_statement')

        doc_id = await db.fetchval("""
            INSERT INTO documents
                (title, doc_type, file_url, file_name, file_size_kb,
                 visibility, investor_id, financial_year, uploaded_by)
            VALUES ($1,$2,$3,$4,$5,$6,$7::uuid,$8,$9::uuid)
            RETURNING id
        """,
            title, _doc_type, file_url, file_name, file_size,
            visibility,
            inv_id_for_doc,
            fin_year or None,
            str(admin['id'])
        )

        return {
            "message": "Statement generated",
            "doc_id": str(doc_id),
            "title": title,
            "file_name": file_name,
            "file_size_kb": file_size,
            "pdf_b64": file_b64,
        }
    except HTTPException:
        raise
    except Exception as exc:
        import traceback as _tb2
        print(f"[generate_statement] {type(exc).__name__}: {exc}")
        print(_tb2.format_exc())
        raise HTTPException(status_code=500, detail=str(exc))


# ── Auto-generate Account Statements for completed FYs ────────
@router.post("/statements/generate-fy-statements")
async def generate_fy_statements(
    body: dict,
    admin: dict = Depends(require_admin),
    db: Database = Depends(get_db)
):
    """Auto-generate Investment Account Statements for all investors for completed FYs."""
    import base64
    from services.pdf_statements import generate_account_statement
    from datetime import date as date_t

    today = date_t.today()
    # Completed FYs: FY22=Dec21-Nov22, FY23=Dec22-Nov23, FY24=Dec23-Nov24, FY25=Dec24-Nov25
    # FY26 not included as Nov 2026 hasn't passed
    fy_ranges = []
    for yr in range(22, 26):  # FY22 to FY25
        fy_label = f"FY{yr}"
        start = date_t(2000 + yr - 1, 12, 1)
        end   = date_t(2000 + yr,     11, 30)
        if today > end:
            fy_ranges.append((fy_label, start, end))

    specific_fy = body.get('financial_year')
    if specific_fy:
        yr = int(specific_fy[2:])
        fy_ranges = [(specific_fy, date_t(2000+yr-1,12,1), date_t(2000+yr,11,30))]

    investors = await db.fetch("SELECT id, name FROM investors WHERE is_active=TRUE")
    generated = 0
    errors = []

    for inv in investors:
        inv_id = str(inv['id'])
        inv_full = await db.fetchrow("""
            SELECT i.*, u.email, u.phone, u.bank_name, u.bank_account_no,
                   u.address_line1, u.address_line2, u.city, u.postcode, u.state, u.country
            FROM investors i
            LEFT JOIN LATERAL (
                SELECT id,name,email,phone,bank_name,bank_account_no,
                       address_line1,address_line2,city,postcode,state,country
                FROM users u2
                WHERE u2.id = (
                    SELECT ih.user_id FROM investor_holders ih
                    WHERE ih.investor_id = i.id
                      AND ih.role = 'primary'
                    ORDER BY ih.created_at ASC LIMIT 1
                )
            ) u ON TRUE
            WHERE i.id=$1
        """, inv_id)
        if not inv_full: continue
        summary = await db.fetchrow("SELECT * FROM v_investor_profile WHERE id=$1", inv_id)
        if not summary: continue

        for fy_label, fy_start, fy_end in fy_ranges:
            # Skip if already generated
            existing = await db.fetchval("""
                SELECT id FROM documents WHERE investor_id=$1 AND financial_year=$2
                AND doc_type='member_statement' AND title LIKE '%Account Statement%'
            """, inv_id, fy_label)
            if existing: continue

            cfs = await db.fetch("""
                SELECT date, cashflow_type, amount, nta_at_date, units
                FROM principal_cashflows
                WHERE investor_id=$1 AND date BETWEEN $2 AND $3 ORDER BY date
            """, inv_id, fy_start, fy_end)

            dists = await db.fetch("""
                SELECT dl.units_at_ex_date, dl.amount, d.title, d.dps_sen,
                       d.ex_date, d.pmt_date, d.financial_year
                FROM distribution_ledger dl
                JOIN distributions d ON d.id=dl.distribution_id
                WHERE dl.investor_id=$1 AND d.ex_date BETWEEN $2 AND $3
                ORDER BY d.ex_date
            """, inv_id, fy_start, fy_end)

            first_cf = await db.fetchrow("""
                SELECT date FROM principal_cashflows WHERE investor_id=$1 ORDER BY date ASC LIMIT 1
            """, inv_id)
            days_held = (date_t.today() - first_cf['date']).days if first_cf else 0
            div_total = await db.fetchval("""
                SELECT COALESCE(SUM(dl.amount),0) FROM distribution_ledger dl
                JOIN distributions d ON d.id=dl.distribution_id
                WHERE dl.investor_id=$1 AND dl.paid=TRUE AND d.ex_date BETWEEN $2 AND $3
            """, inv_id, fy_start, fy_end) or 0

            cfs_list = []
            running = 0.0
            for cf in cfs:
                u = float(cf['units']); amt = float(cf['amount'])
                running += u
                cfs_list.append({**dict(cf),
                    'description': 'Subscription' if amt > 0 else 'Redemption'})

            sum_data = {
                **dict(summary), 'days_held': days_held,
                'dividends_received': float(div_total),
                'adjustment': 0, 'account_type': 'Nominee Account',
            }
            period = f"{fy_start.strftime('%d/%m/%Y')} - {fy_end.strftime('%d/%m/%Y')}"

            try:
                pdf_b = generate_account_statement(
                    investor=dict(inv_full), summary=sum_data,
                    cashflows=cfs_list, dist_history=[dict(d) for d in dists],
                    statement_period=period, financial_year=fy_label)
                b64 = base64.b64encode(pdf_b).decode()
                title = f"Account Statement {fy_label} — {inv['name']}"
                fname = f"Account_{fy_label}_{inv['name'].replace(' ','_')}.pdf"
                await db.execute("""
                    INSERT INTO documents (title,doc_type,file_url,file_name,file_size_kb,
                        visibility,investor_id,financial_year,uploaded_by)
                    VALUES ($1,$2,$3,$4,$5,$6,$7::uuid,$8,$9::uuid)
                """, title,'member_statement',f"data:application/pdf;base64,{b64}",
                    fname, len(pdf_b)//1024, 'member', inv_id, fy_label, str(admin['id']))
                generated += 1
            except Exception as e:
                errors.append(f"{inv['name']} {fy_label}: {str(e)[:80]}")

    return {"generated": generated, "errors": errors[:10]}

# ── Get available cashflow dates for statement generation ─────
@router.get("/investors/{investor_id}/cashflow-dates")
async def get_cashflow_dates(
    investor_id: str,
    admin: dict = Depends(require_admin),
    db: Database = Depends(get_db)
):
    """Return all subscription and redemption dates for date picker."""
    rows = await db.fetch("""
        SELECT date, cashflow_type, amount, units, nta_at_date
        FROM principal_cashflows
        WHERE investor_id = $1
        ORDER BY date DESC
    """, investor_id)
    return [serialise(r) for r in rows]


@router.get("/investors/{investor_id}/distribution-list")
async def get_distribution_list(
    investor_id: str,
    admin: dict = Depends(require_admin),
    db: Database = Depends(get_db)
):
    """Return all distributions for this investor (for dropdown)."""
    rows = await db.fetch("""
        SELECT d.title, d.financial_year, d.dist_type, d.ex_date, d.pmt_date, d.dps_sen
        FROM distribution_ledger dl
        JOIN distributions d ON d.id = dl.distribution_id
        WHERE dl.investor_id = $1
        ORDER BY d.pmt_date DESC
    """, investor_id)
    return [serialise(r) for r in rows]

# ── Document Management ───────────────────────────────────────
@router.get("/documents")
async def list_documents(
    page:           int = 1,
    limit:          int = 20,
    doc_type:       str = None,
    investor_id:    str = None,
    financial_year: str = None,
    visibility:     str = None,
    search:         str = None,
    admin: dict = Depends(require_admin),
    db: Database = Depends(get_db)
):
    offset = (page - 1) * limit
    where, params, i = ["1=1"], [], 1
    if doc_type:
        where.append(f"d.doc_type=${i}"); params.append(doc_type); i += 1
    if investor_id:
        where.append(f"d.investor_id=${i}::uuid"); params.append(investor_id); i += 1
    if financial_year:
        where.append(f"d.financial_year=${i}"); params.append(financial_year); i += 1
    if visibility:
        where.append(f"d.visibility=${i}"); params.append(visibility); i += 1
    if search:
        where.append(f"(d.title ILIKE ${i} OR d.file_name ILIKE ${i})")
        params.append(f"%{search}%"); i += 1
    w = " AND ".join(where)

    total = await db.fetchval(f"""
        SELECT COUNT(*) FROM documents d
        LEFT JOIN investors i ON i.id = d.investor_id
        WHERE {w}
    """, *params)
    rows = await db.fetch(f"""
        SELECT d.id, d.title, d.doc_type, d.file_name, d.file_size_kb,
               d.visibility, d.investor_id, d.financial_year,
               d.created_at, i.name AS investor_name
        FROM documents d
        LEFT JOIN investors i ON i.id = d.investor_id
        WHERE {w}
        ORDER BY d.created_at DESC
        LIMIT {limit} OFFSET {offset}
    """, *params)
    return {"total": total, "page": page, "limit": limit,
            "items": [serialise(r) for r in rows]}


@router.get("/documents/{doc_id}/download")
async def download_document(
    doc_id: str,
    admin: dict = Depends(require_admin),
    db: Database = Depends(get_db)
):
    row = await db.fetchrow(
        "SELECT title, file_name, file_url FROM documents WHERE id=$1::uuid", doc_id)
    if not row:
        raise HTTPException(404, "Document not found")
    file_url = row['file_url'] or ''
    if not file_url:
        raise HTTPException(404, "No file stored for this document")
    # Extract raw base64 from data URI  (data:application/pdf;base64,XXXX)
    if ',' in file_url:
        b64 = file_url.split(',', 1)[1]
    else:
        b64 = file_url   # already raw base64
    if not b64:
        raise HTTPException(404, "File data is empty")
    return {
        "title":     row['title'],
        "file_name": row['file_name'],
        "pdf_b64":   b64,
    }


@router.delete("/documents/{doc_id}")
async def delete_document(
    doc_id: str,
    admin: dict = Depends(require_admin),
    db: Database = Depends(get_db)
):
    deleted = await db.fetchval(
        "DELETE FROM documents WHERE id=$1::uuid RETURNING id", doc_id)
    if not deleted:
        raise HTTPException(404, "Document not found")
    return {"message": "Document deleted"}
# ── Investor account_type + holder management ─────────────────
@router.put("/investors/{investor_id}")
async def update_investor(
    investor_id: str,
    body: dict,
    admin: dict = Depends(require_admin),
    db: Database = Depends(get_db)
):
    allowed = {"account_type", "notes", "is_active"}
    fields, vals, i = [], [], 1
    for k, v in body.items():
        if k in allowed:
            fields.append(f"{k}=${i}"); vals.append(v); i += 1
    if not fields:
        return {"message": "Nothing to update"}
    vals.append(investor_id)
    await db.execute(
        f"UPDATE investors SET {','.join(fields)}, updated_at=NOW() "
        f"WHERE id=${i}::uuid", *vals)
    return {"message": "Investor updated"}


@router.get("/investors/{investor_id}/holders")
async def list_holders(
    investor_id: str,
    admin: dict = Depends(require_admin),
    db: Database = Depends(get_db)
):
    rows = await db.fetch("""
        SELECT ih.id, ih.investor_id, ih.user_id, ih.role,
               ih.share_ratio, ih.created_at,
               u.name, u.email, u.phone, u.is_active
        FROM investor_holders ih
        JOIN users u ON u.id = ih.user_id
        WHERE ih.investor_id = $1
        ORDER BY CASE ih.role WHEN 'primary' THEN 0 ELSE 1 END, ih.created_at
    """, investor_id)
    return [serialise(r) for r in rows]


@router.post("/investors/{investor_id}/holders")
async def add_holder(
    investor_id: str,
    body: dict,
    admin: dict = Depends(require_admin),
    db: Database = Depends(get_db)
):
    user_id     = body.get("user_id")
    role        = body.get("role", "secondary")
    share_ratio = float(body.get("share_ratio", 0))
    if role not in ("primary", "secondary"):
        raise HTTPException(400, "role must be primary or secondary")
    if share_ratio <= 0 or share_ratio > 100:
        raise HTTPException(400, "share_ratio must be between 0 and 100")

    # If promoting to primary, demote current primary → secondary
    if role == "primary":
        await db.execute("""
            UPDATE investor_holders SET role='secondary'
            WHERE investor_id=$1::uuid AND role='primary'
        """, investor_id)
        # Sync users.investor_id for the new primary
        await db.execute(
            "UPDATE users SET investor_id=$1::uuid WHERE id=$2::uuid",
            investor_id, user_id)

    await db.execute("""
        INSERT INTO investor_holders (investor_id, user_id, role, share_ratio)
        VALUES ($1::uuid, $2::uuid, $3, $4)
        ON CONFLICT (investor_id, user_id)
        DO UPDATE SET role=EXCLUDED.role, share_ratio=EXCLUDED.share_ratio
    """, investor_id, user_id, role, share_ratio)
    return {"message": "Holder added"}


@router.put("/investors/{investor_id}/holders/{holder_id}")
async def update_holder(
    investor_id: str,
    holder_id: str,
    body: dict,
    admin: dict = Depends(require_admin),
    db: Database = Depends(get_db)
):
    role        = body.get("role")
    share_ratio = body.get("share_ratio")

    if role and role not in ("primary", "secondary"):
        raise HTTPException(400, "role must be primary or secondary")

    if role == "primary":
        # Get user_id for the new primary before demoting others
        row = await db.fetchrow(
            "SELECT user_id FROM investor_holders WHERE id=$1::uuid", holder_id)
        if row:
            await db.execute("""
                UPDATE investor_holders SET role='secondary'
                WHERE investor_id=$1::uuid AND role='primary' AND id!=$2::uuid
            """, investor_id, holder_id)
            await db.execute(
                "UPDATE users SET investor_id=$1::uuid WHERE id=$2::uuid",
                investor_id, str(row["user_id"]))

    fields, vals, i = [], [], 1
    if role:        fields.append(f"role=${i}");        vals.append(role);        i += 1
    if share_ratio is not None:
        fields.append(f"share_ratio=${i}"); vals.append(float(share_ratio)); i += 1
    if fields:
        vals.extend([holder_id, investor_id])
        await db.execute(
            f"UPDATE investor_holders SET {','.join(fields)} "
            f"WHERE id=${i}::uuid AND investor_id=${i+1}::uuid", *vals)
    return {"message": "Holder updated"}


@router.delete("/investors/{investor_id}/holders/{holder_id}")
async def remove_holder(
    investor_id: str,
    holder_id: str,
    admin: dict = Depends(require_admin),
    db: Database = Depends(get_db)
):
    row = await db.fetchrow(
        "SELECT role FROM investor_holders "
        "WHERE id=$1::uuid AND investor_id=$2::uuid",
        holder_id, investor_id)
    if not row:
        raise HTTPException(404, "Holder not found")
    if row["role"] == "primary":
        raise HTTPException(400, "Cannot remove primary holder. Promote another holder first.")
    await db.execute(
        "DELETE FROM investor_holders WHERE id=$1::uuid", holder_id)
    return {"message": "Holder removed"}


# ── Nominees per investor (admin view across all holders) ─────
@router.get("/investors/{investor_id}/nominees")
async def list_all_nominees(
    investor_id: str,
    admin: dict = Depends(require_admin),
    db: Database = Depends(get_db)
):
    """Returns nominees for all holders of this investor account."""
    rows = await db.fetch("""
        SELECT n.id, n.holder_user_id, n.name, n.phone, n.email,
               n.address_line1, n.address_line2, n.city, n.postcode,
               n.state, n.country, n.relationship, n.created_at,
               u.name AS holder_name, ih.role AS holder_role
        FROM investor_holders ih
        JOIN users u ON u.id = ih.user_id
        LEFT JOIN nominees n ON n.holder_user_id = ih.user_id
        WHERE ih.investor_id = $1
        ORDER BY CASE ih.role WHEN 'primary' THEN 0 ELSE 1 END, n.name
    """, investor_id)
    return [serialise(r) for r in rows]


