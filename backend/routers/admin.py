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
    rows = await db.fetch("SELECT * FROM v_investor_profile ORDER BY name")
    return [dict(r) for r in rows]


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
    rows = await db.fetch(
        "SELECT id, name, email, phone, role, is_active, investor_id, created_at FROM users ORDER BY name"
    )
    return [dict(r) for r in rows]


class UserUpdate(BaseModel):
    name: Optional[str]
    email: Optional[str]
    phone: Optional[str]
    role: Optional[str]
    is_active: Optional[bool]
    investor_id: Optional[str]


@router.put("/users/{user_id}")
async def update_user(
    user_id: str,
    body: UserUpdate,
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
    vals.append(user_id)
    await db.execute(
        f"UPDATE users SET {', '.join(fields)}, updated_at = NOW() WHERE id = ${idx}",
        *vals
    )
    await db.execute(
        "INSERT INTO audit_log (user_id, action, table_name, record_id) VALUES ($1,$2,$3,$4)",
        str(admin["id"]), "UPDATE", "users", user_id
    )
    return {"message": "User updated"}


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
@router.get("/financials/income-statement")
async def get_income_statement(
    admin: dict = Depends(require_admin),
    db: Database = Depends(get_db)
):
    """
    Income Statement per Financial Year (Dec 1 → Nov 30).
    Returns all FYs from inception to latest, plus a 'all' summary.

    Structure:
      Revenue:
        Dividend Income     = SUM(dividends.amount) where pmt_date in FY
        Interest/Fixed      = SUM(others.amount) where income_type ILIKE '%interest%'
                            + SUM(settlement.profit_loss) where asset_class='Money Market'
      Costs:
        Management Fees     = historical FYE mng_fees - historical FYB mng_fees + fee_withdrawals(management)
        Performance Fees    = historical FYE perf_fees - historical FYB perf_fees + fee_withdrawals(performance)
      Gross Profit = Revenue - Costs
      Other Income:
        Realised P&L        = SUM(settlement.profit_loss) excl Money Market
        Unrealised Income   = FYE historical earnings - FYB historical earnings - net_realised
        Others              = SUM(others.amount) excl interest
      Net Profit = FYE historical earnings (retained earnings at year end)
    """
    from datetime import date as date_t

    # Get fund settings for FYE month/day
    settings = await db.fetchrow("SELECT * FROM fund_settings WHERE id=1")
    fye_str  = (settings['financial_year_end'] if settings else '11-30')
    fye_mm, fye_dd = int(fye_str.split('-')[0]), int(fye_str.split('-')[1])

    # Build list of FYs from inception
    inc_row  = await db.fetchrow("SELECT MIN(date) AS d FROM historical")
    inc_date = inc_row['d'] if inc_row and inc_row['d'] else date_t(2021,12,13)
    today    = date_t.today()

    # FY periods: FYB = Dec 1 of previous year, FYE = Nov 30 of FY year
    def fy_period(fy_year: int):
        fyb = date_t(fy_year - 1, fye_mm + 1 if fye_mm < 12 else 1,
                     1 if fye_mm < 11 else 1)
        # Simpler: FYE is Nov 30, FYB is Dec 1 of prior year
        fyb = date_t(fy_year - 1, 12, 1)
        fye = date_t(fy_year,     11, 30)
        return fyb, fye

    # Determine FY years to compute
    start_fy = inc_date.year if inc_date.month >= 12 else inc_date.year
    end_fy   = today.year if today.month >= 12 else today.year
    fy_years = list(range(start_fy, end_fy + 1))

    results = []

    for fy in fy_years:
        fyb, fye = fy_period(fy)
        if fye > today: fye = today   # current FY not complete
        fy_label = f"FY{str(fy)[2:]}"

        # ── Revenue ──────────────────────────────────────────
        # Dividend Income
        div_row = await db.fetchrow("""
            SELECT COALESCE(SUM(amount),0) AS n FROM dividends
            WHERE pmt_date >= $1 AND pmt_date <= $2
        """, fyb, fye)
        dividend_income = float(div_row['n'])

        # Interest income: others type interest
        int_row = await db.fetchrow("""
            SELECT COALESCE(SUM(amount),0) AS n FROM others
            WHERE record_date >= $1 AND record_date <= $2
              AND LOWER(income_type) LIKE '%interest%'
        """, fyb, fye)
        # Money market settlement P&L
        mm_row = await db.fetchrow("""
            SELECT COALESCE(SUM(profit_loss),0) AS n FROM settlement
            WHERE date >= $1 AND date <= $2
              AND LOWER(asset_class) LIKE '%money market%'
        """, fyb, fye)
        interest_income = float(int_row['n']) + float(mm_row['n'])

        revenue = dividend_income + interest_income

        # ── Costs ─────────────────────────────────────────────
        # Get historical rows at FYB-1 and FYE
        hist_fyb = await db.fetchrow("""
            SELECT mng_fees, perf_fees FROM historical
            WHERE date <= $1 ORDER BY date DESC LIMIT 1
        """, fyb - __import__('datetime').timedelta(days=1))
        hist_fye = await db.fetchrow("""
            SELECT mng_fees, perf_fees FROM historical
            WHERE date <= $1 ORDER BY date DESC LIMIT 1
        """, fye)

        mng_fyb  = float(hist_fyb['mng_fees'])  if hist_fyb  else 0.0
        mng_fye  = float(hist_fye['mng_fees'])  if hist_fye  else 0.0
        perf_fyb = float(hist_fyb['perf_fees']) if hist_fyb  else 0.0
        perf_fye = float(hist_fye['perf_fees']) if hist_fye  else 0.0

        fw_mng = await db.fetchrow("""
            SELECT COALESCE(SUM(amount),0) AS n FROM fee_withdrawals
            WHERE date >= $1 AND date <= $2 AND fee_type='management'
        """, fyb, fye)
        fw_prf = await db.fetchrow("""
            SELECT COALESCE(SUM(amount),0) AS n FROM fee_withdrawals
            WHERE date >= $1 AND date <= $2 AND fee_type='performance'
        """, fyb, fye)

        mng_cost  = max(0.0, (mng_fye  - mng_fyb)  + float(fw_mng['n']))
        perf_cost = max(0.0, (perf_fye - perf_fyb) + float(fw_prf['n']))
        total_costs = mng_cost + perf_cost

        gross_profit = revenue - total_costs

        # ── Other Income ──────────────────────────────────────
        # Realised P&L (excl money market)
        real_row = await db.fetchrow("""
            SELECT COALESCE(SUM(profit_loss),0) AS n FROM settlement
            WHERE date >= $1 AND date <= $2
              AND LOWER(asset_class) NOT LIKE '%money market%'
        """, fyb, fye)
        realised = float(real_row['n'])

        # Others (excl interest)
        oth_row = await db.fetchrow("""
            SELECT COALESCE(SUM(amount),0) AS n FROM others
            WHERE record_date >= $1 AND record_date <= $2
              AND LOWER(income_type) NOT LIKE '%interest%'
        """, fyb, fye)
        other_income_misc = float(oth_row['n'])

        # Unrealised = FYE earnings - FYB earnings - realised
        earn_fyb_row = await db.fetchrow("""
            SELECT earnings FROM historical
            WHERE date <= $1 ORDER BY date DESC LIMIT 1
        """, fyb - __import__('datetime').timedelta(days=1))
        earn_fye_row = await db.fetchrow("""
            SELECT earnings FROM historical
            WHERE date <= $1 ORDER BY date DESC LIMIT 1
        """, fye)
        earn_fyb   = float(earn_fyb_row['earnings']) if earn_fyb_row else 0.0
        earn_fye   = float(earn_fye_row['earnings']) if earn_fye_row else 0.0
        unrealised = (earn_fye - earn_fyb) - realised - other_income_misc

        other_total = realised + unrealised + other_income_misc

        # Net Profit = FYE retained earnings
        net_profit = earn_fye

        results.append({
            "fy":              fy_label,
            "fy_year":         fy,
            "fyb":             str(fyb),
            "fye":             str(fye),
            "is_current":      fye >= today,
            # Revenue
            "dividend_income": round(dividend_income, 2),
            "interest_income": round(interest_income, 2),
            "revenue":         round(revenue, 2),
            # Costs
            "mng_cost":        round(mng_cost, 2),
            "perf_cost":       round(perf_cost, 2),
            "total_costs":     round(total_costs, 2),
            # Gross
            "gross_profit":    round(gross_profit, 2),
            # Other Income
            "realised":        round(realised, 2),
            "unrealised":      round(unrealised, 2),
            "other_misc":      round(other_income_misc, 2),
            "other_total":     round(other_total, 2),
            # Net
            "net_profit":      round(net_profit, 2),
        })

    return results


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
    import bcrypt
    await db.execute("""
        UPDATE users SET name=$1, email=$2, phone=$3, role=$4,
               is_active=$5, investor_id=$6, updated_at=NOW()
        WHERE id=$7
    """,
        body['name'], body['email'], body.get('phone'),
        body.get('role','member'), body.get('is_active', True),
        body.get('investor_id'), user_id,
    )
    # Reset password if provided
    if body.get('new_password'):
        pw_hash = bcrypt.hashpw(body['new_password'].encode(), bcrypt.gensalt()).decode()
        await db.execute(
            "UPDATE users SET password_hash=$1 WHERE id=$2",
            pw_hash, user_id
        )
    return {"message": "User updated"}
