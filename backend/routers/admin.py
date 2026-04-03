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
  GET                  /api/admin/audit-log
"""
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from typing import Optional, List
from datetime import date
import io

from database import Database, get_db, serialise
from routers.auth import require_admin
from services.price_fetcher import run_daily_price_fetch, update_manual_price
from services.nta_engine import compute_daily_nta

router = APIRouter()


# ── Investor Management ───────────────────────────────────────
@router.get("/investors")
async def list_investors(
    admin: dict = Depends(require_admin),
    db: Database = Depends(get_db)
):
    rows = await db.fetch("SELECT * FROM v_investor_profile ORDER BY name")
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
    return serialise(row)


# ── User / Account Management ─────────────────────────────────
@router.get("/users")
async def list_users(
    admin: dict = Depends(require_admin),
    db: Database = Depends(get_db)
):
    rows = await db.fetch(
        """SELECT id, name, email, phone, role, is_active, investor_id,
                  bank_name, bank_account_no,
                  address_line1, address_line2, city, postcode, state, country,
                  created_at
           FROM users ORDER BY name"""
    )
    return [serialise(r) for r in rows]


class UserUpdate(BaseModel):
    name: Optional[str] = None
    email: Optional[str] = None
    phone: Optional[str] = None
    role: Optional[str] = None
    is_active: Optional[bool] = None
    investor_id: Optional[str] = None
    new_password: Optional[str] = None
    bank_name: Optional[str] = None
    bank_account_no: Optional[str] = None
    address_line1: Optional[str] = None
    address_line2: Optional[str] = None
    city: Optional[str] = None
    postcode: Optional[str] = None
    state: Optional[str] = None
    country: Optional[str] = None


# ── Fee Schedules ─────────────────────────────────────────────
@router.get("/nta/latest")
async def get_latest_nta(
    admin: dict = Depends(require_admin),
    db:   Database = Depends(get_db)
):
    """Return the most recent historical record (for fee accrual display)."""
    row = await db.fetchrow("SELECT * FROM historical ORDER BY date DESC LIMIT 1")
    return serialise(row) if row else {}


@router.get("/fee-schedules")
async def list_fee_schedules(
    admin: dict = Depends(require_admin),
    db: Database = Depends(get_db)
):
    rows = await db.fetch("SELECT * FROM v_active_fee_schedule")
    return [serialise(r) for r in rows]


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
    return [serialise(r) for r in rows]


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
    admin: dict = Depends(require_admin),
    db: Database = Depends(get_db),
    target_date: Optional[date] = None
):
    result = await compute_daily_nta(db, target_date)
    if not result:
        raise HTTPException(status_code=400, detail="Could not compute NTA — check price data")
    return result


# ── Excel Upload ──────────────────────────────────────────────
# ── Fee Withdrawal ────────────────────────────────────────────
@router.get("/fee-withdrawals")
async def list_fee_withdrawals(
    admin: dict = Depends(require_admin),
    db:    Database = Depends(get_db)
):
    """List all recorded fee withdrawals."""
    rows = await db.fetch("""
        SELECT * FROM fee_withdrawals ORDER BY date DESC
    """)
    return [serialise(r) for r in rows]


@router.post("/fee-withdrawals")
async def record_fee_withdrawal(
    body: dict,
    admin: dict = Depends(require_admin),
    db:   Database = Depends(get_db)
):
    """
    Record a fee withdrawal in the database only.
    NTA computation will pick this up when Compute NTA is run.
    """
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
        raise HTTPException(400, f"Invalid date format: {w_date}")

    rec_id = await db.fetchval("""
        INSERT INTO fee_withdrawals (fee_type, amount, date, notes, created_by)
        VALUES ($1, $2, $3, $4, $5::uuid)
        RETURNING id
    """, fee_type, amount, date_obj, notes, str(admin['id']))

    return {
        "message":  "Fee withdrawal recorded",
        "id":       str(rec_id),
        "date":     w_date,
        "fee_type": fee_type,
        "amount":   amount,
    }


@router.delete("/fee-withdrawals/{withdrawal_id}")
async def delete_fee_withdrawal(
    withdrawal_id: str,
    admin: dict = Depends(require_admin),
    db:   Database = Depends(get_db)
):
    """Delete a fee withdrawal record. Re-run Compute NTA to update historical."""
    row = await db.fetchrow(
        "SELECT id, amount FROM fee_withdrawals WHERE id = $1::uuid", withdrawal_id)
    if not row:
        raise HTTPException(404, "Withdrawal record not found")
    await db.execute(
        "DELETE FROM fee_withdrawals WHERE id = $1::uuid", withdrawal_id)
    return {"message": "Withdrawal deleted. Re-run Compute NTA to update."}


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
    return [serialise(r) for r in rows]


# ── Ticker Map ────────────────────────────────────────────────
@router.get("/ticker-map")
async def get_ticker_map(
    admin: dict = Depends(require_admin),
    db: Database = Depends(get_db)
):
    rows = await db.fetch("SELECT * FROM ticker_map ORDER BY instrument")
    return [serialise(r) for r in rows]


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
    for field, val in body.dict(exclude_none=True).items():
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
        """SELECT t.*, i.name as investor_name
           FROM transactions t
           LEFT JOIN investors i ON i.id = t.investor_id
           ORDER BY t.date DESC LIMIT $1 OFFSET $2""",
        limit, offset
    )
    total = await db.fetchval("SELECT COUNT(*) FROM transactions")
    return {"data": [serialise(r) for r in rows], "total": total, "page": page}


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
        body['investor_id'],
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


# ── Holdings — computed from transactions (VWAP) ─────────────
@router.get("/holdings")
async def get_holdings(
    admin: dict = Depends(require_admin),
    db: Database = Depends(get_db)
):
    """Get current holdings from holdings table. Run /compute first to populate."""
    rows = await db.fetch("""
        SELECT h.*,
               ph.price as last_price,
               CASE
                 WHEN ph.price IS NOT NULL THEN h.units * ph.price
                 ELSE h.total_cost
               END as market_value
        FROM holdings h
        LEFT JOIN (
            SELECT DISTINCT ON (instrument) instrument, price
            FROM price_history
            ORDER BY instrument, date DESC
        ) ph ON ph.instrument = h.instrument
        WHERE h.units > 0.001 OR h.instrument = 'CASH'
        ORDER BY h.instrument = 'CASH' ASC,
                 COALESCE(h.units * ph.price, h.total_cost) DESC NULLS LAST
    """)
    return [serialise(r) for r in rows]


@router.post("/holdings/compute")
async def compute_holdings_and_settlement(
    admin: dict = Depends(require_admin),
    db: Database = Depends(get_db)
):
    """
    Recompute all holdings from transactions using VWAP (net amount basis).
    Uses Python Decimal for exact financial arithmetic — no float rounding errors.
    - BUY:  VWAP = total_net_cost / total_units  (net_amount includes all fees)
    - SELL: realised P&L = (sell_net_per_unit - vwap) * units_sold
    - Saves final positions to holdings table
    - Writes settlement records for all sells
    """
    from decimal import Decimal, ROUND_HALF_UP, getcontext
    getcontext().prec = 28  # 28 significant digits — sufficient for all fund values

    def D(val):
        """Convert any value to Decimal safely."""
        if val is None:
            return Decimal('0')
        return Decimal(str(val))

    rows = await db.fetch("""
        SELECT id, date, instrument, asset_class, sector, region,
               units, price, amount, total_fees, net_amount, theme
        FROM transactions
        ORDER BY date ASC, created_at ASC
    """)

    # positions[instrument] = {units, total_net_cost, avg_cost, ...}
    positions = {}
    settlement_count = 0
    settlement_errors = []

    # Clear existing auto-computed settlement records
    await db.execute("DELETE FROM settlement WHERE remark = 'auto-computed VWAP'")

    for row in rows:
        instr       = row['instrument']
        units       = D(row['units'])
        net_amount  = D(row['net_amount'])
        asset_class = row['asset_class'] or 'Securities [H]'
        sector      = row['sector']
        region      = row['region'] or 'MY'

        if instr not in positions:
            positions[instr] = {
                'units':          Decimal('0'),
                'total_net_cost': Decimal('0'),
                'avg_cost':       Decimal('0'),
                'asset_class': asset_class,
                'sector':      sector,
                'region':      region,
            }

        pos = positions[instr]

        if units > 0:
            # BUY — net_amount is negative (cash outflow), abs() = actual cost paid
            buy_net_cost  = abs(net_amount)
            new_units     = pos['units'] + units
            new_total     = pos['total_net_cost'] + buy_net_cost
            # VWAP = cumulative net cost / cumulative units
            pos['avg_cost']       = new_total / new_units if new_units > 0 else buy_net_cost / units
            pos['total_net_cost'] = new_total
            pos['units']          = new_units
            pos['asset_class']    = asset_class
            pos['sector']         = sector
            pos['region']         = region

        else:
            # SELL — net_amount is positive (cash inflow) = exact proceeds including fees
            # We ALWAYS use net_amount directly — never units × price
            sell_units = abs(units)
            proceeds   = abs(net_amount)  # ground truth from transaction record
            sell_net_per_unit = proceeds / sell_units if sell_units > 0 else D(row['price'] or 0)

            if pos['units'] >= sell_units - Decimal('0.001'):  # tolerance for tiny residuals
                avg_cost   = pos['avg_cost']
                cost_basis = avg_cost * sell_units  # VWAP cost of units being sold
                pl         = proceeds - cost_basis  # ALWAYS net proceeds - cost basis
                ret_pct    = (pl / cost_basis * 100) if cost_basis > 0 else Decimal('0')

                try:
                    await db.execute("""
                        INSERT INTO settlement
                        (date, region, asset_class, sector, instrument,
                         units, bought_price, sale_price, cost_basis, proceeds,
                         profit_loss, return_pct, remark)
                        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
                    """,
                        row['date'],
                        region, asset_class, sector, instr,
                        float(sell_units),
                        float(avg_cost.quantize(Decimal('0.00000001'), ROUND_HALF_UP)),
                        float(sell_net_per_unit.quantize(Decimal('0.00000001'), ROUND_HALF_UP)),
                        float(cost_basis.quantize(Decimal('0.0001'), ROUND_HALF_UP)),
                        float(proceeds.quantize(Decimal('0.0001'), ROUND_HALF_UP)),
                        float(pl.quantize(Decimal('0.0001'), ROUND_HALF_UP)),
                        float(ret_pct.quantize(Decimal('0.0001'), ROUND_HALF_UP)),
                        'auto-computed VWAP'
                    )
                    settlement_count += 1
                except Exception as e:
                    settlement_errors.append(f"{instr} SELL: {e}")

                # Deduct from position — recalculate total_net_cost exactly
                pos['units']          -= sell_units
                pos['total_net_cost']  = pos['avg_cost'] * pos['units']
            else:
                settlement_errors.append(
                    f"SELL {instr}: tried {sell_units} units, only {pos['units']} held"
                )

    # Save final holdings to holdings table
    await db.execute("DELETE FROM holdings")
    holdings_saved = 0
    for instr, pos in positions.items():
        if pos['units'] > Decimal('0.001'):  # filter tiny residuals from decimal CSV imports
            total_cost = pos['avg_cost'] * pos['units']
            try:
                await db.execute("""
                    INSERT INTO holdings
                    (instrument, asset_class, sector, region,
                     units, avg_cost, total_cost, last_updated)
                    VALUES ($1,$2,$3,$4,$5,$6,$7,NOW())
                    ON CONFLICT (instrument) DO UPDATE SET
                        units=EXCLUDED.units,
                        avg_cost=EXCLUDED.avg_cost,
                        total_cost=EXCLUDED.total_cost,
                        last_updated=NOW()
                """,
                    instr,
                    pos['asset_class'],
                    pos['sector'],
                    pos['region'],
                    float(pos['units'].quantize(Decimal('0.00000001'), ROUND_HALF_UP)),
                    float(pos['avg_cost'].quantize(Decimal('0.00000001'), ROUND_HALF_UP)),
                    float(total_cost.quantize(Decimal('0.0001'), ROUND_HALF_UP)),
                )
                holdings_saved += 1
            except Exception as e:
                settlement_errors.append(f"Holdings save {instr}: {e}")

    # ── Cash position ─────────────────────────────────────────
    # Cash = principal (signed) + transactions net + dividends + others - distributions
    # principal_cashflows.amount is already signed: positive=deposit, negative=withdrawal
    cash = Decimal('0')

    # 1. Principal cashflows — amount already signed (positive=in, negative=out)
    pc_rows = await db.fetch("SELECT amount FROM principal_cashflows")
    for r in pc_rows:
        cash += D(r['amount'])

    # 2. Trade transactions — net_amount already signed (buy=negative, sell=positive)
    for row in rows:
        cash += D(row['net_amount'])

    # 3. Others: interest income, management fees, misc (already signed)
    oth_rows = await db.fetch("SELECT amount FROM others")
    for r in oth_rows:
        cash += D(r['amount'])

    # 4. Stock dividends received by fund (cash inflow, always positive)
    div_rows = await db.fetch("SELECT amount FROM dividends")
    for r in div_rows:
        cash += abs(D(r['amount']))

    # 5. Fund distributions paid out to investors (cash outflow)
    dist_rows = await db.fetch(
        "SELECT total_dividend FROM distributions WHERE total_dividend IS NOT NULL"
    )
    for r in dist_rows:
        cash -= abs(D(r['total_dividend']))

    # Save CASH as a holdings row (always — even if negative)
    cash_rounded = cash.quantize(Decimal('0.0001'), ROUND_HALF_UP)
    try:
        await db.execute("""
            INSERT INTO holdings
            (instrument, asset_class, sector, region,
             units, avg_cost, total_cost, last_updated)
            VALUES ($1,$2,$3,$4,$5,$6,$7,NOW())
            ON CONFLICT (instrument) DO UPDATE SET
                units=EXCLUDED.units,
                avg_cost=EXCLUDED.avg_cost,
                total_cost=EXCLUDED.total_cost,
                last_updated=NOW()
        """,
            'CASH',
            'Currencies',
            'Cash & Equivalents',
            'MY',
            float(cash_rounded),
            float(Decimal('1')),
            float(cash_rounded),
        )
        holdings_saved += 1
    except Exception as e:
        settlement_errors.append(f"Cash position: {e}")

    return {
        "message": "Holdings and settlement recomputed",
        "positions": holdings_saved,
        "settlement_records": settlement_count,
        "cash_balance": float(cash.quantize(Decimal('0.0001'), ROUND_HALF_UP)),
        "errors": settlement_errors[:5] if settlement_errors else []
    }


# ── Principal cashflow ────────────────────────────────────────
@router.get("/principal")
async def get_principal(
    admin: dict = Depends(require_admin),
    db: Database = Depends(get_db)
):
    rows = await db.fetch("""
        SELECT p.*,
               p.cashflow_type,
               i.name as investor_name
        FROM principal_cashflows p
        LEFT JOIN investors i ON i.id = p.investor_id
        ORDER BY p.date DESC
    """)
    return [serialise(r) for r in rows]


@router.post("/principal")
async def add_principal(
    body: dict,
    admin: dict = Depends(require_admin),
    db: Database = Depends(get_db)
):
    from datetime import date as date_type
    # cashflow_type must be 'subscription', 'redemption', or 'transfer'
    cashflow_type = body.get('cashflow_type', body.get('flow_type', 'subscription'))
    # Map legacy values if any
    legacy_map = {'deposit': 'subscription', 'withdrawal': 'redemption'}
    cashflow_type = legacy_map.get(cashflow_type, cashflow_type)

    units  = float(body['units'])
    amount = float(body['amount'])

    await db.execute("""
        INSERT INTO principal_cashflows
        (date, investor_id, cashflow_type, amount, nta_at_date, units, notes)
        VALUES ($1,$2,$3,$4,$5,$6,$7)
    """,
        date_type.fromisoformat(body['date']),
        body['investor_id'],
        cashflow_type,
        amount,
        body.get('nta_at_date'),
        units,
        body.get('notes'),
    )
    # Update investor's running unit total
    sign = 1 if cashflow_type in ('subscription', 'transfer') else -1

    # For redemption: compute realized P&L BEFORE updating units/costs
    realized_pl_delta = 0.0
    cost_basis = 0.0  # AVCO cost reduction for redemption
    if cashflow_type == 'redemption' and body.get('nta_at_date'):
        inv_now = await db.fetchrow(
            "SELECT units, total_costs FROM investors WHERE id=$1", body['investor_id'])
        if inv_now and float(inv_now['units']) > 0:
            current_units = float(inv_now['units'])
            current_costs = float(inv_now['total_costs'])
            avg_cost      = current_costs / current_units if current_units > 0 else 0
            u_redeemed    = abs(units)
            nta_now       = float(body['nta_at_date'])
            redeem_value  = u_redeemed * nta_now
            cost_basis    = u_redeemed * avg_cost
            realized_pl_delta = redeem_value - cost_basis
            # Insert redemption ledger record
            try:
                cf_id_row = await db.fetchval(
                    """SELECT id FROM principal_cashflows
                       WHERE investor_id=$1 ORDER BY created_at DESC LIMIT 1""",
                    body['investor_id'])
                await db.execute("""
                    INSERT INTO redemption_ledger
                    (investor_id, cashflow_id, date, units_redeemed,
                     avg_cost_at_redemption, nta_at_date, redemption_value,
                     cost_basis, realized_pl, notes)
                    VALUES ($1,$2,$3::date,$4,$5,$6,$7,$8,$9,$10)
                """,
                    body['investor_id'], cf_id_row,
                    body['date'], u_redeemed, avg_cost, nta_now,
                    redeem_value, cost_basis, realized_pl_delta,
                    body.get('notes'))
            except Exception:
                pass  # table may not exist yet; non-fatal

    # AVCO: for redemption, reduce total_costs by cost_basis (units × avg_cost)
    # NOT by the redemption value (units × NTA)
    cost_reduction = -cost_basis if cashflow_type == 'redemption' else 0
    await db.execute(
        """UPDATE investors
           SET units       = units + $1,
               total_costs = GREATEST(total_costs + $2, 0),
               realized_pl = realized_pl + $3,
               updated_at  = NOW()
           WHERE id = $4""",
        sign * units,
        cost_reduction,
        realized_pl_delta,
        body['investor_id']
    )
    # Auto-generate daily subscription or redemption statement (per record)
    try:
        from services.pdf_statements import generate_subscription, generate_redemption
        import base64
        inv_full = await db.fetchrow("""
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
            FROM investors i LEFT JOIN users u ON u.investor_id = i.id
            WHERE i.id = $1
        """, body['investor_id'])
        # Get the most recently inserted record + ALL prior cashflows for opening balance
        cf_rec = await db.fetchrow("""
            SELECT * FROM principal_cashflows
            WHERE investor_id=$1 AND date=$2::date
            ORDER BY created_at DESC LIMIT 1
        """, body['investor_id'],
            body['date'] if isinstance(body['date'], str) else str(body['date']))
        # All cashflows BEFORE this record (for computing opening balance)
        prior_cfs = await db.fetch("""
            SELECT * FROM principal_cashflows
            WHERE investor_id=$1 AND created_at < (
                SELECT created_at FROM principal_cashflows
                WHERE investor_id=$1 AND date=$2::date
                ORDER BY created_at DESC LIMIT 1
            )
            ORDER BY date ASC
        """, body['investor_id'],
            body['date'] if isinstance(body['date'], str) else str(body['date']))
        # Redemption ledger for this cashflow
        red_record = None
        if cf_rec and float(cf_rec['amount']) < 0:
            red_record = await db.fetchrow("""
                SELECT * FROM redemption_ledger
                WHERE cashflow_id=$1 ORDER BY created_at DESC LIMIT 1
            """, cf_rec['id'])
        if inv_full and cf_rec:
            is_red = float(cf_rec['amount']) < 0
            cf_dict  = dict(cf_rec)
            cf_dict['prior_cashflows'] = [dict(c) for c in (prior_cfs or [])]
            if red_record:
                cf_dict['realized_pl']     = float(red_record['realized_pl'])
                cf_dict['cost_basis']      = float(red_record['cost_basis'])
                cf_dict['redemption_value']= float(red_record['redemption_value'])
                cf_dict['avg_cost_at_redemption'] = float(red_record['avg_cost_at_redemption'])
            if is_red:
                pdf_b = generate_redemption(dict(inv_full), cf_dict)
                lbl   = 'Redemption'
            else:
                pdf_b = generate_subscription(dict(inv_full), cf_dict)
                lbl   = 'Subscription'
            b64  = base64.b64encode(pdf_b).decode()
            nm   = inv_full['name']
            dt   = str(body.get('date',''))
            fname = f"{lbl}_{nm.replace(' ','_')}_{dt}.pdf"
            title = f"{lbl} Statement — {nm} ({dt})"
            await db.execute("""
                INSERT INTO documents (title,doc_type,file_url,file_name,file_size_kb,
                    visibility,investor_id,uploaded_by)
                VALUES ($1,$2,$3,$4,$5,$6,$7::uuid,$8::uuid)
            """, title,'member_statement',f"data:application/pdf;base64,{b64}",
                fname, len(pdf_b)//1024,'member',str(body['investor_id']),str(admin['id']))
    except Exception:
        pass

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
    rows = await db.fetch("""
        SELECT id, ann_date, ex_date, pmt_date, asset_class,
               instrument, units, dps_sen, amount, entitlement, created_at
        FROM dividends ORDER BY ex_date DESC
    """)
    # Serialise date/UUID fields to str for JSON compatibility
    return [serialise(r) for r in rows]


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
    return [serialise(r) for r in rows]


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
    return [serialise(r) for r in rows]


@router.post("/distributions")
async def declare_distribution(
    body: dict,
    admin: dict = Depends(require_admin),
    db: Database = Depends(get_db)
):
    from datetime import date as date_type
    ex_date = date_type.fromisoformat(body['ex_date'])
    dps     = float(body['dps_sen'])

    # FIX v1.6.0: Compute each investor's units AT the ex-date by summing
    # principal_cashflows up to and including the ex-date — NOT current units.
    # This ensures correct entitlements for historical distributions.
    investor_units = await db.fetch("""
        SELECT
            p.investor_id,
            i.name AS investor_name,
            SUM(p.units) AS units_at_ex_date
        FROM principal_cashflows p
        JOIN investors i ON i.id = p.investor_id
        WHERE p.date <= $1
          AND p.investor_id IS NOT NULL
        GROUP BY p.investor_id, i.name
        HAVING SUM(p.units) > 0.0001
        ORDER BY SUM(p.units) DESC
    """, ex_date)

    if not investor_units:
        from fastapi import HTTPException
        raise HTTPException(
            status_code=400,
            detail="No investor units found at the specified ex-date. "
                   "Ensure principal_cashflows records exist before this date."
        )

    total_units = sum(float(r['units_at_ex_date']) for r in investor_units)
    total_div   = sum(float(r['units_at_ex_date']) * dps / 100 for r in investor_units)

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
        round(total_div, 4),
        body.get('payout_ratio'),
    )

    # Insert per-investor entitlements into distribution_ledger (correct table name)
    for inv in investor_units:
        units  = float(inv['units_at_ex_date'])
        amount = round(units * dps / 100, 4)
        await db.execute("""
            INSERT INTO distribution_ledger
            (distribution_id, investor_id, units_at_ex_date, amount, paid)
            VALUES ($1,$2,$3,$4,FALSE)
            ON CONFLICT (distribution_id, investor_id) DO NOTHING
        """, dist_id, str(inv['investor_id']), units, amount)

    return {
        "message": "Distribution declared",
        "investors_count": len(investor_units),
        "total_dividend": round(total_div, 4)
    }

    # Auto-generate per-investor dividend statement (one per distribution record)
    try:
        from services.pdf_statements import generate_dividend_statement
        import base64
        fy = body.get('financial_year','FY?')
        for inv in investor_units:
            inv_full = await db.fetchrow("""
                SELECT i.*, u.email, u.phone, u.bank_name, u.bank_account_no,
                       u.address_line1, u.address_line2, u.city, u.postcode, u.state, u.country
                FROM investors i LEFT JOIN users u ON u.investor_id=i.id
                WHERE i.id=$1
            """, str(inv['investor_id']))
            if not inv_full: continue
            dl = await db.fetchrow("""
                SELECT dl.units_at_ex_date, dl.amount,
                       d.title, d.dist_type, d.dps_sen, d.ex_date, d.pmt_date,
                       d.financial_year, d.payout_ratio
                FROM distribution_ledger dl
                JOIN distributions d ON d.id=dl.distribution_id
                WHERE dl.investor_id=$1 AND dl.distribution_id=(
                    SELECT id FROM distributions ORDER BY created_at DESC LIMIT 1)
            """, str(inv['investor_id']))
            if not dl: continue
            pdf_b = generate_dividend_statement(
                investor=dict(inv_full),
                dist_record=dict(dl),
                financial_year=fy)
            b64  = base64.b64encode(pdf_b).decode()
            nm   = inv_full['name']
            t_lbl = dl.get('title', fy)
            fname = f"Dividend_{t_lbl.replace(' ','_')}_{nm.replace(' ','_')}.pdf"
            title = f"Dividend Statement — {t_lbl} — {nm}"
            await db.execute("""
                INSERT INTO documents (title,doc_type,file_url,file_name,file_size_kb,
                    visibility,investor_id,financial_year,uploaded_by)
                VALUES ($1,$2,$3,$4,$5,$6,$7::uuid,$8,$9::uuid)
            """, title,'member_statement',f"data:application/pdf;base64,{b64}",
                fname,len(pdf_b)//1024,'member',str(inv['investor_id']),fy,str(admin['id']))
    except Exception:
        pass


@router.get("/distributions/{dist_id}/breakdown")
async def get_distribution_breakdown(
    dist_id: str,
    admin: dict = Depends(require_admin),
    db: Database = Depends(get_db)
):
    from fastapi import HTTPException
    try:
        # Check if pre-computed breakdown exists in distribution_ledger
        rows = await db.fetch("""
            SELECT dl.*, i.name as investor_name
            FROM distribution_ledger dl
            LEFT JOIN investors i ON i.id = dl.investor_id
            WHERE dl.distribution_id = $1
            ORDER BY dl.amount DESC
        """, dist_id)

        if rows:
            return [serialise(r) for r in rows]

        # No pre-computed data — compute on-the-fly from principal_cashflows
        dist = await db.fetchrow("SELECT * FROM distributions WHERE id = $1", dist_id)
        if not dist:
            return []

        ex_date = dist['ex_date']
        dps     = float(dist['dps_sen'])

        # FIX v1.6.0: sum principal_cashflows up to ex_date per investor
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
            HAVING SUM(p.units) > 0.0001
            ORDER BY SUM(p.units) DESC
        """, ex_date)

        if not investor_units:
            raise HTTPException(
                status_code=404,
                detail="No investor units found at ex-date. Check principal_cashflows has investor_id linked."
            )

        result = []
        total_units = sum(float(r['units_at_ex_date']) for r in investor_units)

        for inv in investor_units:
            units  = float(inv['units_at_ex_date'])
            amount = round(units * dps / 100, 4)
            result.append({
                'investor_id':      str(inv['investor_id']),
                'investor_name':    inv['investor_name'] or '—',
                'units_at_ex_date': units,
                'dps_sen':          dps,
                'amount':           amount,
                'paid':             False,
            })
            try:
                await db.execute("""
                    INSERT INTO distribution_ledger
                    (distribution_id, investor_id, units_at_ex_date, amount, paid)
                    VALUES ($1,$2,$3,$4,FALSE)
                    ON CONFLICT (distribution_id, investor_id) DO UPDATE SET
                        units_at_ex_date = EXCLUDED.units_at_ex_date,
                        amount = EXCLUDED.amount
                """, dist_id, str(inv['investor_id']), units, amount)
            except Exception:
                pass  # Don't fail if save fails

        # Update distribution header totals if still zero
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
    """Force recompute breakdown from principal_cashflows at ex-date."""
    await db.execute("DELETE FROM distribution_ledger WHERE distribution_id = $1", dist_id)
    return await get_distribution_breakdown(dist_id, admin, db)


# ── Settlement admin ──────────────────────────────────────────
@router.get("/settlement")
async def get_settlement(
    admin: dict = Depends(require_admin),
    db: Database = Depends(get_db)
):
    rows = await db.fetch("SELECT * FROM settlement ORDER BY date DESC")
    return [serialise(r) for r in rows]


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
    body: UserUpdate,
    admin: dict = Depends(require_admin),
    db: Database = Depends(get_db)
):
    """Update user profile including bank, address, role, and optionally reset password."""
    import bcrypt as _bcrypt
    await db.execute("""
        UPDATE users
        SET name=$1, email=$2, phone=$3, role=$4,
            is_active=$5, investor_id=$6,
            bank_name=$7, bank_account_no=$8,
            address_line1=$9, address_line2=$10,
            city=$11, postcode=$12, state=$13, country=$14,
            updated_at=NOW()
        WHERE id=$15
    """,
        body.name, body.email, body.phone,
        body.role or 'member', body.is_active if body.is_active is not None else True,
        body.investor_id or None,
        body.bank_name, body.bank_account_no,
        body.address_line1, body.address_line2,
        body.city, body.postcode,
        body.state, body.country,
        user_id,
    )
    if body.new_password:
        if len(body.new_password) < 8:
            from fastapi import HTTPException
            raise HTTPException(status_code=400, detail="Password must be at least 8 characters")
        pw_hash = _bcrypt.hashpw(body.new_password.encode(), _bcrypt.gensalt()).decode()
        await db.execute(
            "UPDATE users SET password_hash=$1 WHERE id=$2",
            pw_hash, user_id
        )
    await db.execute(
        "INSERT INTO audit_log (user_id, action, table_name, record_id) VALUES ($1,$2,$3,$4)",
        str(admin["id"]), "UPDATE", "users", user_id
    )
    return {"message": "User updated"}


# ── Admin Document Management ─────────────────────────────────
@router.get("/documents")
async def list_documents(
    doc_type: str = None,
    investor_id: str = None,
    financial_year: str = None,
    visibility: str = None,
    search: str = None,
    page: int = 1,
    limit: int = 50,
    admin: dict = Depends(require_admin),
    db: Database = Depends(get_db)
):
    """List all documents with filters. Admin only."""
    where = ["1=1"]
    params = []
    i = 1
    if doc_type:
        where.append(f"d.doc_type = ${i}"); params.append(doc_type); i+=1
    if investor_id:
        where.append(f"d.investor_id = ${i}::uuid"); params.append(investor_id); i+=1
    if financial_year:
        where.append(f"d.financial_year = ${i}"); params.append(financial_year); i+=1
    if visibility:
        where.append(f"d.visibility = ${i}"); params.append(visibility); i+=1
    if search:
        where.append(f"(LOWER(d.title) LIKE ${i} OR LOWER(COALESCE(inv.name,'')) LIKE ${i+1})")
        params.append(f"%{search.lower()}%")
        params.append(f"%{search.lower()}%"); i+=2

    where_sql = " AND ".join(where)
    offset = (page-1)*limit
    params.extend([limit, offset])

    rows = await db.fetch(f"""
        SELECT d.id, d.title, d.doc_type, d.file_name, d.file_size_kb,
               d.visibility, d.financial_year, d.created_at,
               inv.name AS investor_name,
               u2.name AS uploaded_by_name
        FROM documents d
        LEFT JOIN investors inv ON inv.id = d.investor_id
        LEFT JOIN users u2 ON u2.id = d.uploaded_by
        WHERE {where_sql}
        ORDER BY d.created_at DESC
        LIMIT ${i} OFFSET ${i+1}
    """, *params)

    total = await db.fetchval(f"""
        SELECT COUNT(*) FROM documents d
        LEFT JOIN investors inv ON inv.id = d.investor_id
        WHERE {where_sql}
    """, *params[:-2])

    return {"data": [serialise(r) for r in rows], "total": total, "page": page}


@router.delete("/documents/{doc_id}")
async def delete_document(
    doc_id: str,
    admin: dict = Depends(require_admin),
    db: Database = Depends(get_db)
):
    """Delete a document. Admin only."""
    doc = await db.fetchrow("SELECT id, title FROM documents WHERE id=$1::uuid", doc_id)
    if not doc:
        raise HTTPException(404, "Document not found")
    await db.execute("DELETE FROM documents WHERE id=$1::uuid", doc_id)
    await db.execute(
        "INSERT INTO audit_log (user_id, action, table_name) VALUES ($1,$2,$3)",
        str(admin['id']), f"DELETE doc:{doc['title']}", "documents"
    )
    return {"message": "Document deleted"}


@router.get("/documents/{doc_id}/download")
async def admin_download_document(
    doc_id: str,
    admin: dict = Depends(require_admin),
    db: Database = Depends(get_db)
):
    """Get document file URL for download. Admin only."""
    doc = await db.fetchrow(
        "SELECT file_url, title, file_name FROM documents WHERE id=$1::uuid", doc_id)
    if not doc:
        raise HTTPException(404, "Document not found")
    return {"url": doc["file_url"], "title": doc["title"], "file_name": doc["file_name"]}


# ── Statement Generation ──────────────────────────────────────
@router.post("/distributions/{dist_id}/compute-breakdown")
async def compute_distribution_breakdown(
    dist_id: str,
    admin: dict = Depends(require_admin),
    db: Database = Depends(get_db)
):
    """Force recompute breakdown from principal_cashflows at ex-date."""
    await db.execute("DELETE FROM distribution_ledger WHERE distribution_id = $1", dist_id)
    return await get_distribution_breakdown(dist_id, admin, db)


# ── Settlement admin ──────────────────────────────────────────
@router.get("/settlement")
async def get_settlement(
    admin: dict = Depends(require_admin),
    db: Database = Depends(get_db)
):
    rows = await db.fetch("SELECT * FROM settlement ORDER BY date DESC")
    return [serialise(r) for r in rows]


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
    body: UserUpdate,
    admin: dict = Depends(require_admin),
    db: Database = Depends(get_db)
):
    """Update user profile including bank, address, role, and optionally reset password."""
    import bcrypt as _bcrypt
    await db.execute("""
        UPDATE users
        SET name=$1, email=$2, phone=$3, role=$4,
            is_active=$5, investor_id=$6,
            bank_name=$7, bank_account_no=$8,
            address_line1=$9, address_line2=$10,
            city=$11, postcode=$12, state=$13, country=$14,
            updated_at=NOW()
        WHERE id=$15
    """,
        body.name, body.email, body.phone,
        body.role or 'member', body.is_active if body.is_active is not None else True,
        body.investor_id or None,
        body.bank_name, body.bank_account_no,
        body.address_line1, body.address_line2,
        body.city, body.postcode,
        body.state, body.country,
        user_id,
    )
    if body.new_password:
        if len(body.new_password) < 8:
            from fastapi import HTTPException
            raise HTTPException(status_code=400, detail="Password must be at least 8 characters")
        pw_hash = _bcrypt.hashpw(body.new_password.encode(), _bcrypt.gensalt()).decode()
        await db.execute(
            "UPDATE users SET password_hash=$1 WHERE id=$2",
            pw_hash, user_id
        )
    await db.execute(
        "INSERT INTO audit_log (user_id, action, table_name, record_id) VALUES ($1,$2,$3,$4)",
        str(admin["id"]), "UPDATE", "users", user_id
    )
    return {"message": "User updated"}


# ── Admin Document Management ─────────────────────────────────
@router.get("/documents")
async def list_documents(
    doc_type: str = None,
    investor_id: str = None,
    financial_year: str = None,
    visibility: str = None,
    search: str = None,
    page: int = 1,
    limit: int = 50,
    admin: dict = Depends(require_admin),
    db: Database = Depends(get_db)
):
    """List all documents with filters. Admin only."""
    where = ["1=1"]
    params = []
    i = 1
    if doc_type:
        where.append(f"d.doc_type = ${i}"); params.append(doc_type); i+=1
    if investor_id:
        where.append(f"d.investor_id = ${i}::uuid"); params.append(investor_id); i+=1
    if financial_year:
        where.append(f"d.financial_year = ${i}"); params.append(financial_year); i+=1
    if visibility:
        where.append(f"d.visibility = ${i}"); params.append(visibility); i+=1
    if search:
        where.append(f"(LOWER(d.title) LIKE ${i} OR LOWER(COALESCE(inv.name,'')) LIKE ${i+1})")
        params.append(f"%{search.lower()}%")
        params.append(f"%{search.lower()}%"); i+=2

    where_sql = " AND ".join(where)
    offset = (page-1)*limit
    params.extend([limit, offset])

    rows = await db.fetch(f"""
        SELECT d.id, d.title, d.doc_type, d.file_name, d.file_size_kb,
               d.visibility, d.financial_year, d.created_at,
               inv.name AS investor_name,
               u2.name AS uploaded_by_name
        FROM documents d
        LEFT JOIN investors inv ON inv.id = d.investor_id
        LEFT JOIN users u2 ON u2.id = d.uploaded_by
        WHERE {where_sql}
        ORDER BY d.created_at DESC
        LIMIT ${i} OFFSET ${i+1}
    """, *params)

    total = await db.fetchval(f"""
        SELECT COUNT(*) FROM documents d
        LEFT JOIN investors inv ON inv.id = d.investor_id
        WHERE {where_sql}
    """, *params[:-2])

    return {"data": [serialise(r) for r in rows], "total": total, "page": page}


@router.delete("/documents/{doc_id}")
async def delete_document(
    doc_id: str,
    admin: dict = Depends(require_admin),
    db: Database = Depends(get_db)
):
    """Delete a document. Admin only."""
    doc = await db.fetchrow("SELECT id, title FROM documents WHERE id=$1::uuid", doc_id)
    if not doc:
        raise HTTPException(404, "Document not found")
    await db.execute("DELETE FROM documents WHERE id=$1::uuid", doc_id)
    await db.execute(
        "INSERT INTO audit_log (user_id, action, table_name) VALUES ($1,$2,$3)",
        str(admin['id']), f"DELETE doc:{doc['title']}", "documents"
    )
    return {"message": "Document deleted"}


@router.get("/documents/{doc_id}/download")
async def admin_download_document(
    doc_id: str,
    admin: dict = Depends(require_admin),
    db: Database = Depends(get_db)
):
    """Get document file URL for download. Admin only."""
    doc = await db.fetchrow(
        "SELECT file_url, title, file_name FROM documents WHERE id=$1::uuid", doc_id)
    if not doc:
        raise HTTPException(404, "Document not found")
    return {"url": doc["file_url"], "title": doc["title"], "file_name": doc["file_name"]}


# ── Statement Generation ──────────────────────────────────────
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

    stmt_type   = body.get('statement_type')   # factsheet|subscription|dividend|account
    _inv_id_raw = body.get('investor_id')       # None for factsheet
    fin_year    = body.get('financial_year', '')
    period      = body.get('period', '')

    # Convert investor_id to uuid.UUID so asyncpg binds it correctly
    import uuid as _uuid_mod
    from datetime import date as _date_type
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
            import datetime as _dt

            # Use selected period date (end-of-month date from dropdown)
            # All data filtered UP TO this date
            as_of = selected_date_obj or date_type.today()

            # Holdings and sector as of the as_of date
            # (holdings table is current; for historical we filter by date)
            holdings = await db.fetch("""
                SELECT instrument, total_cost,
                       CASE WHEN units > 0 THEN total_cost / NULLIF((SELECT SUM(total_cost) FROM holdings WHERE units > 0.001),0)*100
                            ELSE 0 END AS weight_pct
                FROM holdings WHERE units > 0.001 ORDER BY total_cost DESC LIMIT 10
            """)
            sector_data = await db.fetch("SELECT * FROM v_holdings_by_class ORDER BY total_market_value DESC")

            # NTA history up to as_of date
            nta_hist = await db.fetch(
                "SELECT date, nta FROM historical WHERE date <= $1 ORDER BY date ASC", as_of)

            # Distributions up to as_of date
            dists = await db.fetch(
                "SELECT * FROM distributions WHERE ex_date <= $1 ORDER BY ex_date DESC LIMIT 8", as_of)

            # Fund data as of the as_of date (NTA at that date)
            latest = await db.fetchrow(
                "SELECT nta, date FROM historical WHERE date <= $1 ORDER BY date DESC LIMIT 1", as_of)
            # Override fund_data with as_of values
            if latest:
                fund_data = dict(fund_data)
                fund_data['current_nta'] = float(latest['nta'])
                # AUM and units at as_of
                aum_row = await db.fetchrow(
                    "SELECT nta * total_units AS aum, total_units FROM historical WHERE date <= $1 ORDER BY date DESC LIMIT 1", as_of)
                if aum_row:
                    fund_data['aum']         = float(aum_row['aum'])
                    fund_data['total_units'] = float(aum_row['total_units'])

            # Period returns relative to as_of date
            periods = {}
            if latest:
                inception = await db.fetchrow("SELECT nta FROM historical ORDER BY date ASC LIMIT 1")
                for label, days in [("1M",30),("3M",90),("6M",180),("1Y",365),("3Y",1095)]:
                    ref_date = as_of - _td(days=days)
                    ref = await db.fetchrow(
                        "SELECT nta FROM historical WHERE date <= $1 ORDER BY date DESC LIMIT 1", ref_date)
                    if ref and ref["nta"]:
                        periods[label] = round((float(latest["nta"]) / float(ref["nta"]) - 1) * 100, 4)
                if inception and inception["nta"]:
                    total_ret = round((float(latest["nta"]) / float(inception["nta"]) - 1) * 100, 4)
                else:
                    total_ret = float(fund_data.get('total_return_pct', 0))
            else:
                total_ret = 0.0

            perf_data = {'period_returns': periods, 'total_return_pct': total_ret}
            manager_comment = body.get('manager_comment', '')
            pdf_bytes = generate_factsheet(
                fund_data=fund_data,
                holdings=[dict(h) for h in holdings],
                performance=perf_data,
                as_of_date=as_of,
                distributions=[dict(d) for d in dists],
                nta_history=[dict(r) for r in nta_hist],
                sector_data=[dict(r) for r in sector_data],
                manager_comment=manager_comment,
            )
            title = f"ZY-Invest Factsheet {period or fund_data.get('last_nta_date','')}"
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
                FROM investors i LEFT JOIN users u ON u.investor_id = i.id
                WHERE i.id = $1
            """, investor_id)
            if not inv: raise HTTPException(404, "Investor not found")
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
                FROM investors i LEFT JOIN users u ON u.investor_id = i.id
                WHERE i.id = $1
            """, investor_id)
            if not inv: raise HTTPException(404, "Investor not found")
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
                FROM investors i LEFT JOIN users u ON u.investor_id = i.id
                WHERE i.id = $1
            """, investor_id)
            if not inv: raise HTTPException(404, "Investor not found")
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
                LEFT JOIN users u ON u.investor_id = i.id
                WHERE i.id = $1
            """, investor_id)
            if not inv:
                raise HTTPException(404, "Investor not found")
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

        # Store in documents table (file_url = base64 data URI for now)
        import base64
        file_b64 = base64.b64encode(pdf_bytes).decode()
        file_url  = f"data:application/pdf;base64,{file_b64}"
        file_name = title.replace(' ','_').replace('—','').replace('/','_') + '.pdf'
        file_size = len(pdf_bytes) // 1024

        doc_id = await db.fetchval("""
            INSERT INTO documents
            (title, doc_type, file_url, file_name, file_size_kb,
             visibility, investor_id, financial_year, uploaded_by)
            VALUES ($1,$2,$3,$4,$5,$6,$7::uuid,$8,$9::uuid)
            RETURNING id
        """,
            title, 'member_statement', file_url, file_name, file_size,
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
            FROM investors i LEFT JOIN users u ON u.investor_id=i.id WHERE i.id=$1
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
