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
from fastapi import APIRouter, Depends, HTTPException, UploadFile, File
from pydantic import BaseModel
from typing import Optional, List
from datetime import date
import io

from database import Database, get_db
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
    admin: dict = Depends(require_admin),
    db: Database = Depends(get_db),
    target_date: Optional[date] = None
):
    result = await compute_daily_nta(db, target_date)
    if not result:
        raise HTTPException(status_code=400, detail="Could not compute NTA — check price data")
    return result


# ── Excel Upload ──────────────────────────────────────────────
@router.post("/upload/excel")
async def upload_excel(
    file: UploadFile = File(...),
    admin: dict = Depends(require_admin),
    db: Database = Depends(get_db)
):
    if not file.filename.endswith((".xlsx", ".xlsm")):
        raise HTTPException(status_code=400, detail="Only .xlsx or .xlsm files accepted")
    content = await file.read()
    # Import parser runs synchronously in executor
    import asyncio
    from services.excel_parser import parse_and_import
    loop = asyncio.get_event_loop()
    result = await loop.run_in_executor(None, parse_and_import, content, db)
    return result


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
        """SELECT t.*, i.name as investor_name
           FROM transactions t
           LEFT JOIN investors i ON i.id = t.investor_id
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
    """Compute current holdings from all buy/sell transactions using VWAP."""
    rows = await db.fetch("""
        SELECT instrument, asset_class, sector, region,
               SUM(units) as net_units,
               SUM(CASE WHEN units > 0 THEN units ELSE 0 END) as total_bought,
               SUM(CASE WHEN units > 0 THEN units * price ELSE 0 END) as total_cost
        FROM transactions
        WHERE investor_id IS NULL OR TRUE
        GROUP BY instrument, asset_class, sector, region
        HAVING SUM(units) > 0.001
        ORDER BY instrument
    """)
    holdings = []
    for r in rows:
        avg_cost = float(r['total_cost']) / float(r['total_bought']) if r['total_bought'] else 0
        # Get latest price from price_history
        price_row = await db.fetchrow(
            "SELECT price FROM price_history WHERE instrument=$1 ORDER BY date DESC LIMIT 1",
            r['instrument']
        )
        last_price = float(price_row['price']) if price_row else None
        holdings.append({
            'instrument': r['instrument'],
            'asset_class': r['asset_class'],
            'sector': r['sector'],
            'region': r['region'],
            'units': float(r['net_units']),
            'avg_cost': round(avg_cost, 6),
            'last_price': last_price,
        })
    return holdings


@router.post("/holdings/compute")
async def compute_holdings_and_settlement(
    admin: dict = Depends(require_admin),
    db: Database = Depends(get_db)
):
    """Recompute all holdings and auto-generate settlement records for sells."""
    # Get all transactions ordered by date
    rows = await db.fetch("""
        SELECT * FROM transactions ORDER BY date ASC, created_at ASC
    """)

    # Build holdings with VWAP, detect sells → generate settlement
    positions = {}  # instrument -> {units, total_cost, avg_cost}
    settlement_count = 0

    for row in rows:
        instr = row['instrument']
        units = float(row['units'])
        price = float(row['price'])

        if instr not in positions:
            positions[instr] = {'units': 0, 'total_cost': 0, 'avg_cost': 0}

        pos = positions[instr]

        if units > 0:
            # BUY — update VWAP
            new_total_cost = pos['total_cost'] + (units * price)
            new_units      = pos['units'] + units
            pos['avg_cost']    = new_total_cost / new_units if new_units > 0 else price
            pos['total_cost']  = new_total_cost
            pos['units']       = new_units
        else:
            # SELL — compute realised P&L using VWAP
            sell_units = abs(units)
            if pos['units'] > 0:
                bought_price = pos['avg_cost']
                pl = (price - bought_price) * sell_units
                ret_pct = ((price - bought_price) / bought_price * 100) if bought_price > 0 else 0
                # Insert settlement record
                try:
                    await db.execute("""
                        INSERT INTO settlement
                        (date, investor_id, region, asset_class, sector, instrument,
                         units, bought_price, sale_price, profit_loss, return_pct, remark)
                        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
                        ON CONFLICT DO NOTHING
                    """,
                        row['date'],
                        row.get('investor_id'),
                        row.get('region', 'MY'),
                        row.get('asset_class', ''),
                        row.get('sector'),
                        instr,
                        sell_units,
                        round(bought_price, 6),
                        price,
                        round(pl, 4),
                        round(ret_pct, 4),
                        'auto-computed VWAP'
                    )
                    settlement_count += 1
                except Exception:
                    pass
                # Deduct from position
                pos['units']      -= sell_units
                pos['total_cost']  = pos['units'] * pos['avg_cost']

    return {
        "message": "Holdings and settlement recomputed",
        "positions": len([p for p in positions.values() if p['units'] > 0.001]),
        "settlement_records": settlement_count
    }


# ── Principal cashflow ────────────────────────────────────────
@router.get("/principal")
async def get_principal(
    admin: dict = Depends(require_admin),
    db: Database = Depends(get_db)
):
    rows = await db.fetch("""
        SELECT p.*, i.name as investor_name
        FROM principal_cashflow p
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
        INSERT INTO principal_cashflow
        (date, investor_id, flow_type, amount, nta_at_date, units, notes)
        VALUES ($1,$2,$3,$4,$5,$6,$7)
    """,
        date_type.fromisoformat(body['date']),
        body['investor_id'],
        body['flow_type'],
        body['amount'],
        body['nta_at_date'],
        body['units'],
        body.get('notes'),
    )
    # Update investor units
    sign = 1 if body['flow_type'] == 'deposit' else -1
    await db.execute("""
        UPDATE investors SET units = units + $1 WHERE id = $2
    """, sign * body['units'], body['investor_id'])
    return {"message": "Principal cashflow recorded"}


# ── NTA at date ───────────────────────────────────────────────
@router.get("/nta/at-date")
async def get_nta_at_date(
    date: str,
    admin: dict = Depends(require_admin),
    db: Database = Depends(get_db)
):
    from datetime import date as date_type
    row = await db.fetchrow("""
        SELECT nta, date FROM historical
        WHERE date <= $1
        ORDER BY date DESC LIMIT 1
    """, date_type.fromisoformat(date))
    if row:
        return {"nta": float(row['nta']), "date": str(row['date'])}
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
    rows = await db.fetch("""
        SELECT di.*, i.name as investor_name
        FROM distribution_investors di
        LEFT JOIN investors i ON i.id = di.investor_id
        WHERE di.distribution_id = $1
        ORDER BY di.amount DESC
    """, dist_id)
    return [dict(r) for r in rows]


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
