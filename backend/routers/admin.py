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
