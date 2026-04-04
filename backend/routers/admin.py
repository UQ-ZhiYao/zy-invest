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
from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from pydantic import BaseModel
from typing import Optional, List
from datetime import date
import io

from database import Database, get_db, serialise
from routers.auth import require_admin
from services.price_fetcher import run_daily_price_fetch, update_manual_price
from services.nta_engine import compute_daily_nta, compute_from_dirty

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
    background_tasks: BackgroundTasks,
    body: dict = None,
    admin: dict = Depends(require_admin),
    db: Database = Depends(get_db),
):
    """
    Find earliest dirty date and compute forward to today.
    Pass force_from (YYYY-MM-DD) to override start date.
    Runs in background — returns immediately.
    """
    from datetime import date as date_t
    body = body or {}
    force_from = None
    if body.get("force_from"):
        try:
            force_from = date_t.fromisoformat(str(body["force_from"]))
        except ValueError:
            raise HTTPException(400, "Invalid force_from date")

    try:
        dirty = await db.fetchrow(
            "SELECT MIN(date) AS d FROM compute_status WHERE is_dirty = TRUE")
        from_label = str(dirty["d"]) if dirty and dirty["d"] else "next uncomputed"
    except Exception:
        last = await db.fetchrow("SELECT MAX(date) AS d FROM historical")
        from_label = str(last["d"]) if last and last["d"] else "beginning"

    background_tasks.add_task(compute_from_dirty, db, force_from)
    return {
        "message": "Computation started in background",
        "from":    str(force_from) if force_from else from_label,
        "to":      str(date_t.today()),
    }


@router.get("/nta/dirty-status")
async def get_dirty_status(
    admin: dict = Depends(require_admin),
    db: Database = Depends(get_db),
):
    """Return which dates are dirty and need recomputation."""
    try:
        count = await db.fetchval(
            "SELECT COUNT(*) FROM compute_status WHERE is_dirty = TRUE") or 0
        sample = await db.fetch("""
            SELECT date, reason FROM compute_status
            WHERE is_dirty = TRUE ORDER BY date ASC LIMIT 10
        """)
        return {
            "dirty_count":   int(count),
            "sample_dates":  [serialise(r) for r in sample],
            "needs_compute": count > 0,
        }
    except Exception:
        return {"dirty_count": 0, "sample_dates": [], "needs_compute": False,
                "note": "Run migration 13_compute_status.sql first"}


