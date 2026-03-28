"""
Member router  v1.0.0
All endpoints require valid JWT with role=member or admin.
Data is always filtered by investor_id from the JWT.

GET /api/member/account/summary
GET /api/member/account/profile
PUT /api/member/account/profile
GET /api/member/account/distributions
GET /api/member/account/transactions
GET /api/member/fund/performance
GET /api/member/fund/statement
GET /api/member/fund/analysis
GET /api/member/fund/documents
"""
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from typing import Optional
from datetime import date

from database import Database, get_db
from routers.auth import get_current_user
from services.irr import compute_irr

router = APIRouter()


def investor_id_from_user(user: dict) -> str:
    iid = user.get("investor_id")
    if not iid:
        raise HTTPException(status_code=403, detail="No investor profile linked to this account")
    return str(iid)


# ── Account Summary ───────────────────────────────────────────
@router.get("/account/summary")
async def account_summary(
    current_user: dict = Depends(get_current_user),
    db: Database = Depends(get_db)
):
    inv_id = investor_id_from_user(current_user)

    # Core investor metrics
    investor = await db.fetchrow(
        "SELECT * FROM v_investor_profile WHERE id = $1", inv_id
    )
    if not investor:
        raise HTTPException(status_code=404, detail="Investor profile not found")

    # Compute fresh IRR via Newton-Raphson
    cashflows = await db.fetch(
        """
        SELECT date, amount, cashflow_type FROM principal_cashflows
        WHERE investor_id = $1 ORDER BY date
        """, inv_id
    )
    dist_received = await db.fetch(
        """
        SELECT dl.paid_date AS date, dl.amount
        FROM distribution_ledger dl
        WHERE dl.investor_id = $1 AND dl.paid = TRUE
        ORDER BY dl.paid_date
        """, inv_id
    )

    irr = compute_irr(
        principal_cashflows=list(cashflows),
        distributions=list(dist_received),
        current_market_value=float(investor["market_value"] or 0),
        today=date.today()
    )

    return {
        **dict(investor),
        "irr": round(irr * 100, 4) if irr is not None else None,
    }


# ── Profile ───────────────────────────────────────────────────
@router.get("/account/profile")
async def get_profile(
    current_user: dict = Depends(get_current_user),
    db: Database = Depends(get_db)
):
    user = await db.fetchrow(
        """SELECT id, name, email, phone,
                  bank_name, bank_account_no,
                  address_line1, address_line2, city, postcode, state, country,
                  created_at
           FROM users WHERE id = $1""",
        str(current_user["id"])
    )
    return dict(user)


class ProfileUpdate(BaseModel):
    name: Optional[str] = None
    email: Optional[str] = None
    phone: Optional[str] = None
    bank_name: Optional[str] = None
    bank_account_no: Optional[str] = None
    address_line1: Optional[str] = None
    address_line2: Optional[str] = None
    city: Optional[str] = None
    postcode: Optional[str] = None
    state: Optional[str] = None
    country: Optional[str] = None


@router.put("/account/profile")
async def update_profile(
    body: ProfileUpdate,
    current_user: dict = Depends(get_current_user),
    db: Database = Depends(get_db)
):
    fields, vals, idx = [], [], 1
    for field, val in body.model_dump(exclude_none=True).items():
        if field == "email":
            val = val.lower().strip()
        fields.append(f"{field} = ${idx}")
        vals.append(val)
        idx += 1

    if not fields:
        return {"message": "Nothing to update"}

    vals.append(str(current_user["id"]))
    await db.execute(
        f"UPDATE users SET {', '.join(fields)}, updated_at = NOW() WHERE id = ${idx}",
        *vals
    )
    return {"message": "Profile updated"}


# ── My Distributions ─────────────────────────────────────────
@router.get("/account/distributions")
async def my_distributions(
    current_user: dict = Depends(get_current_user),
    db: Database = Depends(get_db)
):
    inv_id = investor_id_from_user(current_user)
    rows = await db.fetch(
        "SELECT * FROM v_distribution_breakdown WHERE investor_id = $1 ORDER BY pmt_date DESC",
        inv_id
    )
    return [dict(r) for r in rows]


# ── My Transactions ───────────────────────────────────────────
@router.get("/account/transactions")
async def my_transactions(
    current_user: dict = Depends(get_current_user),
    db: Database = Depends(get_db),
    page: int = 1,
    limit: int = 50
):
    inv_id = investor_id_from_user(current_user)
    offset = (page - 1) * limit
    rows = await db.fetch(
        """
        SELECT date, asset_class, sector, instrument, units, price,
               net_amount, theme
        FROM transactions
        WHERE investor_id = $1
        ORDER BY date DESC
        LIMIT $2 OFFSET $3
        """,
        inv_id, limit, offset
    )
    total = await db.fetchval(
        "SELECT COUNT(*) FROM transactions WHERE investor_id = $1", inv_id
    )
    return {"data": [dict(r) for r in rows], "total": total, "page": page}


# ── Fund Performance ─────────────────────────────────────────
@router.get("/fund/performance")
async def fund_performance(
    db: Database = Depends(get_db),
    _: dict = Depends(get_current_user)
):
    # NTA history
    nta_history = await db.fetch(
        """
        SELECT date, nta, daily_return_pct, cumulative_return_pct
        FROM v_historical_nta
        ORDER BY date
        """
    )
    # Period returns
    latest = await db.fetchrow(
        "SELECT nta, date FROM historical ORDER BY date DESC LIMIT 1"
    )
    periods = {}
    if latest:
        for label, days in [("1W",7),("1M",30),("3M",90),("6M",180),("1Y",365),("YTD",None)]:
            if label == "YTD":
                start_of_year = date(latest["date"].year, 1, 1)
                ref = await db.fetchrow(
                    "SELECT nta FROM historical WHERE date <= $1 ORDER BY date DESC LIMIT 1",
                    start_of_year
                )
            else:
                ref_date = latest["date"] - __import__("datetime").timedelta(days=days)
                ref = await db.fetchrow(
                    "SELECT nta FROM historical WHERE date <= $1 ORDER BY date DESC LIMIT 1",
                    ref_date
                )
            if ref and ref["nta"]:
                periods[label] = round(
                    (float(latest["nta"]) / float(ref["nta"]) - 1) * 100, 4
                )
    # Fund overview for since-inception
    overview = await db.fetchrow("SELECT * FROM v_fund_overview")

    return {
        "nta_history": [dict(r) for r in nta_history],
        "period_returns": periods,
        "total_return_pct": float(overview["total_return_pct"]) if overview else None,
        "inception_date": str(overview["inception_date"]) if overview else None,
        "trading_days": overview["trading_days"] if overview else None,
    }


# ── Corporate Results (Fund P&L) ──────────────────────────────
@router.get("/fund/statement")
async def fund_statement(
    db: Database = Depends(get_db),
    _: dict = Depends(get_current_user)
):
    rows = await db.fetch("SELECT * FROM v_fund_statement ORDER BY financial_year")
    return [dict(r) for r in rows]


# ── Data Analysis (aggregate only — no stock names) ───────────
@router.get("/fund/analysis")
async def fund_analysis(
    db: Database = Depends(get_db),
    _: dict = Depends(get_current_user)
):
    by_class  = await db.fetch("SELECT * FROM v_holdings_by_class")
    by_sector = await db.fetch("SELECT * FROM v_holdings_by_sector")
    by_region = await db.fetch("SELECT * FROM v_holdings_by_region")
    cashflow  = await db.fetch("SELECT * FROM v_cashflow_18m ORDER BY month")
    overview  = await db.fetchrow("SELECT * FROM v_fund_overview")

    return {
        "by_class":  [dict(r) for r in by_class],
        "by_sector": [dict(r) for r in by_sector],
        "by_region": [dict(r) for r in by_region],
        "cashflow_18m": [dict(r) for r in cashflow],
        "aum":    float(overview["aum"])    if overview else None,
        "nta":    float(overview["current_nta"]) if overview else None,
    }


# ── Documents ─────────────────────────────────────────────────
@router.get("/fund/documents")
async def my_documents(
    current_user: dict = Depends(get_current_user),
    db: Database = Depends(get_db)
):
    inv_id = investor_id_from_user(current_user)
    rows = await db.fetch(
        """
        SELECT id, title, doc_type, file_name, file_size_kb,
               visibility, financial_year, created_at
        FROM documents
        WHERE visibility = 'fund'
           OR investor_id = $1
        ORDER BY created_at DESC
        """,
        inv_id
    )
    return [dict(r) for r in rows]


# ── Document download (signed URL via Supabase Storage) ───────
@router.get("/fund/documents/{doc_id}/download")
async def download_document(
    doc_id: str,
    current_user: dict = Depends(get_current_user),
    db: Database = Depends(get_db)
):
    inv_id = investor_id_from_user(current_user)
    doc = await db.fetchrow(
        """
        SELECT file_url, visibility, investor_id, title
        FROM documents WHERE id = $1
        """,
        doc_id
    )
    if not doc:
        raise HTTPException(status_code=404, detail="Document not found")
    if doc["visibility"] == "member" and str(doc["investor_id"]) != inv_id:
        raise HTTPException(status_code=403, detail="Access denied")

    return {"url": doc["file_url"], "title": doc["title"]}
