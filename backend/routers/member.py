"""
Member router  v2.0.0  — joint account / nominee support
Data is filtered by investor_id from the JWT.
Primary/secondary holders see the same investor.
Nominees are accessible via nominee_links.
"""
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from typing import Optional
from datetime import date

from database import Database, get_db, serialise
from routers.auth import get_current_user
from services.irr import compute_irr

router = APIRouter()


# ── Core helper ───────────────────────────────────────────────
def investor_id_from_user(user: dict) -> str:
    """
    Returns the investor_id for the current user.
    Works for primary holders (users.investor_id)
    and secondary/nominee holders (investor_holders).
    Both are resolved in get_current_user() and stored in user dict.
    """
    iid = user.get("investor_id")
    if not iid:
        raise HTTPException(403, "No investor profile linked to this account")
    return str(iid)


async def get_accessible_investor_ids(user: dict, db: Database) -> list:
    """
    Returns ALL investor_ids accessible to this user:
    - Their own investor_id
    - Any nominees they can manage (via nominee_links)
    """
    own = investor_id_from_user(user)
    nominees = await db.fetch("""
        SELECT nominee_investor_id FROM nominee_links
        WHERE holder_investor_id = $1
    """, own)
    return [own] + [str(r["nominee_investor_id"]) for r in nominees]


# ── Account Summary ───────────────────────────────────────────
@router.get("/account/summary")
async def account_summary(
    investor_id: Optional[str] = None,
    current_user: dict = Depends(get_current_user),
    db: Database = Depends(get_db)
):
    """
    investor_id param allows viewing a nominee's account.
    If not provided, returns the caller's own account.
    """
    own_id      = investor_id_from_user(current_user)
    accessible  = await get_accessible_investor_ids(current_user, db)

    target_id = investor_id or own_id
    if target_id not in accessible:
        raise HTTPException(403, "Access denied to this investor account")

    investor = await db.fetchrow(
        "SELECT * FROM v_investor_profile WHERE id=$1", target_id)
    if not investor:
        raise HTTPException(404, "Investor profile not found")

    cashflows = await db.fetch(
        "SELECT date, amount, cashflow_type FROM principal_cashflows "
        "WHERE investor_id=$1 ORDER BY date", target_id)
    dist_received = await db.fetch(
        "SELECT dl.paid_date AS date, dl.amount FROM distribution_ledger dl "
        "WHERE dl.investor_id=$1 AND dl.paid=TRUE ORDER BY dl.paid_date", target_id)

    irr = compute_irr(
        principal_cashflows=list(cashflows),
        distributions=list(dist_received),
        current_market_value=float(investor["market_value"] or 0),
        today=date.today()
    )

    # Attach holder info for the response
    holders = await db.fetch("""
        SELECT u.name, u.email, ih.role
        FROM investor_holders ih
        JOIN users u ON u.id = ih.user_id
        WHERE ih.investor_id = $1
        ORDER BY CASE ih.role WHEN 'primary' THEN 0 WHEN 'secondary' THEN 1 ELSE 2 END
    """, target_id)

    return {
        **serialise(investor),
        "irr":     round(irr * 100, 4) if irr is not None else None,
        "holders": [{"name": h["name"], "email": h["email"],
                     "role": h["role"]} for h in holders],
        "is_nominee_view": target_id != own_id,
    }


# ── Nominee list ──────────────────────────────────────────────
@router.get("/account/nominees")
async def my_nominees(
    current_user: dict = Depends(get_current_user),
    db: Database = Depends(get_db)
):
    """Returns all nominee accounts this user can manage."""
    own_id = investor_id_from_user(current_user)
    rows = await db.fetch("""
        SELECT i.id, i.name, i.account_type, i.units, i.is_active,
               nl.created_at AS linked_at
        FROM nominee_links nl
        JOIN investors i ON i.id = nl.nominee_investor_id
        WHERE nl.holder_investor_id = $1
        ORDER BY i.name
    """, own_id)
    return [serialise(r) for r in rows]


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
           FROM users WHERE id=$1""",
        str(current_user["id"])
    )
    if not user:
        raise HTTPException(404, "User not found")
    return serialise(dict(user))


class ProfileUpdate(BaseModel):
    name: Optional[str]            = None
    email: Optional[str]           = None
    phone: Optional[str]           = None
    bank_name: Optional[str]       = None
    bank_account_no: Optional[str] = None
    address_line1: Optional[str]   = None
    address_line2: Optional[str]   = None
    city: Optional[str]            = None
    postcode: Optional[str]        = None
    state: Optional[str]           = None
    country: Optional[str]         = None


@router.put("/account/profile")
async def update_profile(
    body: ProfileUpdate,
    current_user: dict = Depends(get_current_user),
    db: Database = Depends(get_db)
):
    fields, vals, idx = [], [], 1
    for field, val in body.dict(exclude_none=True).items():
        if field == "email": val = val.lower().strip()
        fields.append(f"{field}=${idx}"); vals.append(val); idx += 1
    if not fields:
        return {"message": "Nothing to update"}
    vals.append(str(current_user["id"]))
    await db.execute(
        f"UPDATE users SET {','.join(fields)},updated_at=NOW() WHERE id=${idx}", *vals)
    return {"message": "Profile updated"}


# ── Distributions ─────────────────────────────────────────────
@router.get("/account/distributions")
async def my_distributions(
    investor_id: Optional[str] = None,
    current_user: dict = Depends(get_current_user),
    db: Database = Depends(get_db)
):
    accessible = await get_accessible_investor_ids(current_user, db)
    target_id  = investor_id or investor_id_from_user(current_user)
    if target_id not in accessible:
        raise HTTPException(403, "Access denied")
    rows = await db.fetch(
        "SELECT * FROM v_distribution_breakdown WHERE investor_id=$1 ORDER BY pmt_date DESC",
        target_id)
    return [serialise(r) for r in rows]


# ── Transactions ──────────────────────────────────────────────
@router.get("/account/transactions")
async def my_transactions(
    investor_id: Optional[str] = None,
    current_user: dict = Depends(get_current_user),
    db: Database = Depends(get_db),
    page: int = 1,
    limit: int = 50
):
    accessible = await get_accessible_investor_ids(current_user, db)
    target_id  = investor_id or investor_id_from_user(current_user)
    if target_id not in accessible:
        raise HTTPException(403, "Access denied")
    offset = (page - 1) * limit
    rows = await db.fetch(
        "SELECT date,asset_class,sector,instrument,units,price,net_amount,theme "
        "FROM transactions WHERE investor_id=$1 ORDER BY date DESC LIMIT $2 OFFSET $3",
        target_id, limit, offset)
    total = await db.fetchval(
        "SELECT COUNT(*) FROM transactions WHERE investor_id=$1", target_id)
    return {"data": [serialise(r) for r in rows], "total": total, "page": page}


# ── Fund Performance (fund-wide, no filtering) ────────────────
@router.get("/fund/performance")
async def fund_performance(db: Database = Depends(get_db),
                           _: dict = Depends(get_current_user)):
    nta_history = await db.fetch(
        "SELECT date,nta,daily_return_pct,cumulative_return_pct "
        "FROM v_historical_nta ORDER BY date")
    latest = await db.fetchrow(
        "SELECT nta,date FROM historical ORDER BY date DESC LIMIT 1")
    periods = {}
    if latest:
        import datetime as _dt
        for label, days in [("1W",7),("1M",30),("3M",90),("6M",180),("1Y",365),("YTD",None)]:
            if label == "YTD":
                ref_date = date(latest["date"].year, 1, 1)
            else:
                ref_date = latest["date"] - _dt.timedelta(days=days)
            ref = await db.fetchrow(
                "SELECT nta FROM historical WHERE date<=$1 ORDER BY date DESC LIMIT 1", ref_date)
            if ref and ref["nta"]:
                periods[label] = round(
                    (float(latest["nta"]) / float(ref["nta"]) - 1) * 100, 4)
    overview = await db.fetchrow("SELECT * FROM v_fund_overview")
    return {
        "nta_history":      [dict(r) for r in nta_history],
        "period_returns":   periods,
        "total_return_pct": float(overview["total_return_pct"]) if overview else None,
        "inception_date":   str(overview["inception_date"]) if overview else None,
        "trading_days":     overview["trading_days"] if overview else None,
    }


@router.get("/fund/statement")
async def fund_statement(db: Database = Depends(get_db),
                         _: dict = Depends(get_current_user)):
    rows = await db.fetch("SELECT * FROM v_fund_statement ORDER BY financial_year")
    return [serialise(r) for r in rows]


@router.get("/fund/analysis")
async def fund_analysis(db: Database = Depends(get_db),
                        _: dict = Depends(get_current_user)):
    by_class  = await db.fetch("SELECT * FROM v_holdings_by_class")
    by_sector = await db.fetch("SELECT * FROM v_holdings_by_sector")
    by_region = await db.fetch("SELECT * FROM v_holdings_by_region")
    overview  = await db.fetchrow("SELECT * FROM v_fund_overview")
    import datetime as _dt
    principal_qtr = await db.fetch("""
        SELECT TO_CHAR(DATE_TRUNC('quarter',date),'YYYY "Q"Q') AS quarter,
               DATE_TRUNC('quarter',date) AS quarter_date,
               SUM(CASE WHEN cashflow_type='subscription' THEN amount ELSE 0 END) AS inflow,
               SUM(CASE WHEN cashflow_type='redemption'   THEN ABS(amount) ELSE 0 END) AS outflow,
               SUM(amount) AS net
        FROM principal_cashflows
        GROUP BY DATE_TRUNC('quarter',date) ORDER BY 2""")
    investment_qtr = await db.fetch("""
        SELECT TO_CHAR(DATE_TRUNC('quarter',date),'YYYY "Q"Q') AS quarter,
               DATE_TRUNC('quarter',date) AS quarter_date,
               SUM(CASE WHEN units>0 THEN ABS(net_amount) ELSE 0 END) AS deployed,
               SUM(CASE WHEN units<0 THEN ABS(net_amount) ELSE 0 END) AS proceeds,
               SUM(CASE WHEN units>0 THEN -ABS(net_amount)
                        WHEN units<0 THEN  ABS(net_amount) ELSE 0 END) AS net_deployed
        FROM transactions
        GROUP BY DATE_TRUNC('quarter',date) ORDER BY 2""")
    return {
        "by_class":             [serialise(r) for r in by_class],
        "by_sector":            [serialise(r) for r in by_sector],
        "by_region":            [serialise(r) for r in by_region],
        "aum":                  float(overview["aum"])         if overview else None,
        "nta":                  float(overview["current_nta"]) if overview else None,
        "principal_quarterly":  [serialise(r) for r in principal_qtr],
        "investment_quarterly": [serialise(r) for r in investment_qtr],
    }


# ── Documents ─────────────────────────────────────────────────
@router.get("/fund/documents")
async def my_documents(
    investor_id: Optional[str] = None,
    current_user: dict = Depends(get_current_user),
    db: Database = Depends(get_db)
):
    accessible = await get_accessible_investor_ids(current_user, db)
    target_id  = investor_id or investor_id_from_user(current_user)
    if target_id not in accessible:
        raise HTTPException(403, "Access denied")
    rows = await db.fetch("""
        SELECT id,title,doc_type,file_name,file_size_kb,visibility,financial_year,created_at
        FROM documents
        WHERE visibility='fund' OR investor_id=$1
        ORDER BY created_at DESC
    """, target_id)
    return [serialise(r) for r in rows]


@router.get("/fund/documents/{doc_id}/download")
async def download_document(
    doc_id: str,
    current_user: dict = Depends(get_current_user),
    db: Database = Depends(get_db)
):
    accessible = await get_accessible_investor_ids(current_user, db)
    doc = await db.fetchrow(
        "SELECT file_url,visibility,investor_id,title FROM documents WHERE id=$1", doc_id)
    if not doc:
        raise HTTPException(404, "Document not found")
    if doc["visibility"] == "member" and str(doc["investor_id"]) not in accessible:
        raise HTTPException(403, "Access denied")
    return {"url": doc["file_url"], "title": doc["title"]}
