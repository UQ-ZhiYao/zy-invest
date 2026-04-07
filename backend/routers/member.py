"""
Member router v2.1 — individual & joint account support
All member endpoints filtered by investor_id from JWT.
Nominees are per-holder contact records (no investor link).
"""
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from typing import Optional
from datetime import date

from database import Database, get_db, serialise
from routers.auth import get_current_user
from services.irr import compute_irr

router = APIRouter()


def investor_id_from_user(user: dict) -> str:
    iid = user.get("investor_id")
    if not iid:
        raise HTTPException(
            403, "No investor profile linked to this account")
    return str(iid)


# ── Account Summary ───────────────────────────────────────────
@router.get("/account/summary")
async def account_summary(
    current_user: dict = Depends(get_current_user),
    db: Database = Depends(get_db)
):
    inv_id   = investor_id_from_user(current_user)
    investor = await db.fetchrow(
        "SELECT * FROM v_investor_profile WHERE id=$1", inv_id)
    if not investor:
        raise HTTPException(404, "Investor profile not found")

    # ── Market value = units × latest NTA (always fresh) ──────────
    latest_nta_row = await db.fetchrow(
        "SELECT nta FROM historical ORDER BY date DESC LIMIT 1")
    latest_nta        = float(latest_nta_row["nta"]) if latest_nta_row else 0.0
    units_held        = float(investor["units"] or 0)
    total_costs       = float(investor["total_costs"] or 0)
    live_market_value = round(units_held * latest_nta, 2)
    live_unrealized_pl = round(live_market_value - total_costs, 2)

    # ── Realised P&L from redemption_ledger ─────────────────────
    rl_row = await db.fetchrow(
        "SELECT COALESCE(SUM(realized_pl), 0) AS total "
        "FROM redemption_ledger WHERE investor_id=$1", inv_id)
    realised_pl = round(float(rl_row["total"] or 0), 2)

    # ── Dividends Received = distributions paid TO this member ──
    # Join distributions to get pmt_date as fallback when paid_date is NULL
    dist_received = await db.fetch("""
        SELECT
            COALESCE(dl.paid_date, d.pmt_date) AS date,
            dl.amount
        FROM distribution_ledger dl
        JOIN distributions d ON d.id = dl.distribution_id
        WHERE dl.investor_id = $1
        ORDER BY COALESCE(dl.paid_date, d.pmt_date)
    """, inv_id)
    dividends_received = round(
        sum(float(r["amount"] or 0) for r in dist_received), 2)

    # ── Total P&L: unrealised + realised + dividends received ───
    total_pl = round(live_unrealized_pl + realised_pl + dividends_received, 2)

    # ── Simple return = (MV + Realised + Dividends) / Cost - 1 ──
    if total_costs > 0:
        simple_return = round(
            (live_market_value + realised_pl + dividends_received)
            / total_costs * 100 - 100, 4)
    else:
        simple_return = 0.0

    # ── IRR via Newton-Raphson ───────────────────────────────────
    # Subscriptions → negative, Redemptions → positive,
    # Distributions → positive, Terminal MV → positive
    cashflows = await db.fetch(
        "SELECT date, amount, cashflow_type FROM principal_cashflows "
        "WHERE investor_id=$1 ORDER BY date", inv_id)

    irr = compute_irr(
        principal_cashflows=list(cashflows),
        distributions=list(dist_received),
        current_market_value=live_market_value,
        today=date.today())

    # ── Co-holders for joint accounts ───────────────────────────
    holders = await db.fetch("""
        SELECT u.name, u.email, ih.role, ih.share_ratio
        FROM investor_holders ih
        JOIN users u ON u.id = ih.user_id
        WHERE ih.investor_id = $1
        ORDER BY CASE ih.role WHEN 'primary' THEN 0 ELSE 1 END
    """, inv_id)

    return {
        **serialise(investor),
        "market_value":       live_market_value,
        "unrealized_pl":      live_unrealized_pl,
        "realized_pl":        realised_pl,
        "dividends_received": dividends_received,
        "total_pl":           total_pl,
        "simple_return_pct":  simple_return,
        "fund_nta":           latest_nta,
        "irr":                round(irr * 100, 4) if irr is not None else None,
        "holders":            [serialise(h) for h in holders],
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
           FROM users WHERE id=$1""",
        str(current_user["id"]))
    if not user:
        raise HTTPException(404, "User not found")
    return serialise(dict(user))


class ProfileUpdate(BaseModel):
    name:            Optional[str] = None
    email:           Optional[str] = None
    phone:           Optional[str] = None
    bank_name:       Optional[str] = None
    bank_account_no: Optional[str] = None
    address_line1:   Optional[str] = None
    address_line2:   Optional[str] = None
    city:            Optional[str] = None
    postcode:        Optional[str] = None
    state:           Optional[str] = None
    country:         Optional[str] = None


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
        f"UPDATE users SET {','.join(fields)},updated_at=NOW() WHERE id=${idx}",
        *vals)
    return {"message": "Profile updated"}


# ── Nominees (per holder — plain contacts, no investor link) ──
@router.get("/account/nominees")
async def get_nominees(
    current_user: dict = Depends(get_current_user),
    db: Database = Depends(get_db)
):
    rows = await db.fetch(
        "SELECT * FROM nominees WHERE holder_user_id=$1 ORDER BY name",
        str(current_user["id"]))
    return [serialise(r) for r in rows]


class NomineeCreate(BaseModel):
    name:         str
    phone:        Optional[str] = None
    email:        Optional[str] = None
    address_line1: Optional[str] = None
    address_line2: Optional[str] = None
    city:         Optional[str] = None
    postcode:     Optional[str] = None
    state:        Optional[str] = None
    country:      Optional[str] = "Malaysia"
    relationship: Optional[str] = None


@router.post("/account/nominees")
async def add_nominee(
    body: NomineeCreate,
    current_user: dict = Depends(get_current_user),
    db: Database = Depends(get_db)
):
    await db.execute("""
        INSERT INTO nominees
            (holder_user_id, name, phone, email,
             address_line1, address_line2, city, postcode, state, country,
             relationship)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
    """, str(current_user["id"]),
        body.name, body.phone, body.email,
        body.address_line1, body.address_line2,
        body.city, body.postcode, body.state, body.country,
        body.relationship)
    return {"message": "Nominee added"}


@router.put("/account/nominees/{nominee_id}")
async def update_nominee(
    nominee_id: str,
    body: NomineeCreate,
    current_user: dict = Depends(get_current_user),
    db: Database = Depends(get_db)
):
    row = await db.fetchrow(
        "SELECT id FROM nominees WHERE id=$1::uuid AND holder_user_id=$2",
        nominee_id, str(current_user["id"]))
    if not row:
        raise HTTPException(404, "Nominee not found")
    await db.execute("""
        UPDATE nominees SET
            name=$1, phone=$2, email=$3,
            address_line1=$4, address_line2=$5,
            city=$6, postcode=$7, state=$8, country=$9,
            relationship=$10, updated_at=NOW()
        WHERE id=$11::uuid AND holder_user_id=$12
    """, body.name, body.phone, body.email,
        body.address_line1, body.address_line2,
        body.city, body.postcode, body.state, body.country,
        body.relationship, nominee_id, str(current_user["id"]))
    return {"message": "Nominee updated"}


@router.delete("/account/nominees/{nominee_id}")
async def delete_nominee(
    nominee_id: str,
    current_user: dict = Depends(get_current_user),
    db: Database = Depends(get_db)
):
    await db.execute(
        "DELETE FROM nominees WHERE id=$1::uuid AND holder_user_id=$2",
        nominee_id, str(current_user["id"]))
    return {"message": "Nominee deleted"}


# ── Distributions ─────────────────────────────────────────────
@router.get("/account/distributions")
async def my_distributions(
    current_user: dict = Depends(get_current_user),
    db: Database = Depends(get_db)
):
    inv_id = investor_id_from_user(current_user)
    rows = await db.fetch(
        "SELECT * FROM v_distribution_breakdown "
        "WHERE investor_id=$1 ORDER BY pmt_date DESC", inv_id)
    return [serialise(r) for r in rows]


# ── Transactions ──────────────────────────────────────────────
@router.get("/account/transactions")
async def my_transactions(
    current_user: dict = Depends(get_current_user),
    db: Database = Depends(get_db),
    page: int = 1, limit: int = 50
):
    inv_id = investor_id_from_user(current_user)
    offset = (page - 1) * limit
    rows = await db.fetch(
        "SELECT date,asset_class,sector,instrument,units,price,net_amount,theme "
        "FROM transactions WHERE investor_id=$1 "
        "ORDER BY date DESC LIMIT $2 OFFSET $3",
        inv_id, limit, offset)
    total = await db.fetchval(
        "SELECT COUNT(*) FROM transactions WHERE investor_id=$1", inv_id)
    return {"data": [serialise(r) for r in rows], "total": total, "page": page}


# ── Fund Performance ──────────────────────────────────────────
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
                "SELECT nta FROM historical WHERE date<=$1 "
                "ORDER BY date DESC LIMIT 1", ref_date)
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
    """
    Returns ALL financial statement FYs from the pre-computed cache.
    Zero computation — pure DB read.
    Members see all FYs including current (as last computed by admin).
    If cache is empty, returns empty list — admin must run Recompute first.
    """
    import json
    rows = await db.fetch("""
        SELECT fy, fy_year, is_current, data, computed_at
        FROM financial_statements
        ORDER BY fy_year ASC
    """)
    if not rows:
        return []
    results = []
    for row in rows:
        d = row['data'] if isinstance(row['data'], dict) else json.loads(row['data'])
        d['computed_at'] = str(row['computed_at'])
        results.append(d)
    return results


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
        FROM transactions GROUP BY DATE_TRUNC('quarter',date) ORDER BY 2""")
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
    current_user: dict = Depends(get_current_user),
    db: Database = Depends(get_db)
):
    inv_id = investor_id_from_user(current_user)
    rows = await db.fetch("""
        SELECT id,title,doc_type,file_name,file_size_kb,
               visibility,financial_year,created_at
        FROM documents
        WHERE visibility='fund' OR investor_id=$1
        ORDER BY created_at DESC
    """, inv_id)
    return [serialise(r) for r in rows]


@router.get("/fund/documents/{doc_id}/download")
async def download_document(
    doc_id: str,
    current_user: dict = Depends(get_current_user),
    db: Database = Depends(get_db)
):
    inv_id = investor_id_from_user(current_user)
    doc = await db.fetchrow(
        "SELECT file_url,visibility,investor_id,title FROM documents WHERE id=$1",
        doc_id)
    if not doc:
        raise HTTPException(404, "Document not found")
    if (doc["visibility"] == "member"
            and str(doc["investor_id"]) != inv_id):
        raise HTTPException(403, "Access denied")
    return {"url": doc["file_url"], "title": doc["title"]}
