"""
Public router  v1.0.0
Unauthenticated endpoints — used by external (public) pages
GET /api/public/fund-overview
"""
from fastapi import APIRouter, Depends
from database import Database, get_db

router = APIRouter()


@router.get("/fund-overview")
async def fund_overview(db: Database = Depends(get_db)):
    """Public fund snapshot — used on Home page hero section"""
    overview = await db.fetchrow("SELECT * FROM v_fund_overview")
    if not overview:
        return {}

    # Portfolio snapshot (class breakdown — aggregate only)
    by_class = await db.fetch(
        "SELECT asset_class, weight_pct FROM v_holdings_by_class ORDER BY weight_pct DESC"
    )

    return {
        "fund_name":         overview["fund_name"],
        "inception_date":    str(overview["inception_date"]),
        "fund_type":         overview["fund_type"],
        "primary_market":    overview["primary_market"],
        "base_currency":     overview["base_currency"],
        "current_nta":       float(overview["current_nta"]),
        "aum":               float(overview["aum"]),
        "total_return_pct":  float(overview["total_return_pct"]),
        "investor_count":    overview["investor_count"],
        "trading_days":      overview["trading_days"],
        "last_nta_date":     str(overview["last_nta_date"]) if overview["last_nta_date"] else None,
        "portfolio_snapshot": [dict(r) for r in by_class],
    }
