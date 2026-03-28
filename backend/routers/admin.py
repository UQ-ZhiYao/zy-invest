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
        WHERE h.units > 0.001
        ORDER BY COALESCE(h.units * ph.price, h.total_cost) DESC NULLS LAST
    """)
    return [dict(r) for r in rows]


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

    return {
        "message": "Holdings and settlement recomputed",
        "positions": holdings_saved,
        "settlement_records": settlement_count,
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
        body['investor_id'],
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
    investor_units = await db.fetch("""
        SELECT
            p.investor_id,
            i.name as investor_name,
            SUM(CASE
                WHEN LOWER(COALESCE(p.flow_type, p.cashflow_type)) LIKE '%deposit%'
                  OR LOWER(COALESCE(p.flow_type, p.cashflow_type)) LIKE '%paid%'
                  OR LOWER(COALESCE(p.flow_type, p.cashflow_type)) LIKE '%capital%'
                THEN p.units
                ELSE -p.units
            END) as units_at_ex_date
        FROM principal_cashflows p
        LEFT JOIN investors i ON i.id = p.investor_id
        WHERE p.date <= $1
          AND p.investor_id IS NOT NULL
        GROUP BY p.investor_id, i.name
        HAVING SUM(CASE
                WHEN LOWER(COALESCE(p.flow_type, p.cashflow_type)) LIKE '%deposit%'
                  OR LOWER(COALESCE(p.flow_type, p.cashflow_type)) LIKE '%paid%'
                  OR LOWER(COALESCE(p.flow_type, p.cashflow_type)) LIKE '%capital%'
                THEN p.units
                ELSE -p.units
            END) > 0
        ORDER BY units_at_ex_date DESC
    """, ex_date)

    # Save to distribution_investors for future use
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
        # Save to DB
        try:
            await db.execute("""
                INSERT INTO distribution_investors
                (distribution_id, investor_id, units_at_ex_date, dps_sen, amount, paid)
                VALUES ($1,$2,$3,$4,$5,FALSE)
                ON CONFLICT (distribution_id, investor_id) DO UPDATE SET
                    units_at_ex_date = EXCLUDED.units_at_ex_date,
                    amount = EXCLUDED.amount
            """, dist_id, str(inv['investor_id']), units, dps, round(amount, 4))
        except Exception:
            pass

    # Update distribution total
    total_div = sum(r['amount'] for r in result)
    await db.execute("""
        UPDATE distributions
        SET total_units = $1, total_dividend = $2
        WHERE id = $3
    """, total_units, round(total_div, 4), dist_id)

    return result


@router.post("/distributions/{dist_id}/compute-breakdown")
async def compute_distribution_breakdown(
    dist_id: str,
    admin: dict = Depends(require_admin),
    db: Database = Depends(get_db)
):
    """Force recompute breakdown from principal_cashflows."""
    # Delete existing
    await db.execute("DELETE FROM distribution_investors WHERE distribution_id = $1", dist_id)
    # Trigger recompute via GET
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
