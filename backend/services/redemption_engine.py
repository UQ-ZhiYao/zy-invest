"""
Redemption Engine  v1.0.0
Computes realized P&L per redemption using AVCO (Average Cost) method.

AVCO rules:
  - Subscription: units += new_units; cost += amount; VWAP recalculates
  - Redemption:   cost_basis = units_redeemed × VWAP_before
                  realized_pl = redemption_value − cost_basis
                  Remaining cost = op_cost − cost_basis
                  VWAP is UNCHANGED after redemption (AVCO property)

Cashflow sign convention (principal_cashflows):
  amount > 0  → subscription
  amount < 0  → redemption
"""
import logging
from datetime import date
from typing import Optional

logger = logging.getLogger(__name__)


async def compute_avco_for_investor(db, investor_id: str) -> list:
    """
    Replay all principal cashflows for one investor in chronological order.
    Returns list of dicts, one per redemption, with full AVCO breakdown.
    Only computes redemption rows — subscriptions are used to build VWAP state.
    """
    cashflows = await db.fetch("""
        SELECT id, date, cashflow_type, units, amount, nta_at_date
        FROM principal_cashflows
        WHERE investor_id = $1
        ORDER BY date ASC, created_at ASC
    """, investor_id)

    results   = []
    op_units  = 0.0
    op_cost   = 0.0

    for cf in cashflows:
        amount  = float(cf["amount"])
        units   = float(cf["units"])
        nta_raw = cf["nta_at_date"]

        if amount > 0:
            # ── Subscription ───────────────────────────────────
            op_units += abs(units)
            op_cost  += abs(amount)

        elif amount < 0:
            # ── Redemption ─────────────────────────────────────
            # VWAP just before this redemption
            op_avg = (op_cost / op_units) if op_units > 0 else 0.0

            # Get NTA at redemption date
            if nta_raw:
                nta = float(nta_raw)
            else:
                nta_row = await db.fetchrow(
                    "SELECT nta FROM historical WHERE date <= $1 "
                    "ORDER BY date DESC LIMIT 1", cf["date"])
                nta = float(nta_row["nta"]) if nta_row else op_avg

            if nta <= 0:
                logger.warning(f"No NTA for redemption {cf['id']} on {cf['date']}, skipping")
                continue

            # Units redeemed derived from amount ÷ NTA
            units_r       = abs(amount) / nta
            redeem_value  = abs(amount)
            cost_basis    = round(units_r * op_avg, 2)
            realized_pl   = round(redeem_value - cost_basis, 2)

            results.append({
                "cashflow_id":            str(cf["id"]),
                "investor_id":            investor_id,
                "date":                   cf["date"],
                "units_redeemed":         round(units_r, 6),
                "avg_cost_at_redemption": round(op_avg, 6),
                "nta_at_date":            round(nta, 6),
                "redemption_value":       round(redeem_value, 2),
                "cost_basis":             cost_basis,
                "realized_pl":            realized_pl,
            })

            # Update running position (AVCO: cost reduces, VWAP unchanged)
            op_units = max(0.0, op_units - units_r)
            op_cost  = max(0.0, op_cost  - cost_basis)

    return results


async def upsert_redemption_ledger(db, entries: list):
    """Insert or update redemption_ledger rows for the given entries."""
    for e in entries:
        await db.execute("""
            INSERT INTO redemption_ledger
                (investor_id, cashflow_id, date,
                 units_redeemed, avg_cost_at_redemption, nta_at_date,
                 redemption_value, cost_basis, realized_pl)
            VALUES
                ($1::uuid, $2::uuid, $3,
                 $4, $5, $6, $7, $8, $9)
            ON CONFLICT (cashflow_id) DO UPDATE SET
                units_redeemed         = EXCLUDED.units_redeemed,
                avg_cost_at_redemption = EXCLUDED.avg_cost_at_redemption,
                nta_at_date            = EXCLUDED.nta_at_date,
                redemption_value       = EXCLUDED.redemption_value,
                cost_basis             = EXCLUDED.cost_basis,
                realized_pl            = EXCLUDED.realized_pl
        """,
            e["investor_id"], e["cashflow_id"], e["date"],
            e["units_redeemed"], e["avg_cost_at_redemption"],
            e["nta_at_date"], e["redemption_value"],
            e["cost_basis"], e["realized_pl"],
        )


async def refresh_investor_realized_pl(db, investor_id: str):
    """Recalculate and store investors.realized_pl from redemption_ledger."""
    await db.execute("""
        UPDATE investors SET realized_pl = (
            SELECT COALESCE(SUM(realized_pl), 0)
            FROM redemption_ledger WHERE investor_id = $1::uuid
        ) WHERE id = $1::uuid
    """, investor_id)


async def process_single_redemption(db, cashflow_id: str, investor_id: str):
    """
    Called by API after a new redemption is inserted.
    Recomputes the FULL AVCO sequence for this investor so the
    new entry is consistent with all prior redemptions.
    """
    entries = await compute_avco_for_investor(db, investor_id)
    await upsert_redemption_ledger(db, entries)
    await refresh_investor_realized_pl(db, investor_id)
    logger.info(f"Redemption ledger updated for investor {investor_id}: "
                f"{len(entries)} redemption(s)")
    return entries


async def recompute_all_redemptions(db) -> dict:
    """
    Full recompute for ALL investors.
    Clears and rebuilds redemption_ledger completely.
    Used by the admin manual recompute endpoint.
    """
    # Get all investors who have at least one redemption
    investors = await db.fetch("""
        SELECT DISTINCT investor_id FROM principal_cashflows
        WHERE cashflow_type = 'redemption'
    """)

    total_entries   = 0
    total_investors = 0

    # Clear ledger first for a clean rebuild
    await db.execute("TRUNCATE redemption_ledger")

    for row in investors:
        inv_id  = str(row["investor_id"])
        entries = await compute_avco_for_investor(db, inv_id)
        if entries:
            await upsert_redemption_ledger(db, entries)
            await refresh_investor_realized_pl(db, inv_id)
            total_entries   += len(entries)
            total_investors += 1

    # Zero out realized_pl for investors with no redemptions
    await db.execute("""
        UPDATE investors SET realized_pl = 0
        WHERE id NOT IN (SELECT DISTINCT investor_id FROM redemption_ledger)
          AND realized_pl IS DISTINCT FROM 0
    """)

    logger.info(f"Full recompute done: {total_entries} entries, "
                f"{total_investors} investors")
    return {
        "investors_processed": total_investors,
        "redemption_entries":  total_entries,
    }
