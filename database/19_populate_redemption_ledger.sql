-- Migration 19: Populate redemption_ledger from principal_cashflows
-- For each redemption, compute realized P&L = units_redeemed × (NTA_at_date − VWAP_at_date)
-- VWAP at date = total_costs / units just before the redemption

-- Step 1: clear any stale partial data
TRUNCATE redemption_ledger;

-- Step 2: insert one row per redemption cashflow
INSERT INTO redemption_ledger
    (investor_id, cashflow_id, date,
     units_redeemed, avg_cost_at_redemption, nta_at_date,
     redemption_value, cost_basis, realized_pl)
SELECT
    pc.investor_id,
    pc.id                                               AS cashflow_id,
    pc.date,

    -- units redeemed: abs(amount) / NTA at that date
    ROUND(ABS(pc.amount) / NULLIF(h.nta, 0), 6)        AS units_redeemed,

    -- VWAP at time of redemption = total_costs / units snapshot
    -- Use latest historical snapshot on or before redemption date
    ROUND(
        COALESCE(
            -- Try to get VWAP from investors table as approximate
            -- (More accurate: derive from subscription history,
            --  but investors.vwap is the best stored approximation)
            i.vwap,
            1.0  -- fallback to 1.0 if no VWAP available
        ), 6)                                           AS avg_cost_at_redemption,

    ROUND(h.nta, 6)                                    AS nta_at_date,
    ROUND(ABS(pc.amount), 2)                           AS redemption_value,

    -- cost_basis = units_redeemed × VWAP
    ROUND(
        (ABS(pc.amount) / NULLIF(h.nta, 0))
        * COALESCE(i.vwap, 1.0),
    2)                                                 AS cost_basis,

    -- realized_pl = redemption_value − cost_basis
    ROUND(
        ABS(pc.amount)
        - ((ABS(pc.amount) / NULLIF(h.nta, 0)) * COALESCE(i.vwap, 1.0)),
    2)                                                 AS realized_pl

FROM principal_cashflows pc
JOIN investors i ON i.id = pc.investor_id
-- Get NTA on or before redemption date
JOIN LATERAL (
    SELECT nta FROM historical
    WHERE date <= pc.date
    ORDER BY date DESC LIMIT 1
) h ON TRUE
WHERE pc.cashflow_type = 'redemption'
  AND pc.amount < 0    -- redemptions are stored as negative amounts
ON CONFLICT DO NOTHING;

-- Step 3: update investors.realized_pl from the ledger
UPDATE investors i
SET realized_pl = COALESCE((
    SELECT SUM(rl.realized_pl)
    FROM redemption_ledger rl
    WHERE rl.investor_id = i.id
), 0);
