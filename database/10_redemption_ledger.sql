-- ── Redemption Ledger — records realized P&L per redemption ───────
-- Run in Supabase SQL Editor
CREATE TABLE IF NOT EXISTS redemption_ledger (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    investor_id     UUID NOT NULL REFERENCES investors(id) ON DELETE CASCADE,
    cashflow_id     UUID REFERENCES principal_cashflows(id) ON DELETE CASCADE,
    date            DATE NOT NULL,
    units_redeemed  NUMERIC(18,6) NOT NULL,
    avg_cost_at_redemption NUMERIC(18,6) NOT NULL, -- VWAP before redemption
    nta_at_date     NUMERIC(18,6) NOT NULL,         -- price at redemption
    redemption_value NUMERIC(18,2) NOT NULL,        -- units × NTA
    cost_basis      NUMERIC(18,2) NOT NULL,         -- units × avg_cost
    realized_pl     NUMERIC(18,2) NOT NULL,         -- redemption_value − cost_basis
    notes           TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_redemption_investor ON redemption_ledger(investor_id);
CREATE INDEX IF NOT EXISTS idx_redemption_date     ON redemption_ledger(date);

COMMENT ON TABLE redemption_ledger IS
  'Per-redemption realized P&L log. realized_pl = units × (NTA − avg_cost_at_redemption)';
