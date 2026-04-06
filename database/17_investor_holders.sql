-- Migration 17 (v2 — clean rebuild)
-- Individual & Joint accounts, holders with share_ratio, nominees per holder

-- ── 1. Drop wrong tables from v1 if they exist ────────────────
DROP TABLE IF EXISTS nominee_links    CASCADE;
DROP TABLE IF EXISTS investor_holders CASCADE;

-- ── 2. Fix account_type constraint (remove 'nominee') ─────────
ALTER TABLE investors
    DROP CONSTRAINT IF EXISTS investors_account_type_check;

ALTER TABLE investors
    ADD COLUMN IF NOT EXISTS account_type TEXT NOT NULL DEFAULT 'individual';

ALTER TABLE investors
    ADD CONSTRAINT investors_account_type_check
    CHECK (account_type IN ('individual', 'joint'));

-- ── 3. investor_holders ───────────────────────────────────────
-- Who owns this investment account, and what share (informational only).
CREATE TABLE IF NOT EXISTS investor_holders (
    id           UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    investor_id  UUID NOT NULL REFERENCES investors(id) ON DELETE CASCADE,
    user_id      UUID NOT NULL REFERENCES users(id)     ON DELETE CASCADE,
    role         TEXT NOT NULL DEFAULT 'primary'
                 CHECK (role IN ('primary', 'secondary')),
    share_ratio  NUMERIC(5,2) NOT NULL DEFAULT 100.00
                 CHECK (share_ratio > 0 AND share_ratio <= 100),
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (investor_id, user_id)
);

CREATE INDEX IF NOT EXISTS idx_holders_investor ON investor_holders(investor_id);
CREATE INDEX IF NOT EXISTS idx_holders_user     ON investor_holders(user_id);

-- Backfill: existing users.investor_id → primary holder, 100%
INSERT INTO investor_holders (investor_id, user_id, role, share_ratio)
SELECT investor_id, id, 'primary', 100.00
FROM   users
WHERE  investor_id IS NOT NULL
ON CONFLICT (investor_id, user_id) DO NOTHING;

-- ── 4. nominees ───────────────────────────────────────────────
-- Each holder (user) can designate their own nominees.
-- Nominees are plain contacts — no login, no investor account.
CREATE TABLE IF NOT EXISTS nominees (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    holder_user_id  UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    name            TEXT NOT NULL,
    phone           TEXT,
    email           TEXT,
    address_line1   TEXT,
    address_line2   TEXT,
    city            TEXT,
    postcode        TEXT,
    state           TEXT,
    country         TEXT NOT NULL DEFAULT 'Malaysia',
    relationship    TEXT,          -- e.g. Spouse, Child, Sibling, Parent
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_nominees_holder ON nominees(holder_user_id);

-- ── 5. Update v_investor_profile to include all investors + account_type ──
-- The old view filtered WHERE is_active=TRUE which hid all investors
-- We keep the original view for member-facing use but add a separate admin-facing query
-- (admin.py now uses direct SQL, not this view)
-- DROP then recreate (CREATE OR REPLACE can't change column types)
DROP VIEW IF EXISTS v_investor_profile CASCADE;

CREATE VIEW v_investor_profile AS
SELECT
    i.id,
    i.name,
    i.units,
    i.vwap,
    i.total_costs,
    i.current_nta,
    i.market_value,
    i.unrealized_pl,
    i.realized_pl,
    i.unrealized_pl + i.realized_pl                          AS total_pl,
    ROUND((i.market_value - i.total_costs) /
          NULLIF(i.total_costs, 0) * 100, 4)                 AS simple_return_pct,
    i.irr                                                    AS irr_pct,
    i.joined_date,
    i.account_type,
    i.is_active,
    fs.current_nta                                           AS fund_nta,
    ROUND(i.units / NULLIF(fs.total_units, 0) * 100, 4)     AS fund_ownership_pct
FROM investors i
CROSS JOIN fund_settings fs;
-- Note: removed WHERE is_active = TRUE so all investors are visible
