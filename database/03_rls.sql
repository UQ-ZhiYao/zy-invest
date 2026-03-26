-- ============================================================
-- ZY-Invest Row-Level Security (RLS)  v1.0.0
-- Run AFTER 01_schema.sql
-- ============================================================
-- HOW IT WORKS:
--   The FastAPI backend connects with TWO Supabase roles:
--   1. service_role  → admin API (bypasses RLS, full access)
--   2. anon/authenticated → member API (RLS enforced)
--
--   The JWT from login contains: { "investor_id": "<uuid>", "role": "member"|"admin" }
--   Supabase sets: current_setting('request.jwt.claims')
-- ============================================================

-- Helper: extract investor_id from JWT
CREATE OR REPLACE FUNCTION auth_investor_id()
RETURNS UUID AS $$
BEGIN
    RETURN (
        current_setting('request.jwt.claims', true)::jsonb ->> 'investor_id'
    )::UUID;
EXCEPTION WHEN OTHERS THEN
    RETURN NULL;
END;
$$ LANGUAGE plpgsql STABLE;

-- Helper: extract role from JWT
CREATE OR REPLACE FUNCTION auth_role()
RETURNS TEXT AS $$
BEGIN
    RETURN current_setting('request.jwt.claims', true)::jsonb ->> 'role';
EXCEPTION WHEN OTHERS THEN
    RETURN NULL;
END;
$$ LANGUAGE plpgsql STABLE;

-- ============================================================
-- Enable RLS on all sensitive tables
-- ============================================================
ALTER TABLE users                ENABLE ROW LEVEL SECURITY;
ALTER TABLE investors            ENABLE ROW LEVEL SECURITY;
ALTER TABLE transactions         ENABLE ROW LEVEL SECURITY;
ALTER TABLE holdings             ENABLE ROW LEVEL SECURITY;
ALTER TABLE settlement           ENABLE ROW LEVEL SECURITY;
ALTER TABLE dividends            ENABLE ROW LEVEL SECURITY;
ALTER TABLE distribution_ledger  ENABLE ROW LEVEL SECURITY;
ALTER TABLE principal_cashflows  ENABLE ROW LEVEL SECURITY;
ALTER TABLE documents            ENABLE ROW LEVEL SECURITY;
ALTER TABLE others               ENABLE ROW LEVEL SECURITY;

-- ============================================================
-- USERS table policies
-- ============================================================
-- Members can only read/update their own user row
CREATE POLICY users_select_own ON users
    FOR SELECT USING (
        id = (SELECT id FROM users WHERE investor_id = auth_investor_id() LIMIT 1)
        OR auth_role() = 'admin'
    );

CREATE POLICY users_update_own ON users
    FOR UPDATE USING (
        id = (SELECT id FROM users WHERE investor_id = auth_investor_id() LIMIT 1)
        OR auth_role() = 'admin'
    );

-- ============================================================
-- INVESTORS table policies
-- ============================================================
CREATE POLICY investors_select ON investors
    FOR SELECT USING (
        id = auth_investor_id()
        OR auth_role() = 'admin'
    );

CREATE POLICY investors_update_admin ON investors
    FOR ALL USING (auth_role() = 'admin');

-- ============================================================
-- TRANSACTIONS — member sees only their own
-- ============================================================
CREATE POLICY transactions_select ON transactions
    FOR SELECT USING (
        investor_id = auth_investor_id()
        OR auth_role() = 'admin'
    );

CREATE POLICY transactions_admin ON transactions
    FOR ALL USING (auth_role() = 'admin');

-- ============================================================
-- HOLDINGS — member sees only their own
-- ============================================================
CREATE POLICY holdings_select ON holdings
    FOR SELECT USING (
        investor_id = auth_investor_id()
        OR auth_role() = 'admin'
    );

CREATE POLICY holdings_admin ON holdings
    FOR ALL USING (auth_role() = 'admin');

-- ============================================================
-- SETTLEMENT — member sees only their own
-- ============================================================
CREATE POLICY settlement_select ON settlement
    FOR SELECT USING (
        investor_id = auth_investor_id()
        OR auth_role() = 'admin'
    );

-- ============================================================
-- DIVIDENDS — member sees only their own
-- ============================================================
CREATE POLICY dividends_select ON dividends
    FOR SELECT USING (
        investor_id = auth_investor_id()
        OR auth_role() = 'admin'
    );

-- ============================================================
-- DISTRIBUTION LEDGER — member sees only their own rows
-- ============================================================
CREATE POLICY dist_ledger_select ON distribution_ledger
    FOR SELECT USING (
        investor_id = auth_investor_id()
        OR auth_role() = 'admin'
    );

-- ============================================================
-- PRINCIPAL CASHFLOWS — member sees only their own
-- ============================================================
CREATE POLICY principal_select ON principal_cashflows
    FOR SELECT USING (
        investor_id = auth_investor_id()
        OR auth_role() = 'admin'
    );

-- ============================================================
-- DOCUMENTS — fund-wide OR tagged to this member only
-- ============================================================
CREATE POLICY documents_select ON documents
    FOR SELECT USING (
        visibility = 'fund'
        OR investor_id = auth_investor_id()
        OR auth_role() = 'admin'
    );

CREATE POLICY documents_admin ON documents
    FOR ALL USING (auth_role() = 'admin');

-- ============================================================
-- OTHERS — fund-level entries visible to all; member-tagged to own
-- ============================================================
CREATE POLICY others_select ON others
    FOR SELECT USING (
        investor_id IS NULL
        OR investor_id = auth_investor_id()
        OR auth_role() = 'admin'
    );

-- ============================================================
-- Public read tables (no RLS needed — safe to be public)
-- ============================================================
-- distributions     → all members see fund distributions
-- historical        → all members see NTA history
-- fund_settings     → all members see fund info
-- fee_schedules     → all members see fee schedule (transparency)
-- ticker_map        → admin only (internal)
-- price_history     → admin only (internal)
