-- ============================================================
-- ZY-Invest Database Schema  v1.0.0
-- PostgreSQL (Supabase)
-- Run this in Supabase SQL Editor in order:
--   01_schema.sql  → tables + indexes
--   02_views.sql   → computed views
--   03_rls.sql     → row-level security
--   04_seed.sql    → initial admin user + fund settings
-- ============================================================

-- Enable UUID generation
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- ============================================================
-- 1. USERS & AUTH
-- ============================================================
CREATE TABLE users (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name            TEXT NOT NULL,
    email           TEXT UNIQUE NOT NULL,
    phone           TEXT,
    password_hash   TEXT NOT NULL,
    role            TEXT NOT NULL DEFAULT 'member' CHECK (role IN ('admin', 'member')),
    investor_id     UUID,                          -- FK set after investors table created
    is_active       BOOLEAN NOT NULL DEFAULT TRUE,
    avatar_url      TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_users_email      ON users(email);
CREATE INDEX idx_users_investor   ON users(investor_id);

-- ============================================================
-- 2. INVESTORS  (fund participants — linked to users)
-- ============================================================
CREATE TABLE investors (
    id                  UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name                TEXT NOT NULL,
    units               NUMERIC(18,6) NOT NULL DEFAULT 0,
    vwap                NUMERIC(18,6),              -- weighted avg purchase price per unit
    total_costs         NUMERIC(18,2) NOT NULL DEFAULT 0,
    current_nta         NUMERIC(18,6),              -- updated daily
    market_value        NUMERIC(18,2),              -- updated daily
    unrealized_pl       NUMERIC(18,2),
    realized_pl         NUMERIC(18,2) NOT NULL DEFAULT 0,
    irr                 NUMERIC(10,6),              -- Newton-Raphson result, updated daily
    joined_date         DATE,
    is_active           BOOLEAN NOT NULL DEFAULT TRUE,
    notes               TEXT,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Add FK from users to investors
ALTER TABLE users
    ADD CONSTRAINT fk_users_investor
    FOREIGN KEY (investor_id) REFERENCES investors(id) ON DELETE SET NULL;

-- ============================================================
-- 3. FUND SETTINGS  (single-row config table)
-- ============================================================
CREATE TABLE fund_settings (
    id                  SMALLINT PRIMARY KEY DEFAULT 1 CHECK (id = 1),  -- enforce single row
    fund_name           TEXT NOT NULL DEFAULT 'ZY-Invest',
    inception_date      DATE NOT NULL DEFAULT '2021-12-13',
    base_currency       TEXT NOT NULL DEFAULT 'MYR',
    fund_type           TEXT NOT NULL DEFAULT 'Private Equity Fund',
    primary_market      TEXT NOT NULL DEFAULT 'Bursa Malaysia',
    financial_year_end  TEXT NOT NULL DEFAULT '11-30',  -- MM-DD
    total_units         NUMERIC(18,6) NOT NULL DEFAULT 0,
    current_nta         NUMERIC(18,6) NOT NULL DEFAULT 1.0,
    aum                 NUMERIC(18,2) NOT NULL DEFAULT 0,
    last_nta_date       DATE,
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ============================================================
-- 4. FEE SCHEDULES  (license-period based)
-- ============================================================
CREATE TABLE fee_schedules (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    fee_type        TEXT NOT NULL CHECK (fee_type IN ('base', 'performance')),
    rate            NUMERIC(10,6) NOT NULL,          -- decimal: 0.01 = 1% p.a.
    basis           TEXT NOT NULL DEFAULT 'daily' CHECK (basis IN ('daily', 'annual')),
    hurdle_rate     NUMERIC(10,6),                   -- for performance fee only (e.g. 0.08 = 8%)
    valid_from      DATE NOT NULL,
    valid_to        DATE,                            -- NULL = open-ended / current
    description     TEXT,
    created_by      UUID REFERENCES users(id),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT no_overlap UNIQUE (fee_type, valid_from)
);

CREATE INDEX idx_fee_schedules_type_date ON fee_schedules(fee_type, valid_from, valid_to);

-- ============================================================
-- 5. TICKER MAPPING  (instrument → Yahoo Finance ticker)
-- ============================================================
CREATE TABLE ticker_map (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    instrument      TEXT NOT NULL UNIQUE,           -- e.g. 'MYEG', 'PPHB'
    yahoo_ticker    TEXT,                           -- e.g. 'MYEG.KL', NULL if manual
    asset_class     TEXT NOT NULL DEFAULT 'Securities [H]',
    region          TEXT NOT NULL DEFAULT 'MY',
    sector          TEXT,
    is_manual       BOOLEAN NOT NULL DEFAULT FALSE, -- TRUE = admin must input price
    last_price      NUMERIC(18,4),
    last_price_date DATE,
    notes           TEXT,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ============================================================
-- 6. TRANSACTIONS  (buy/sell orders)
-- ============================================================
CREATE TABLE transactions (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    date            DATE NOT NULL,
    investor_id     UUID NOT NULL REFERENCES investors(id) ON DELETE CASCADE,
    region          TEXT NOT NULL DEFAULT 'MY',
    asset_class     TEXT NOT NULL,
    sector          TEXT,
    instrument      TEXT NOT NULL,
    units           NUMERIC(18,0) NOT NULL,         -- negative = sell
    price           NUMERIC(18,4) NOT NULL,
    amount          NUMERIC(18,2) NOT NULL,
    total_fees      NUMERIC(18,2) NOT NULL DEFAULT 0,
    net_amount      NUMERIC(18,2) NOT NULL,
    theme           TEXT,
    notes           TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_transactions_investor ON transactions(investor_id);
CREATE INDEX idx_transactions_date     ON transactions(date);
CREATE INDEX idx_transactions_instr    ON transactions(instrument);

-- ============================================================
-- 7. HOLDINGS  (current open positions)
-- ============================================================
CREATE TABLE holdings (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    investor_id     UUID NOT NULL REFERENCES investors(id) ON DELETE CASCADE,
    instrument      TEXT NOT NULL,
    asset_class     TEXT NOT NULL,
    sector          TEXT,
    region          TEXT NOT NULL DEFAULT 'MY',
    units           NUMERIC(18,6) NOT NULL DEFAULT 0,
    vwap            NUMERIC(18,6),
    total_costs     NUMERIC(18,2) NOT NULL DEFAULT 0,
    last_price      NUMERIC(18,4),
    market_value    NUMERIC(18,2),
    unrealized_pl   NUMERIC(18,2),
    return_pct      NUMERIC(10,6),
    mv_portion      NUMERIC(10,6),
    last_trade_date DATE,
    holding_days    INTEGER,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (investor_id, instrument)
);

CREATE INDEX idx_holdings_investor   ON holdings(investor_id);
CREATE INDEX idx_holdings_instrument ON holdings(instrument);

-- ============================================================
-- 8. SETTLEMENT  (closed / realised trades)
-- ============================================================
CREATE TABLE settlement (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    date            DATE NOT NULL,
    investor_id     UUID NOT NULL REFERENCES investors(id) ON DELETE CASCADE,
    region          TEXT NOT NULL DEFAULT 'MY',
    asset_class     TEXT NOT NULL,
    sector          TEXT,
    instrument      TEXT NOT NULL,
    units           NUMERIC(18,0) NOT NULL,
    bought_price    NUMERIC(18,4) NOT NULL,
    sale_price      NUMERIC(18,4) NOT NULL,
    profit_loss     NUMERIC(18,2) NOT NULL,
    return_pct      NUMERIC(10,6),
    remark          TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_settlement_investor ON settlement(investor_id);
CREATE INDEX idx_settlement_date     ON settlement(date);

-- ============================================================
-- 9. DIVIDENDS  (stock dividends received)
-- ============================================================
CREATE TABLE dividends (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    investor_id     UUID NOT NULL REFERENCES investors(id) ON DELETE CASCADE,
    ann_date        DATE,
    ex_date         DATE NOT NULL,
    pmt_date        DATE,
    asset_class     TEXT,
    instrument      TEXT NOT NULL,
    units           NUMERIC(18,0) NOT NULL,
    dps_sen         NUMERIC(18,4) NOT NULL,
    amount          NUMERIC(18,2) NOT NULL,
    entitlement     TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_dividends_investor ON dividends(investor_id);
CREATE INDEX idx_dividends_pmt_date ON dividends(pmt_date);

-- ============================================================
-- 10. DISTRIBUTIONS  (fund distributions to investors)
-- ============================================================
CREATE TABLE distributions (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    ann_date        DATE,
    ex_date         DATE NOT NULL,
    pmt_date        DATE NOT NULL,
    financial_year  TEXT NOT NULL,                  -- e.g. 'FY24'
    title           TEXT NOT NULL,
    dist_type       TEXT NOT NULL DEFAULT 'interim' CHECK (dist_type IN ('interim', 'final', 'special')),
    dps_sen         NUMERIC(18,4) NOT NULL,
    total_units     NUMERIC(18,6),
    total_dividend  NUMERIC(18,2),
    payout_ratio    NUMERIC(10,6),
    notes           TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Per-investor distribution ledger
CREATE TABLE distribution_ledger (
    id                  UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    distribution_id     UUID NOT NULL REFERENCES distributions(id) ON DELETE CASCADE,
    investor_id         UUID NOT NULL REFERENCES investors(id) ON DELETE CASCADE,
    units_at_ex_date    NUMERIC(18,6) NOT NULL,
    amount              NUMERIC(18,2) NOT NULL,
    paid                BOOLEAN NOT NULL DEFAULT FALSE,
    paid_date           DATE,
    UNIQUE (distribution_id, investor_id)
);

CREATE INDEX idx_dist_ledger_investor ON distribution_ledger(investor_id);

-- ============================================================
-- 11. OTHERS  (miscellaneous income: interest, rebates)
-- ============================================================
CREATE TABLE others (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    record_date     DATE NOT NULL,
    title           TEXT NOT NULL,
    income_type     TEXT NOT NULL,                  -- 'Interest', 'Rebate', etc.
    amount          NUMERIC(18,2) NOT NULL,
    platform        TEXT,
    description     TEXT,
    investor_id     UUID REFERENCES investors(id),  -- NULL = fund-level
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ============================================================
-- 12. HISTORICAL NTA  (daily fund balance sheet)
-- ============================================================
CREATE TABLE historical (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    date            DATE NOT NULL UNIQUE,
    -- Assets
    derivatives     NUMERIC(18,2) NOT NULL DEFAULT 0,
    securities      NUMERIC(18,2) NOT NULL DEFAULT 0,
    reits           NUMERIC(18,2) NOT NULL DEFAULT 0,
    bonds           NUMERIC(18,2) NOT NULL DEFAULT 0,
    money_market    NUMERIC(18,2) NOT NULL DEFAULT 0,
    receivables     NUMERIC(18,2) NOT NULL DEFAULT 0,
    cash            NUMERIC(18,2) NOT NULL DEFAULT 0,
    -- Liabilities
    mng_fees        NUMERIC(18,2) NOT NULL DEFAULT 0,
    perf_fees       NUMERIC(18,2) NOT NULL DEFAULT 0,
    ints_on_fees    NUMERIC(18,2) NOT NULL DEFAULT 0,
    loans           NUMERIC(18,2) NOT NULL DEFAULT 0,
    ints_on_loans   NUMERIC(18,2) NOT NULL DEFAULT 0,
    -- Equity
    capital         NUMERIC(18,2) NOT NULL DEFAULT 0,
    earnings        NUMERIC(18,2) NOT NULL DEFAULT 0,
    total_units     NUMERIC(18,6) NOT NULL DEFAULT 0,
    nta             NUMERIC(18,6) NOT NULL,
    is_locked       BOOLEAN NOT NULL DEFAULT FALSE,  -- TRUE = imported from Excel, read-only
    source          TEXT DEFAULT 'system',           -- 'excel_import' | 'system' | 'admin_override'
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_historical_date ON historical(date);

-- ============================================================
-- 13. PRINCIPAL CASHFLOWS  (investor top-ups / redemptions)
-- ============================================================
CREATE TABLE principal_cashflows (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    investor_id     UUID NOT NULL REFERENCES investors(id) ON DELETE CASCADE,
    date            DATE NOT NULL,
    cashflow_type   TEXT NOT NULL CHECK (cashflow_type IN ('subscription', 'redemption', 'transfer')),
    units           NUMERIC(18,6) NOT NULL,
    amount          NUMERIC(18,2) NOT NULL,          -- negative = redemption
    nta_at_date     NUMERIC(18,6),
    notes           TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_principal_investor ON principal_cashflows(investor_id);
CREATE INDEX idx_principal_date     ON principal_cashflows(date);

-- ============================================================
-- 14. DOCUMENTS
-- ============================================================
CREATE TABLE documents (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    title           TEXT NOT NULL,
    doc_type        TEXT NOT NULL CHECK (doc_type IN (
                        'annual_report', 'distribution_notice', 'member_statement',
                        'fund_rules', 'meeting_minutes', 'other'
                    )),
    file_url        TEXT NOT NULL,                  -- Supabase Storage URL
    file_name       TEXT NOT NULL,
    file_size_kb    INTEGER,
    visibility      TEXT NOT NULL DEFAULT 'fund' CHECK (visibility IN ('fund', 'member')),
    investor_id     UUID REFERENCES investors(id),  -- NULL = fund-wide
    financial_year  TEXT,
    uploaded_by     UUID REFERENCES users(id),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_documents_visibility  ON documents(visibility);
CREATE INDEX idx_documents_investor    ON documents(investor_id);

-- ============================================================
-- 15. PRICE HISTORY  (daily prices per instrument)
-- ============================================================
CREATE TABLE price_history (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    instrument      TEXT NOT NULL,
    date            DATE NOT NULL,
    price           NUMERIC(18,4) NOT NULL,
    source          TEXT NOT NULL DEFAULT 'yahoo'   -- 'yahoo' | 'admin_manual'
                        CHECK (source IN ('yahoo', 'admin_manual')),
    UNIQUE (instrument, date)
);

CREATE INDEX idx_price_history_instrument ON price_history(instrument, date DESC);

-- ============================================================
-- 16. AUDIT LOG  (track all admin mutations)
-- ============================================================
CREATE TABLE audit_log (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id         UUID REFERENCES users(id),
    action          TEXT NOT NULL,                  -- 'INSERT', 'UPDATE', 'DELETE'
    table_name      TEXT NOT NULL,
    record_id       TEXT,
    old_values      JSONB,
    new_values      JSONB,
    ip_address      TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_audit_log_user  ON audit_log(user_id);
CREATE INDEX idx_audit_log_table ON audit_log(table_name, created_at DESC);

-- ============================================================
-- updated_at trigger  (auto-update timestamp on any row change)
-- ============================================================
CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_users_updated_at
    BEFORE UPDATE ON users
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

CREATE TRIGGER trg_investors_updated_at
    BEFORE UPDATE ON investors
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

CREATE TRIGGER trg_fund_settings_updated_at
    BEFORE UPDATE ON fund_settings
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

CREATE TRIGGER trg_ticker_map_updated_at
    BEFORE UPDATE ON ticker_map
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

CREATE TRIGGER trg_holdings_updated_at
    BEFORE UPDATE ON holdings
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();
