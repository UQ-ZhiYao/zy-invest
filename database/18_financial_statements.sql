-- Migration 18: Pre-computed financial statements cache
-- Stores IS/BS/CF/Ratio data per FY as JSONB.
-- Past FYs are frozen; current FY is refreshed on demand or after NTA compute.

CREATE TABLE IF NOT EXISTS financial_statements (
    id          UUID        PRIMARY KEY DEFAULT uuid_generate_v4(),
    fy          TEXT        NOT NULL,   -- e.g. 'FY22'
    fy_year     INTEGER     NOT NULL,   -- e.g. 2022
    is_current  BOOLEAN     NOT NULL DEFAULT FALSE,  -- TRUE = incomplete current FY
    data        JSONB       NOT NULL,   -- full result dict
    computed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (fy)
);

CREATE INDEX IF NOT EXISTS idx_fin_stmt_fy_year ON financial_statements(fy_year);
