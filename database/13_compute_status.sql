-- ============================================================
-- compute_status: tracks which dates need recomputation
-- A date is "dirty" when any input table changes for that date.
-- ============================================================
CREATE TABLE IF NOT EXISTS compute_status (
    date        DATE PRIMARY KEY,
    is_dirty    BOOLEAN NOT NULL DEFAULT TRUE,
    reason      TEXT,                          -- which table triggered it
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ── Helper: mark a date dirty ────────────────────────────────
CREATE OR REPLACE FUNCTION mark_dirty(p_date DATE, p_reason TEXT)
RETURNS void LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO compute_status (date, is_dirty, reason, updated_at)
    VALUES (p_date, TRUE, p_reason, NOW())
    ON CONFLICT (date) DO UPDATE
        SET is_dirty   = TRUE,
            reason     = EXCLUDED.reason,
            updated_at = NOW()
    WHERE compute_status.is_dirty = FALSE;  -- only update if currently clean
END;
$$;

-- ── Triggers on every input table ────────────────────────────

-- transactions
CREATE OR REPLACE FUNCTION trg_transactions_dirty() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    PERFORM mark_dirty(COALESCE(NEW.date, OLD.date), 'transactions');
    RETURN COALESCE(NEW, OLD);
END;$$;
DROP TRIGGER IF EXISTS trg_transactions ON transactions;
CREATE TRIGGER trg_transactions
    AFTER INSERT OR UPDATE OR DELETE ON transactions
    FOR EACH ROW EXECUTE FUNCTION trg_transactions_dirty();

-- principal_cashflows
CREATE OR REPLACE FUNCTION trg_principal_dirty() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    PERFORM mark_dirty(COALESCE(NEW.date, OLD.date), 'principal_cashflows');
    RETURN COALESCE(NEW, OLD);
END;$$;
DROP TRIGGER IF EXISTS trg_principal ON principal_cashflows;
CREATE TRIGGER trg_principal
    AFTER INSERT OR UPDATE OR DELETE ON principal_cashflows
    FOR EACH ROW EXECUTE FUNCTION trg_principal_dirty();

-- dividends
CREATE OR REPLACE FUNCTION trg_dividends_dirty() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    PERFORM mark_dirty(COALESCE(NEW.ex_date, OLD.ex_date), 'dividends');
    IF COALESCE(NEW.pmt_date, OLD.pmt_date) IS NOT NULL THEN
        PERFORM mark_dirty(COALESCE(NEW.pmt_date, OLD.pmt_date), 'dividends');
    END IF;
    RETURN COALESCE(NEW, OLD);
END;$$;
DROP TRIGGER IF EXISTS trg_dividends ON dividends;
CREATE TRIGGER trg_dividends
    AFTER INSERT OR UPDATE OR DELETE ON dividends
    FOR EACH ROW EXECUTE FUNCTION trg_dividends_dirty();

-- others
CREATE OR REPLACE FUNCTION trg_others_dirty() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    PERFORM mark_dirty(COALESCE(NEW.record_date, OLD.record_date), 'others');
    RETURN COALESCE(NEW, OLD);
END;$$;
DROP TRIGGER IF EXISTS trg_others ON others;
CREATE TRIGGER trg_others
    AFTER INSERT OR UPDATE OR DELETE ON others
    FOR EACH ROW EXECUTE FUNCTION trg_others_dirty();

-- fee_withdrawals
CREATE OR REPLACE FUNCTION trg_fee_withdrawals_dirty() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    PERFORM mark_dirty(COALESCE(NEW.date, OLD.date), 'fee_withdrawals');
    RETURN COALESCE(NEW, OLD);
END;$$;
DROP TRIGGER IF EXISTS trg_fee_withdrawals ON fee_withdrawals;
CREATE TRIGGER trg_fee_withdrawals
    AFTER INSERT OR UPDATE OR DELETE ON fee_withdrawals
    FOR EACH ROW EXECUTE FUNCTION trg_fee_withdrawals_dirty();

-- price_history (yahooquery updates)
CREATE OR REPLACE FUNCTION trg_price_history_dirty() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    PERFORM mark_dirty(COALESCE(NEW.date, OLD.date), 'price_history');
    RETURN COALESCE(NEW, OLD);
END;$$;
DROP TRIGGER IF EXISTS trg_price_history ON price_history;
CREATE TRIGGER trg_price_history
    AFTER INSERT OR UPDATE OR DELETE ON price_history
    FOR EACH ROW EXECUTE FUNCTION trg_price_history_dirty();

-- ── Seed: mark all existing unlocked historical rows as clean ─
-- (they came from Excel and are already computed)
INSERT INTO compute_status (date, is_dirty, reason)
    SELECT date, FALSE, 'seeded_from_excel'
    FROM historical
    WHERE is_locked = TRUE
ON CONFLICT (date) DO UPDATE SET is_dirty = FALSE;
