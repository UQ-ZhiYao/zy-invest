-- Migration 20: Auto-compute redemption_ledger via PostgreSQL trigger
-- Uses AVCO (Average Cost) method.
-- Run AFTER migration 19 (which creates the table).

-- ── 1. Add UNIQUE on cashflow_id for safe upserts ────────────
ALTER TABLE redemption_ledger
    DROP CONSTRAINT IF EXISTS uq_redemption_cashflow;
ALTER TABLE redemption_ledger
    ADD CONSTRAINT uq_redemption_cashflow UNIQUE (cashflow_id);


-- ── 2. Trigger function (AVCO engine) ─────────────────────────
CREATE OR REPLACE FUNCTION fn_compute_redemption_ledger()
RETURNS TRIGGER AS $$
DECLARE
    v_nta           NUMERIC(18,6);
    v_units_r       NUMERIC(18,6);
    v_redeem_value  NUMERIC(18,2);
    v_op_units      NUMERIC(18,6) := 0;
    v_op_cost       NUMERIC(18,2) := 0;
    v_op_avg        NUMERIC(18,6) := 0;
    v_cost_basis    NUMERIC(18,2);
    v_realized_pl   NUMERIC(18,2);
    cf              RECORD;
BEGIN
    -- Only process redemptions
    IF NEW.cashflow_type != 'redemption' THEN
        RETURN NEW;
    END IF;

    -- NTA on or before redemption date
    SELECT nta INTO v_nta
    FROM historical
    WHERE date <= NEW.date
    ORDER BY date DESC LIMIT 1;

    IF v_nta IS NULL OR v_nta = 0 THEN
        RETURN NEW; -- cannot compute without NTA
    END IF;

    -- Replay all prior cashflows for this investor (AVCO)
    FOR cf IN
        SELECT cashflow_type, units, amount
        FROM principal_cashflows
        WHERE investor_id = NEW.investor_id
          AND (date < NEW.date
               OR (date = NEW.date AND created_at < NEW.created_at))
        ORDER BY date ASC, created_at ASC
    LOOP
        IF cf.amount > 0 THEN
            v_op_units := v_op_units + ABS(cf.units);
            v_op_cost  := v_op_cost  + ABS(cf.amount);
        ELSIF cf.amount < 0 AND v_op_units > 0 THEN
            v_op_avg   := v_op_cost / v_op_units;
            v_op_units := GREATEST(0, v_op_units - ABS(cf.units));
            v_op_cost  := GREATEST(0, v_op_cost  - ABS(cf.units) * v_op_avg);
        END IF;
    END LOOP;

    -- VWAP just before this redemption
    v_op_avg      := CASE WHEN v_op_units > 0 THEN v_op_cost / v_op_units ELSE v_nta END;
    v_units_r     := ROUND(ABS(NEW.amount) / v_nta, 6);
    v_redeem_value := ABS(NEW.amount);
    v_cost_basis  := ROUND(v_units_r * v_op_avg, 2);
    v_realized_pl := ROUND(v_redeem_value - v_cost_basis, 2);

    -- Upsert
    INSERT INTO redemption_ledger
        (investor_id, cashflow_id, date,
         units_redeemed, avg_cost_at_redemption, nta_at_date,
         redemption_value, cost_basis, realized_pl)
    VALUES
        (NEW.investor_id, NEW.id, NEW.date,
         v_units_r, ROUND(v_op_avg,6), ROUND(v_nta,6),
         v_redeem_value, v_cost_basis, v_realized_pl)
    ON CONFLICT (cashflow_id) DO UPDATE SET
        units_redeemed         = EXCLUDED.units_redeemed,
        avg_cost_at_redemption = EXCLUDED.avg_cost_at_redemption,
        nta_at_date            = EXCLUDED.nta_at_date,
        redemption_value       = EXCLUDED.redemption_value,
        cost_basis             = EXCLUDED.cost_basis,
        realized_pl            = EXCLUDED.realized_pl;

    -- Refresh investor realized_pl
    UPDATE investors SET realized_pl = (
        SELECT COALESCE(SUM(realized_pl), 0)
        FROM redemption_ledger WHERE investor_id = NEW.investor_id
    ) WHERE id = NEW.investor_id;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;


-- ── 3. Attach trigger ─────────────────────────────────────────
DROP TRIGGER IF EXISTS trg_redemption_ledger ON principal_cashflows;

CREATE TRIGGER trg_redemption_ledger
    AFTER INSERT ON principal_cashflows
    FOR EACH ROW
    EXECUTE FUNCTION fn_compute_redemption_ledger();


-- ── 4. Backfill: fire trigger logic for all existing redemptions ──
-- Simulate INSERT trigger by running the same AVCO logic for each row.
-- Process in strict chronological order to preserve AVCO accuracy.
DO $$
DECLARE
    rec       RECORD;
    v_nta     NUMERIC(18,6);
    v_units_r NUMERIC(18,6);
    v_rv      NUMERIC(18,2);
    v_op_u    NUMERIC(18,6);
    v_op_c    NUMERIC(18,2);
    v_op_avg  NUMERIC(18,6);
    v_cb      NUMERIC(18,2);
    v_rpl     NUMERIC(18,2);
    cf        RECORD;
BEGIN
    -- Clear stale data first
    TRUNCATE redemption_ledger;

    FOR rec IN
        SELECT * FROM principal_cashflows
        WHERE cashflow_type = 'redemption'
        ORDER BY date ASC, created_at ASC
    LOOP
        -- Get NTA
        SELECT nta INTO v_nta FROM historical
        WHERE date <= rec.date ORDER BY date DESC LIMIT 1;
        CONTINUE WHEN v_nta IS NULL OR v_nta = 0;

        -- AVCO replay
        v_op_u := 0; v_op_c := 0;
        FOR cf IN
            SELECT cashflow_type, units, amount
            FROM principal_cashflows
            WHERE investor_id = rec.investor_id
              AND (date < rec.date
                   OR (date = rec.date AND created_at < rec.created_at))
            ORDER BY date ASC, created_at ASC
        LOOP
            IF cf.amount > 0 THEN
                v_op_u := v_op_u + ABS(cf.units);
                v_op_c := v_op_c + ABS(cf.amount);
            ELSIF cf.amount < 0 AND v_op_u > 0 THEN
                v_op_avg := v_op_c / v_op_u;
                v_op_u   := GREATEST(0, v_op_u - ABS(cf.units));
                v_op_c   := GREATEST(0, v_op_c - ABS(cf.units) * v_op_avg);
            END IF;
        END LOOP;

        v_op_avg := CASE WHEN v_op_u > 0 THEN v_op_c / v_op_u ELSE v_nta END;
        v_units_r := ROUND(ABS(rec.amount) / v_nta, 6);
        v_rv      := ABS(rec.amount);
        v_cb      := ROUND(v_units_r * v_op_avg, 2);
        v_rpl     := ROUND(v_rv - v_cb, 2);

        INSERT INTO redemption_ledger
            (investor_id, cashflow_id, date,
             units_redeemed, avg_cost_at_redemption, nta_at_date,
             redemption_value, cost_basis, realized_pl)
        VALUES
            (rec.investor_id, rec.id, rec.date,
             v_units_r, ROUND(v_op_avg,6), ROUND(v_nta,6),
             v_rv, v_cb, v_rpl)
        ON CONFLICT (cashflow_id) DO UPDATE SET
            units_redeemed         = EXCLUDED.units_redeemed,
            avg_cost_at_redemption = EXCLUDED.avg_cost_at_redemption,
            nta_at_date            = EXCLUDED.nta_at_date,
            redemption_value       = EXCLUDED.redemption_value,
            cost_basis             = EXCLUDED.cost_basis,
            realized_pl            = EXCLUDED.realized_pl;
    END LOOP;

    -- Update all investors realized_pl
    UPDATE investors i SET realized_pl = (
        SELECT COALESCE(SUM(rl.realized_pl), 0)
        FROM redemption_ledger rl WHERE rl.investor_id = i.id
    );

    RAISE NOTICE 'Backfill complete. Rows: %',
        (SELECT COUNT(*) FROM redemption_ledger);
END $$;
