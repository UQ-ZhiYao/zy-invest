-- Remove investor_id from transactions table
-- Trades are fund-level, not per-investor
ALTER TABLE transactions DROP COLUMN IF EXISTS investor_id;
