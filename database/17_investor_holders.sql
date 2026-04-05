-- Migration 17: Option B — investor_holders junction table
-- Adds joint account / nominee support

-- 1. account_type on investors
ALTER TABLE investors
  ADD COLUMN IF NOT EXISTS account_type TEXT NOT NULL DEFAULT 'individual'
      CHECK (account_type IN ('individual', 'joint', 'nominee'));

-- 2. Junction table — one investor can have multiple holders
CREATE TABLE IF NOT EXISTS investor_holders (
    id           UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    investor_id  UUID NOT NULL REFERENCES investors(id) ON DELETE CASCADE,
    user_id      UUID NOT NULL REFERENCES users(id)     ON DELETE CASCADE,
    role         TEXT NOT NULL DEFAULT 'secondary'
                 CHECK (role IN ('primary', 'secondary', 'nominee')),
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (investor_id, user_id)
);

CREATE INDEX idx_holders_investor ON investor_holders(investor_id);
CREATE INDEX idx_holders_user     ON investor_holders(user_id);

-- 3. Backfill existing primary holders from users.investor_id
INSERT INTO investor_holders (investor_id, user_id, role)
SELECT investor_id, id, 'primary'
FROM   users
WHERE  investor_id IS NOT NULL
ON CONFLICT (investor_id, user_id) DO NOTHING;

-- 4. Nominee positions — share positions across investors
--    nominee_id = the investor whose position is being managed
--    holder_investor_id = the joint/primary investor who can view/manage it
CREATE TABLE IF NOT EXISTS nominee_links (
    id                   UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    holder_investor_id   UUID NOT NULL REFERENCES investors(id) ON DELETE CASCADE,
    nominee_investor_id  UUID NOT NULL REFERENCES investors(id) ON DELETE CASCADE,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (holder_investor_id, nominee_investor_id),
    CHECK (holder_investor_id != nominee_investor_id)
);

CREATE INDEX idx_nominee_holder  ON nominee_links(holder_investor_id);
CREATE INDEX idx_nominee_nominee ON nominee_links(nominee_investor_id);
