-- Single-row job tracker for the compute background task
CREATE TABLE IF NOT EXISTS compute_job (
    id          INT PRIMARY KEY DEFAULT 1,  -- always 1 row
    status      TEXT NOT NULL DEFAULT 'idle'
                    CHECK (status IN ('idle','running','done','error')),
    started_at  TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    from_date   DATE,
    to_date     DATE,
    processing_date DATE,          -- last date processed (live progress)
    computed    INT DEFAULT 0,  -- days successfully computed
    errors      INT DEFAULT 0,
    message     TEXT,
    updated_at  TIMESTAMPTZ DEFAULT NOW()
);

-- Seed the single row
INSERT INTO compute_job (id, status) VALUES (1, 'idle')
ON CONFLICT (id) DO NOTHING;
