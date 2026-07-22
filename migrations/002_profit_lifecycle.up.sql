BEGIN;

ALTER TABLE positions ADD COLUMN IF NOT EXISTS position_version INTEGER NOT NULL DEFAULT 1;
ALTER TABLE positions ADD COLUMN IF NOT EXISTS first_target_executed BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE positions ADD COLUMN IF NOT EXISTS second_target_executed BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE positions ADD COLUMN IF NOT EXISTS total_exited_quantity NUMERIC(24, 8) NOT NULL DEFAULT 0;
ALTER TABLE positions ADD COLUMN IF NOT EXISTS last_profit_decision_id TEXT;
ALTER TABLE positions ADD COLUMN IF NOT EXISTS last_profit_decision_status TEXT;
ALTER TABLE positions ADD COLUMN IF NOT EXISTS last_profit_decision_at TIMESTAMPTZ;

CREATE TABLE IF NOT EXISTS profit_decisions (
    record_id BIGSERIAL PRIMARY KEY,
    account_id TEXT NOT NULL,
    position_id TEXT NOT NULL,
    position_version INTEGER NOT NULL,
    decision_id TEXT NOT NULL,
    decision_type TEXT NOT NULL,
    status TEXT NOT NULL,
    proposed_quantity NUMERIC(24, 8) NOT NULL,
    executed_quantity NUMERIC(24, 8) NOT NULL DEFAULT 0,
    correlation_id TEXT,
    next_lifecycle_state JSONB NOT NULL DEFAULT '{}'::jsonb,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    error TEXT,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_profit_decision_identity
        UNIQUE (account_id, position_id, decision_id)
);

CREATE INDEX IF NOT EXISTS idx_profit_decisions_position
    ON profit_decisions(account_id, position_id, created_at);
CREATE INDEX IF NOT EXISTS idx_profit_decisions_status
    ON profit_decisions(status, updated_at);

COMMIT;
