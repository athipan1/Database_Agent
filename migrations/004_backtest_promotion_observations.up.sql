BEGIN;

CREATE TABLE IF NOT EXISTS backtest_promotion_observations (
    observation_id TEXT PRIMARY KEY,
    promotion_id TEXT NOT NULL,
    observation_key TEXT NOT NULL,
    action TEXT NOT NULL CHECK (
        action IN ('START_OBSERVING', 'HEARTBEAT', 'EXPIRE', 'REVOKE')
    ),
    reason_code TEXT NOT NULL,
    from_state TEXT NOT NULL,
    to_state TEXT NOT NULL,
    from_version INTEGER NOT NULL CHECK (from_version >= 1),
    to_version INTEGER NOT NULL CHECK (to_version >= 1),
    observed_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    correlation_id TEXT,
    paper_drawdown_pct DOUBLE PRECISION NOT NULL CHECK (
        paper_drawdown_pct >= 0 AND paper_drawdown_pct <= 1
    ),
    reconciliation_ok BOOLEAN NOT NULL,
    duplicate_order_count INTEGER NOT NULL CHECK (duplicate_order_count >= 0),
    broker_order_count INTEGER NOT NULL CHECK (broker_order_count >= 0),
    database_order_count INTEGER NOT NULL CHECK (database_order_count >= 0),
    filled_order_count INTEGER NOT NULL CHECK (filled_order_count >= 0),
    strategy_drift BOOLEAN NOT NULL,
    emergency_halt BOOLEAN NOT NULL,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    result_snapshot JSONB NOT NULL,
    CONSTRAINT uq_promotion_observation_key
        UNIQUE (promotion_id, observation_key),
    CONSTRAINT fk_promotion_observation
        FOREIGN KEY (promotion_id)
        REFERENCES backtest_promotions(promotion_id)
        ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_promotion_observations_timeline
    ON backtest_promotion_observations (
        promotion_id, observed_at DESC, created_at DESC
    );

COMMIT;
