BEGIN;

-- Backfill policy: historical backtest runs are not promoted automatically.
-- A caller must create a GENERATED promotion from one exact stored run so the
-- lifecycle starts with an auditable evidence identity and correlation ID.
CREATE TABLE IF NOT EXISTS backtest_promotions (
    promotion_id TEXT PRIMARY KEY,
    account_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    skill_id TEXT NOT NULL,
    strategy_id TEXT NOT NULL,
    symbol TEXT NOT NULL,
    timeframe TEXT NOT NULL,
    dataset_fingerprint TEXT NOT NULL,
    engine_version TEXT NOT NULL,
    validation_profile TEXT NOT NULL,
    state TEXT NOT NULL,
    version INTEGER NOT NULL DEFAULT 1,
    evidence_version INTEGER NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    validated_at TIMESTAMPTZ,
    oos_passed_at TIMESTAMPTZ,
    robustness_passed_at TIMESTAMPTZ,
    approved_for_paper_at TIMESTAMPTZ,
    paper_observing_at TIMESTAMPTZ,
    expires_at TIMESTAMPTZ,
    revoked_at TIMESTAMPTZ,
    rejected_at TIMESTAMPTZ,
    failed_at TIMESTAMPTZ,
    last_observed_at TIMESTAMPTZ,
    reason_code TEXT,
    reason TEXT,
    correlation_id TEXT,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    CONSTRAINT ck_backtest_promotions_state CHECK (
        state IN (
            'GENERATED',
            'VALIDATED',
            'OOS_PASSED',
            'ROBUSTNESS_PASSED',
            'APPROVED_FOR_PAPER',
            'PAPER_OBSERVING',
            'REJECTED',
            'FAILED',
            'EXPIRED',
            'REVOKED'
        )
    ),
    CONSTRAINT ck_backtest_promotions_version CHECK (version >= 1),
    CONSTRAINT ck_backtest_promotions_evidence_version CHECK (evidence_version >= 1),
    CONSTRAINT uq_backtest_promotions_account_run UNIQUE (account_id, run_id),
    CONSTRAINT uq_backtest_promotions_exact_evidence UNIQUE (
        account_id,
        symbol,
        strategy_id,
        timeframe,
        dataset_fingerprint,
        engine_version
    ),
    CONSTRAINT uq_backtest_promotions_version UNIQUE (promotion_id, version)
);

CREATE TABLE IF NOT EXISTS backtest_promotion_transitions (
    transition_id TEXT PRIMARY KEY,
    promotion_id TEXT NOT NULL REFERENCES backtest_promotions(promotion_id),
    from_state TEXT NOT NULL,
    to_state TEXT NOT NULL,
    from_version INTEGER NOT NULL,
    to_version INTEGER NOT NULL,
    status TEXT NOT NULL,
    reason_code TEXT NOT NULL,
    reason TEXT NOT NULL,
    evidence_run_id TEXT NOT NULL,
    correlation_id TEXT,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT ck_backtest_promotion_transitions_from_state CHECK (
        from_state IN (
            'GENERATED', 'VALIDATED', 'OOS_PASSED', 'ROBUSTNESS_PASSED',
            'APPROVED_FOR_PAPER', 'PAPER_OBSERVING', 'REJECTED', 'FAILED',
            'EXPIRED', 'REVOKED'
        )
    ),
    CONSTRAINT ck_backtest_promotion_transitions_to_state CHECK (
        to_state IN (
            'GENERATED', 'VALIDATED', 'OOS_PASSED', 'ROBUSTNESS_PASSED',
            'APPROVED_FOR_PAPER', 'PAPER_OBSERVING', 'REJECTED', 'FAILED',
            'EXPIRED', 'REVOKED'
        )
    ),
    CONSTRAINT ck_backtest_promotion_transitions_version CHECK (
        from_version >= 1 AND to_version = from_version + 1
    ),
    CONSTRAINT ck_backtest_promotion_transitions_status CHECK (
        status IN ('COMPLETED', 'REJECTED')
    ),
    CONSTRAINT uq_backtest_promotion_transition_version UNIQUE (
        promotion_id,
        from_version,
        to_version
    )
);

CREATE INDEX IF NOT EXISTS idx_backtest_promotions_account
    ON backtest_promotions(account_id);
CREATE INDEX IF NOT EXISTS idx_backtest_promotions_symbol
    ON backtest_promotions(symbol);
CREATE INDEX IF NOT EXISTS idx_backtest_promotions_strategy
    ON backtest_promotions(strategy_id);
CREATE INDEX IF NOT EXISTS idx_backtest_promotions_timeframe
    ON backtest_promotions(timeframe);
CREATE INDEX IF NOT EXISTS idx_backtest_promotions_state
    ON backtest_promotions(state);
CREATE INDEX IF NOT EXISTS idx_backtest_promotions_expires
    ON backtest_promotions(expires_at);
CREATE INDEX IF NOT EXISTS idx_backtest_promotions_run
    ON backtest_promotions(run_id);
CREATE INDEX IF NOT EXISTS idx_backtest_promotions_dataset
    ON backtest_promotions(dataset_fingerprint);
CREATE INDEX IF NOT EXISTS idx_backtest_promotions_updated
    ON backtest_promotions(updated_at);
CREATE INDEX IF NOT EXISTS idx_backtest_promotions_exact_lookup
    ON backtest_promotions(
        account_id,
        symbol,
        strategy_id,
        timeframe,
        updated_at DESC
    );
CREATE INDEX IF NOT EXISTS idx_backtest_promotion_transitions_history
    ON backtest_promotion_transitions(promotion_id, created_at);

COMMIT;
