BEGIN;

-- Rollback removes only promotion lifecycle data. Existing backtest evidence,
-- trades, equity curves, positions, orders, and broker records are untouched.
DROP TABLE IF EXISTS backtest_promotion_transitions;
DROP TABLE IF EXISTS backtest_promotions;

COMMIT;
