BEGIN;

DROP INDEX IF EXISTS idx_promotion_observations_timeline;
DROP TABLE IF EXISTS backtest_promotion_observations;

COMMIT;
