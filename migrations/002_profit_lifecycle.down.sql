BEGIN;

DROP TABLE IF EXISTS profit_decisions;

ALTER TABLE positions DROP COLUMN IF EXISTS last_profit_decision_at;
ALTER TABLE positions DROP COLUMN IF EXISTS last_profit_decision_status;
ALTER TABLE positions DROP COLUMN IF EXISTS last_profit_decision_id;
ALTER TABLE positions DROP COLUMN IF EXISTS total_exited_quantity;
ALTER TABLE positions DROP COLUMN IF EXISTS second_target_executed;
ALTER TABLE positions DROP COLUMN IF EXISTS first_target_executed;
ALTER TABLE positions DROP COLUMN IF EXISTS position_version;

COMMIT;
