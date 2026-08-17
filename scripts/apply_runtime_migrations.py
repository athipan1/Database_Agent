"""Apply Database_Agent schema migrations before application deployment.

The application startup path is deliberately read-only. This command owns the
legacy idempotent schema setup sequence while the repository transitions to
fully versioned SQL migrations. Railway runs it as a pre-deploy command.
"""

from __future__ import annotations

import logging

from dotenv import load_dotenv

from app.core.config import Settings
from app.services.database_provider import create_trading_db
from backtest_promotion_repository import setup_backtest_promotion_tables
from backtest_repository import setup_backtest_tables
from broker_sync_repository import setup_broker_sync_tables
from execution_job_repository import setup_execution_job_table
from fill_repository import setup_fill_table
from history_repository import setup_history_tables
from plan_record_repository import setup_plan_record_table
from policy_review_repository import setup_policy_review_table
from profit_lifecycle_repository import setup_profit_lifecycle_tables
from protective_order_repository import setup_protective_order_columns
from risk_approval_repository import setup_risk_approval_table
from schema_identity_repository import (
    SCHEMA_VERSION,
    schema_identity_matches,
    setup_schema_identity_table,
)


def apply_runtime_migrations(db) -> bool:
    """Bring the database to the release schema exactly once per version.

    Returns True when migration work was required and False when the schema was
    already current. The migration command is idempotent; application startup
    never invokes it.
    """

    try:
        if schema_identity_matches(db):
            logging.info("Schema %s is already current; no migration needed.", SCHEMA_VERSION)
            return False
    except Exception:
        # A missing identity table is expected on an unbootstrapped database.
        logging.info("Schema identity is absent or unreadable; applying migrations.")

    logging.info("Applying Database_Agent schema migration target %s.", SCHEMA_VERSION)
    db.setup_database()
    setup_history_tables(db)
    setup_risk_approval_table(db)
    setup_protective_order_columns(db)
    setup_execution_job_table(db)
    setup_fill_table(db)
    setup_broker_sync_tables(db)
    setup_plan_record_table(db)
    setup_policy_review_table(db)
    setup_profit_lifecycle_tables(db)
    setup_backtest_tables(db)
    setup_backtest_promotion_tables(db)
    db.ensure_price_partitions()

    # The identity marker is written last. If any prior migration step fails,
    # the target release is not marked as applied and deployment fails closed.
    setup_schema_identity_table(db)
    if not schema_identity_matches(db):
        raise RuntimeError(
            f"Database schema migration completed but identity {SCHEMA_VERSION} did not verify"
        )
    logging.info("Database schema migration target %s verified.", SCHEMA_VERSION)
    return True


def main() -> int:
    load_dotenv()
    logging.basicConfig(level=logging.INFO)
    settings = Settings.from_environ()
    settings.validate()
    database = create_trading_db(settings)
    apply_runtime_migrations(database)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
