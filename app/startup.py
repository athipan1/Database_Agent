"""Database runtime startup, table setup, and shutdown orchestration."""

from __future__ import annotations

import logging

from broker_sync_repository import setup_broker_sync_tables
from execution_job_repository import setup_execution_job_table
from fill_repository import setup_fill_table
from history_repository import setup_history_tables
from plan_record_repository import setup_plan_record_table
from policy_review_repository import setup_policy_review_table
from profit_lifecycle_repository import setup_profit_lifecycle_tables
from protective_order_repository import setup_protective_order_columns
from risk_approval_repository import setup_risk_approval_table
from schema_identity_repository import setup_schema_identity_table


def setup_runtime_tables(db) -> None:
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
    setup_schema_identity_table(db)


def log_database_stats(db) -> None:
    logging.info("Collecting database statistics.")
    try:
        stats = db.get_database_stats()
        if stats:
            logging.info("Database Statistics", extra={"db_stats": stats})
    except Exception as exc:
        logging.warning("Could not collect database stats: %s", exc)


async def startup_runtime(runtime) -> None:
    logging.info("Database Agent API starting up.")
    try:
        setup_runtime_tables(runtime.db)
        logging.info("Database tables verification/creation complete.")
        runtime.runtime_scheduler.configure(
            ingestion_job=runtime.run_ingestion_job,
            partition_job=runtime.db.ensure_price_partitions,
            stats_job=runtime.log_database_stats,
        )
        runtime.runtime_scheduler.start()
    except Exception as exc:
        logging.critical(
            "FATAL: Application startup failed: %s",
            exc,
            exc_info=True,
        )
        if not runtime.DATABASE_DEV_MODE:
            raise
        logging.warning(
            "DATABASE_DEV_MODE is enabled, continuing startup with fallback responses."
        )


async def shutdown_runtime(runtime) -> None:
    logging.info("Database Agent API shutting down.")
    runtime.runtime_scheduler.stop()
