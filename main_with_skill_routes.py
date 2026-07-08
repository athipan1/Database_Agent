"""Database_Agent runtime entrypoint.

This entrypoint keeps the legacy ``main.py`` application while mounting the
Curator-facing routes and installing the atomic strategy-bucket order creation
contract used by the Docker runtime.

Mounted Curator routes:

- POST /skills/execution-logs
- GET  /skills/performance/rank
- GET  /skills/{skill_id}/backtest-status
"""

from __future__ import annotations

import logging

import main as main_module
from backtest_repository import setup_backtest_tables
from backtest_routes import create_backtest_routes
from order_creation_persistence import install_strategy_bucket_order_creation
from skill_performance_repository import setup_skill_performance_tables
from skill_performance_routes import create_skill_performance_routes


install_strategy_bucket_order_creation(main_module)

app = main_module.app
db = main_module.db
get_api_key = main_module.get_api_key
get_correlation_id = main_module.get_correlation_id

app.include_router(create_skill_performance_routes(db, get_api_key, get_correlation_id))
app.include_router(create_backtest_routes(db, get_api_key, get_correlation_id))


@app.on_event("startup")
async def startup_skill_and_backtest_tables() -> None:
    """Ensure Curator-facing tables exist when the API starts."""
    try:
        setup_skill_performance_tables(db)
        setup_backtest_tables(db)
        logging.info("Skill performance and backtest tables verification/creation complete.")
    except Exception:
        logging.exception("Failed to verify/create skill performance and backtest tables.")
        raise
