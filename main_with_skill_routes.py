"""Database_Agent runtime entrypoint.

This entrypoint keeps the legacy ``main.py`` application while installing the
atomic strategy-bucket order creation contract and the account-aware order
execution contract used by the Docker runtime.

Curator-facing skill and backtest routes are mounted once by
``policy_review_routes.create_policy_review_routes``.  This module only keeps
their startup table verification so OpenAPI and request routing remain unique.
"""

from __future__ import annotations

import logging

import main as main_module
from backtest_repository import setup_backtest_tables
from order_creation_persistence import install_strategy_bucket_order_creation
from order_execution_contract import install_order_execution_contract
from skill_performance_repository import setup_skill_performance_tables


install_strategy_bucket_order_creation(main_module)
install_order_execution_contract(main_module)

app = main_module.app
db = main_module.db
get_api_key = main_module.get_api_key
get_correlation_id = main_module.get_correlation_id


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
