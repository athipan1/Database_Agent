"""Database_Agent runtime entrypoint.

This entrypoint keeps the legacy ``main.py`` application while installing the
atomic strategy-bucket order creation contract and the account-aware order
execution contract used by the Docker runtime.

Curator-facing skill and backtest routes are mounted defensively: an existing
route is preserved, while a missing route group is added exactly once.
"""

from __future__ import annotations

import logging

import main as main_module
from backtest_repository import setup_backtest_tables
from backtest_routes import create_backtest_routes
from order_creation_persistence import install_strategy_bucket_order_creation
from order_execution_contract import install_order_execution_contract
from skill_performance_repository import setup_skill_performance_tables
from skill_performance_routes import create_skill_performance_routes


install_strategy_bucket_order_creation(main_module)
install_order_execution_contract(main_module)

app = main_module.app
db = main_module.db
get_api_key = main_module.get_api_key
get_correlation_id = main_module.get_correlation_id


def _has_route(path: str, method: str) -> bool:
    expected_method = method.upper()
    return any(
        getattr(route, "path", None) == path
        and expected_method in (getattr(route, "methods", None) or set())
        for route in app.router.routes
    )


def _mount_router_once(name: str, sentinel_path: str, router) -> None:
    before = len(app.router.routes)
    already_present = _has_route(sentinel_path, "GET")
    candidate_routes = len(router.routes)
    if not already_present:
        app.router.include_router(router)
    after = len(app.router.routes)
    logging.info(
        "Runtime router mount evaluated",
        extra={
            "router_name": name,
            "sentinel_path": sentinel_path,
            "already_present": already_present,
            "candidate_routes": candidate_routes,
            "routes_before": before,
            "routes_after": after,
        },
    )


_mount_router_once(
    "skill-performance",
    "/skills/performance/rank",
    create_skill_performance_routes(db, get_api_key, get_correlation_id),
)
_mount_router_once(
    "backtests",
    "/backtests/runs/latest",
    create_backtest_routes(db, get_api_key, get_correlation_id),
)


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
