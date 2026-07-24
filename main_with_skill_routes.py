"""Database_Agent production runtime entrypoint.

The modular ``main.py`` facade assembles the core API while this entrypoint
installs the atomic strategy-bucket order creation contract, the account-aware
order execution contract, and Curator-facing routes.

Curator routes are merged by ``path + methods`` signature. Existing routes are
preserved and only missing routes are appended.
"""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager

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


def _route_signature(route) -> tuple[str, frozenset[str]]:
    return (
        str(getattr(route, "path", "")),
        frozenset(getattr(route, "methods", None) or set()),
    )


def _mount_missing_routes(name: str, router) -> None:
    """Append only routes not already registered on the active FastAPI app."""

    existing_signatures = {
        _route_signature(route) for route in app.router.routes
    }
    added = 0
    skipped = 0

    for route in router.routes:
        signature = _route_signature(route)
        if signature in existing_signatures:
            skipped += 1
            continue
        app.router.routes.append(route)
        existing_signatures.add(signature)
        added += 1

    logging.info(
        "Runtime routes merged",
        extra={
            "router_name": name,
            "candidate_routes": len(router.routes),
            "routes_added": added,
            "routes_skipped": skipped,
            "total_routes": len(app.router.routes),
        },
    )


_mount_missing_routes(
    "skill-performance",
    create_skill_performance_routes(db, get_api_key, get_correlation_id),
)
_mount_missing_routes(
    "backtests",
    create_backtest_routes(db, get_api_key, get_correlation_id),
)


_base_lifespan = app.router.lifespan_context


@asynccontextmanager
async def runtime_lifespan(app_instance):
    """Run core startup first, then verify Curator-facing tables."""

    async with _base_lifespan(app_instance):
        try:
            setup_skill_performance_tables(db)
            setup_backtest_tables(db)
            logging.info(
                "Skill performance and backtest tables "
                "verification/creation complete."
            )
        except Exception:
            logging.exception(
                "Failed to verify/create skill performance and backtest tables."
            )
            raise
        yield


app.router.lifespan_context = runtime_lifespan
