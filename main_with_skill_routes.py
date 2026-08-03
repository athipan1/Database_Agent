"""Database_Agent production runtime entrypoint.

The modular ``main.py`` facade assembles the core API while this entrypoint
installs atomic order contracts, promotion observation reconciliation, and
Curator-facing skill telemetry routes.
"""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager

import main as main_module
from backtest_promotion_observation_routes import (
    create_backtest_promotion_observation_routes,
    install_backtest_promotion_observation_openapi,
)
from backtest_promotion_observation_service import (
    setup_backtest_promotion_observation_tables,
)
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
    "backtest-promotion-observations",
    create_backtest_promotion_observation_routes(
        db,
        get_api_key,
        get_correlation_id,
    ),
)
install_backtest_promotion_observation_openapi(app)


_base_lifespan = app.router.lifespan_context


@asynccontextmanager
async def runtime_lifespan(app_instance):
    async with _base_lifespan(app_instance):
        try:
            setup_skill_performance_tables(db)
            setup_backtest_promotion_observation_tables(db)
            logging.info("Runtime extension table verification complete.")
        except Exception:
            logging.exception("Failed to verify runtime extension tables.")
            raise
        yield


app.router.lifespan_context = runtime_lifespan
