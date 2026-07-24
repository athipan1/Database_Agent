"""Database Agent FastAPI application assembly."""

from __future__ import annotations

from typing import Any

from fastapi import FastAPI
from prometheus_fastapi_instrumentator import Instrumentator

from app.core.middleware import install_correlation_id_middleware
from app.core.responses import AGENT_VERSION, install_exception_handlers
from app.lifespan import create_runtime_lifespan
from app.route_registry import assert_unique_routes, mount_router_routes
from app.routers.accounts_orders import create_accounts_orders_router
from app.routers.execution import create_execution_router
from app.routers.history import create_history_router
from app.routers.system import create_system_router
from plan_record_routes import create_plan_record_routes
from policy_review_routes import create_policy_review_routes
from profit_lifecycle_routes import create_profit_lifecycle_routes


def create_application(runtime: Any) -> FastAPI:
    """Build the complete API directly from modular runtime components."""

    app = FastAPI(
        title="Database Agent - Secure Trading API",
        version=AGENT_VERSION,
        lifespan=create_runtime_lifespan(runtime),
    )

    install_correlation_id_middleware(app, runtime.correlation_id_var)
    install_exception_handlers(app, runtime.wrap_response)

    mount_router_routes(app, create_system_router(runtime))
    mount_router_routes(
        app,
        create_plan_record_routes(
            runtime.db,
            runtime.get_api_key,
            runtime.get_correlation_id,
        ),
    )
    mount_router_routes(
        app,
        create_policy_review_routes(
            runtime.db,
            runtime.get_api_key,
            runtime.get_correlation_id,
        ),
    )
    mount_router_routes(
        app,
        create_profit_lifecycle_routes(
            runtime.db,
            runtime.get_api_key,
            runtime.get_correlation_id,
        ),
    )
    mount_router_routes(app, create_history_router(runtime))
    mount_router_routes(app, create_execution_router(runtime))
    mount_router_routes(app, create_accounts_orders_router(runtime))

    Instrumentator().instrument(app)
    assert_unique_routes(app)
    return app
