"""Database Agent FastAPI application assembly."""

from __future__ import annotations

from typing import Any

from fastapi import FastAPI

from app.lifespan import create_runtime_lifespan
from app.route_registry import (
    assert_unique_routes,
    copy_missing_routes,
    method_signatures,
    mount_router_routes,
)
from app.routers.accounts_orders import (
    ROUTE_SIGNATURES as ACCOUNT_ORDER_SIGNATURES,
    create_accounts_orders_router,
)
from app.routers.execution import (
    ROUTE_SIGNATURES as EXECUTION_SIGNATURES,
    create_execution_router,
)
from app.routers.history import (
    ROUTE_SIGNATURES as HISTORY_SIGNATURES,
    create_history_router,
)


def _framework_paths(source: FastAPI) -> set[str]:
    paths = {
        source.openapi_url,
        source.docs_url,
        source.redoc_url,
        source.swagger_ui_oauth2_redirect_url,
    }
    return {path for path in paths if path}


def _copy_middleware(source: FastAPI, target: FastAPI) -> None:
    # add_middleware inserts at the front, so reverse to retain source order.
    for middleware in reversed(source.user_middleware):
        target.add_middleware(
            middleware.cls,
            *middleware.args,
            **middleware.kwargs,
        )


def create_application(runtime: Any) -> FastAPI:
    """Create the modular app while preserving unextracted legacy routes.

    The source runtime remains the compatibility boundary during migration.
    Routes owned by modular routers are excluded before the new routers are
    mounted, making activation atomic and preventing duplicate OpenAPI operations.
    """

    source = runtime.app
    app = FastAPI(
        title=source.title,
        summary=getattr(source, "summary", None),
        description=source.description,
        version=source.version,
        openapi_url=source.openapi_url,
        docs_url=source.docs_url,
        redoc_url=source.redoc_url,
        swagger_ui_oauth2_redirect_url=(
            source.swagger_ui_oauth2_redirect_url
        ),
        lifespan=create_runtime_lifespan(runtime),
    )

    _copy_middleware(source, app)
    app.exception_handlers.update(source.exception_handlers)
    app.dependency_overrides.update(source.dependency_overrides)
    app.router.redirect_slashes = source.router.redirect_slashes
    app.state._state.update(source.state._state)

    excluded = method_signatures(
        ACCOUNT_ORDER_SIGNATURES
        | EXECUTION_SIGNATURES
        | HISTORY_SIGNATURES
    )
    copy_missing_routes(
        source,
        app,
        excluded=excluded,
        excluded_paths=_framework_paths(source),
    )

    mount_router_routes(app, create_history_router(runtime))
    mount_router_routes(app, create_execution_router(runtime))
    mount_router_routes(app, create_accounts_orders_router(runtime))

    assert_unique_routes(app)
    return app
