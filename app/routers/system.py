"""Health, readiness, and operational system routes."""

from __future__ import annotations

import logging
from typing import Any

from fastapi import APIRouter, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from app.core.config import Settings
from app.services.database_readiness import inspect_database_readiness
from models import StandardAgentResponse


ROUTE_SIGNATURES = frozenset(
    {
        ("/health", "GET"),
        ("/ready", "GET"),
        ("/metrics", "GET"),
    }
)


def create_system_router(runtime: Any) -> APIRouter:
    router = APIRouter(tags=["system"])

    @router.get("/health", response_model=StandardAgentResponse[dict])
    async def health_check():
        logging.info("Health check endpoint was called.")
        try:
            is_connected = runtime.db.check_connection()
        except Exception as exc:
            logging.warning("Health check database connection failed: %s", exc)
            is_connected = False
        return runtime.wrap_response(
            data={
                "status": (
                    "healthy"
                    if is_connected or runtime.DATABASE_DEV_MODE
                    else "unhealthy"
                ),
                "database_connection": (
                    "connected"
                    if is_connected
                    else "dev_fallback"
                    if runtime.DATABASE_DEV_MODE
                    else "disconnected"
                ),
                "dev_mode": runtime.DATABASE_DEV_MODE,
                "trading_mode": runtime.TRADING_MODE,
                "database_emergency_halt": runtime.DATABASE_EMERGENCY_HALT,
            }
        )

    @router.get("/ready", response_model=StandardAgentResponse[dict])
    async def readiness_check(response: Response):
        """Return 503 until the configured primary passes all cutover gates."""

        # main.py validates this environment snapshot before the app is built.
        # Reading it here preserves cutover-only fields without widening the
        # patch-compatible runtime facade.
        report = inspect_database_readiness(runtime.db, Settings.from_environ())
        if report["ready"]:
            return runtime.wrap_response(data=report)
        response.status_code = 503
        return runtime.wrap_response(
            status="error",
            data=report,
            error={
                "code": "DATABASE_NOT_READY",
                "message": "Database primary did not pass readiness gates",
                "retryable": True,
            },
        )

    @router.get("/metrics")
    async def metrics():
        return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)

    return router
