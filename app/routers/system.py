"""Health and operational system routes."""

from __future__ import annotations

import logging
from typing import Any

from fastapi import APIRouter, Response, status
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from models import StandardAgentResponse


ROUTE_SIGNATURES = frozenset(
    {
        ("/health", "GET"),
        ("/ready", "GET"),
        ("/metrics", "GET"),
    }
)


def _database_connection_status(runtime: Any) -> bool:
    try:
        return bool(runtime.db.check_connection())
    except Exception as exc:
        logging.warning("Database connection check failed: %s", exc)
        return False


def _database_status_payload(runtime: Any, is_connected: bool) -> dict:
    return {
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


def create_system_router(runtime: Any) -> APIRouter:
    router = APIRouter(tags=["system"])

    @router.get("/health", response_model=StandardAgentResponse[dict])
    async def health_check():
        """Report process health without removing an instance from service."""

        logging.info("Health check endpoint was called.")
        is_connected = _database_connection_status(runtime)
        return runtime.wrap_response(
            data=_database_status_payload(runtime, is_connected)
        )

    @router.get(
        "/ready",
        response_model=StandardAgentResponse[dict],
        responses={
            status.HTTP_503_SERVICE_UNAVAILABLE: {
                "description": "PostgreSQL is not ready",
            }
        },
    )
    async def readiness_check(response: Response):
        """Return 503 until the production PostgreSQL connection is usable."""

        logging.info("Readiness check endpoint was called.")
        is_connected = _database_connection_status(runtime)
        if not is_connected:
            response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
        return runtime.wrap_response(
            data=_database_status_payload(runtime, is_connected)
        )

    @router.get("/metrics")
    async def metrics():
        return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)

    return router
