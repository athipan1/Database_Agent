"""Health and operational system routes."""

from __future__ import annotations

import logging
from typing import Any

from fastapi import APIRouter, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from models import StandardAgentResponse


ROUTE_SIGNATURES = frozenset(
    {
        ("/health", "GET"),
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

    @router.get("/metrics")
    async def metrics():
        return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)

    return router
