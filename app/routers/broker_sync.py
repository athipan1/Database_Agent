"""Broker synchronization status and snapshot API routes.

Route registration is intentionally independent from database migrations. DDL is
owned by the migration runner; FastAPI routes belong to the API process.
"""

from __future__ import annotations

from typing import Any, Dict

from fastapi import APIRouter, Depends

from broker_sync_repository import sync_broker_state
from broker_sync_status_repository import broker_sync_status


ROUTE_SIGNATURES = frozenset(
    {
        ("/broker-sync/status", "GET"),
        ("/broker-sync/snapshot", "POST"),
        ("/skills/trade-outcomes", "POST"),
    }
)


def create_broker_sync_router(runtime: Any) -> APIRouter:
    router = APIRouter(tags=["broker-sync"])

    @router.get("/broker-sync/status")
    async def broker_sync_status_endpoint(
        account_id: int = 1,
        api_key: str = Depends(runtime.get_api_key),
        correlation_id: str = Depends(runtime.get_correlation_id),
    ):
        return runtime.wrap_response(
            data=broker_sync_status(runtime.db, account_id=account_id)
        )

    @router.post("/broker-sync/snapshot")
    async def broker_sync_snapshot_endpoint(
        payload: Dict[str, Any],
        api_key: str = Depends(runtime.get_api_key),
        correlation_id: str = Depends(runtime.get_correlation_id),
    ):
        return runtime.wrap_response(
            data=sync_broker_state(runtime.db, payload)
        )

    @router.post("/skills/trade-outcomes")
    async def skill_trade_outcome_endpoint(
        payload: Dict[str, Any],
        api_key: str = Depends(runtime.get_api_key),
        correlation_id: str = Depends(runtime.get_correlation_id),
    ):
        from skill_trade_outcome_repository import create_skill_trade_outcome
        return runtime.wrap_response(
            data=create_skill_trade_outcome(runtime.db, payload)
        )

    return router
