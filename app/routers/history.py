"""Signal history and performance metric API routes."""

from __future__ import annotations

from typing import Any, List, Optional

from fastapi import APIRouter, Depends

from models import (
    CreatePerformanceMetricBody,
    CreateSignalHistoryBody,
    PerformanceMetric,
    SignalHistory,
    StandardAgentResponse,
)


ROUTE_SIGNATURES = frozenset(
    {
        ("/history/signals", "POST"),
        ("/history/signals", "GET"),
        ("/history/performance", "POST"),
        ("/history/performance", "GET"),
    }
)


def create_history_router(runtime: Any) -> APIRouter:
    """Build history routes against a runtime module.

    Repository functions are resolved from ``runtime`` inside each request so
    existing tests and operator tooling can continue patching ``main`` symbols.
    """

    router = APIRouter(tags=["history"])

    @router.post(
        "/history/signals",
        response_model=StandardAgentResponse[SignalHistory],
    )
    async def create_signal_history_endpoint(
        body: CreateSignalHistoryBody,
        api_key: str = Depends(runtime.get_api_key),
        correlation_id: str = Depends(runtime.get_correlation_id),
    ):
        record = runtime.create_signal_record(runtime.db, body)
        return runtime.wrap_response(data=record)

    @router.get(
        "/history/signals",
        response_model=StandardAgentResponse[List[SignalHistory]],
    )
    async def list_signal_history_endpoint(
        limit: int = 100,
        symbol: Optional[str] = None,
        api_key: str = Depends(runtime.get_api_key),
        correlation_id: str = Depends(runtime.get_correlation_id),
    ):
        records = runtime.get_signal_records(
            runtime.db,
            limit=limit,
            symbol=symbol,
        )
        return runtime.wrap_response(data=records)

    @router.post(
        "/history/performance",
        response_model=StandardAgentResponse[PerformanceMetric],
    )
    async def create_performance_endpoint(
        body: CreatePerformanceMetricBody,
        api_key: str = Depends(runtime.get_api_key),
        correlation_id: str = Depends(runtime.get_correlation_id),
    ):
        record = runtime.create_performance_record(
            runtime.db,
            body,
            correlation_id,
        )
        return runtime.wrap_response(data=record)

    @router.get(
        "/history/performance",
        response_model=StandardAgentResponse[List[PerformanceMetric]],
    )
    async def list_performance_endpoint(
        limit: int = 100,
        strategy: Optional[str] = None,
        api_key: str = Depends(runtime.get_api_key),
        correlation_id: str = Depends(runtime.get_correlation_id),
    ):
        records = runtime.get_performance_records(
            runtime.db,
            limit=limit,
            strategy=strategy,
        )
        return runtime.wrap_response(data=records)

    return router
