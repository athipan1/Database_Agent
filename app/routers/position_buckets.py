"""Position and canonical strategy-bucket API contracts."""

from __future__ import annotations

from typing import Any, Dict

from fastapi import APIRouter, Depends, HTTPException

import position_bucket_repository as repository


ROUTE_SIGNATURES = frozenset(
    {
        ("/accounts/{account_id}/position-buckets", "GET"),
        ("/accounts/{account_id}/strategy-bucket-assignments", "GET"),
        ("/accounts/{account_id}/position-buckets/{symbol}", "PATCH"),
        ("/accounts/{account_id}/position-buckets/bulk", "POST"),
    }
)


def create_position_buckets_router(runtime: Any) -> APIRouter:
    """Expose the durable bucket repository through the modular FastAPI app."""

    router = APIRouter(tags=["position-buckets"])

    @router.get("/accounts/{account_id}/position-buckets")
    async def list_position_buckets_endpoint(
        account_id: int,
        api_key: str = Depends(runtime.get_api_key),
        correlation_id: str = Depends(runtime.get_correlation_id),
    ):
        del api_key, correlation_id
        rows = repository.list_position_buckets(runtime.db, account_id)
        return runtime.wrap_response(data=rows)

    @router.get("/accounts/{account_id}/strategy-bucket-assignments")
    async def list_strategy_bucket_assignments_endpoint(
        account_id: int,
        api_key: str = Depends(runtime.get_api_key),
        correlation_id: str = Depends(runtime.get_correlation_id),
    ):
        del api_key, correlation_id
        rows = repository.list_strategy_bucket_assignments(runtime.db, account_id)
        return runtime.wrap_response(data={"assignments": rows, "count": len(rows)})

    @router.patch("/accounts/{account_id}/position-buckets/{symbol}")
    async def set_position_bucket_endpoint(
        account_id: int,
        symbol: str,
        payload: Dict[str, Any],
        api_key: str = Depends(runtime.get_api_key),
        correlation_id: str = Depends(runtime.get_correlation_id),
    ):
        del api_key, correlation_id
        try:
            row = repository.upsert_position_bucket(
                runtime.db,
                account_id,
                symbol,
                payload.get("strategy_bucket")
                or payload.get("bucket")
                or repository.UNASSIGNED,
                source=payload.get("source") or "manual",
                reason=payload.get("reason"),
            )
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        return runtime.wrap_response(data=row)

    @router.post("/accounts/{account_id}/position-buckets/bulk")
    async def bulk_set_position_buckets_endpoint(
        account_id: int,
        payload: Dict[str, Any],
        api_key: str = Depends(runtime.get_api_key),
        correlation_id: str = Depends(runtime.get_correlation_id),
    ):
        del api_key, correlation_id
        assignments = payload.get("assignments") or []
        if not isinstance(assignments, list):
            raise HTTPException(status_code=422, detail="assignments must be a list")
        try:
            updated = repository.bulk_upsert_position_buckets(
                runtime.db,
                account_id,
                assignments,
                default_source=payload.get("source") or "manual",
            )
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        return runtime.wrap_response(
            data={
                "updated": updated,
                "updated_count": len(updated),
                "requested_count": len(assignments),
            }
        )

    return router
