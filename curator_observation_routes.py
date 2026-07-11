from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Security
from fastapi.security import APIKeyHeader

from curator_observation_models import (
    CreateCuratorObservationBatchBody,
    CreateCuratorObservationBody,
)
from curator_observation_repository import (
    build_curator_observation_readiness,
    create_curator_observation,
    create_curator_observation_batch,
    get_curator_observation,
    list_curator_observations,
)


def _wrap_response(
    *,
    data: Any = None,
    status: str = "success",
    error: Optional[dict] = None,
    correlation_id: Optional[str] = None,
):
    return {
        "status": status,
        "agent_type": "database",
        "version": "1.1.0",
        "schema_version": "1.0",
        "timestamp": datetime.now(timezone.utc),
        "correlation_id": correlation_id,
        "data": data,
        "metadata": {},
        "error": error,
        "confidence_score": None,
    }


def create_curator_observation_routes(
    db,
    get_api_key_dependency,
    get_correlation_id_dependency,
) -> APIRouter:
    router = APIRouter(tags=["curator-observations"])
    api_key_header = APIKeyHeader(name="X-API-KEY", auto_error=False)

    async def _api_key(api_key_header_value: str = Security(api_key_header)):
        return get_api_key_dependency(api_key_header_value)

    async def _correlation_id():
        return await get_correlation_id_dependency()

    @router.post("/curator/observations", response_model=dict)
    async def create_curator_observation_endpoint(
        body: CreateCuratorObservationBody,
        api_key: str = Depends(_api_key),
        correlation_id: str = Depends(_correlation_id),
    ):
        try:
            record = create_curator_observation(
                db,
                body,
                correlation_id=correlation_id,
            )
        except Exception as exc:
            raise HTTPException(status_code=500, detail=str(exc)) from exc
        return _wrap_response(
            data=record.model_dump(mode="json"),
            correlation_id=correlation_id,
        )

    @router.post("/curator/observations/batch", response_model=dict)
    async def create_curator_observation_batch_endpoint(
        body: CreateCuratorObservationBatchBody,
        api_key: str = Depends(_api_key),
        correlation_id: str = Depends(_correlation_id),
    ):
        try:
            records = create_curator_observation_batch(
                db,
                body.observations,
                correlation_id=correlation_id,
            )
        except Exception as exc:
            raise HTTPException(status_code=500, detail=str(exc)) from exc
        return _wrap_response(
            data={
                "created_count": len(records),
                "observations": [
                    record.model_dump(mode="json") for record in records
                ],
            },
            correlation_id=correlation_id,
        )

    @router.get("/curator/observations", response_model=dict)
    async def list_curator_observations_endpoint(
        account_id: Optional[str] = None,
        symbol: Optional[str] = None,
        mode: Optional[str] = Query(
            default=None,
            pattern="^(shadow_ensemble|single_skill)$",
        ),
        status: Optional[str] = None,
        limit: int = Query(default=100, ge=1, le=1000),
        offset: int = Query(default=0, ge=0),
        api_key: str = Depends(_api_key),
        correlation_id: str = Depends(_correlation_id),
    ):
        records = list_curator_observations(
            db,
            account_id=account_id,
            symbol=symbol,
            mode=mode,
            status=status,
            limit=limit,
            offset=offset,
        )
        return _wrap_response(
            data=[record.model_dump(mode="json") for record in records],
            correlation_id=correlation_id,
        )

    @router.get("/curator/observations/readiness", response_model=dict)
    async def curator_observation_readiness_endpoint(
        account_id: Optional[str] = None,
        mode: str = Query(
            default="shadow_ensemble",
            pattern="^(shadow_ensemble|single_skill)$",
        ),
        observation_target: int = Query(default=50, ge=1, le=100000),
        api_key: str = Depends(_api_key),
        correlation_id: str = Depends(_correlation_id),
    ):
        readiness = build_curator_observation_readiness(
            db,
            account_id=account_id,
            mode=mode,
            observation_target=observation_target,
        )
        return _wrap_response(
            data=readiness.model_dump(mode="json"),
            correlation_id=correlation_id,
        )

    @router.get("/curator/observations/{observation_id}", response_model=dict)
    async def get_curator_observation_endpoint(
        observation_id: str,
        api_key: str = Depends(_api_key),
        correlation_id: str = Depends(_correlation_id),
    ):
        record = get_curator_observation(db, observation_id)
        if not record:
            raise HTTPException(
                status_code=404,
                detail=f"Curator observation {observation_id} not found",
            )
        return _wrap_response(
            data=record.model_dump(mode="json"),
            correlation_id=correlation_id,
        )

    return router
