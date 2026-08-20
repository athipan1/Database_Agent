from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Security
from fastapi.security import APIKeyHeader

from shadow_observation_models import CreateShadowObservationBody
from shadow_observation_repository import (
    create_shadow_observation,
    get_shadow_observation,
    list_shadow_observations,
)


def _wrap_response(*, data: Any = None, correlation_id: Optional[str] = None):
    return {
        "status": "success",
        "agent_type": "database",
        "version": "1.1.0",
        "schema_version": "1.0",
        "timestamp": datetime.now(timezone.utc),
        "correlation_id": correlation_id,
        "data": data,
        "metadata": {
            "contract": "database-shadow-observation.v1",
            "append_only": True,
            "broker_mutation": False,
        },
        "error": None,
        "confidence_score": None,
    }


def create_shadow_observation_routes(
    db,
    get_api_key_dependency,
    get_correlation_id_dependency,
) -> APIRouter:
    router = APIRouter(tags=["shadow-observations"])
    api_key_header = APIKeyHeader(name="X-API-KEY", auto_error=False)

    async def _api_key(api_key_header_value: str = Security(api_key_header)):
        return get_api_key_dependency(api_key_header_value)

    async def _correlation_id():
        return await get_correlation_id_dependency()

    @router.post("/shadow/observations", response_model=dict)
    async def create_shadow_observation_endpoint(
        body: CreateShadowObservationBody,
        api_key: str = Depends(_api_key),
        correlation_id: str = Depends(_correlation_id),
    ):
        try:
            record = create_shadow_observation(
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

    @router.get("/shadow/observations", response_model=dict)
    async def list_shadow_observations_endpoint(
        account_id: Optional[str] = None,
        shadow_trade_id: Optional[str] = None,
        symbol: Optional[str] = None,
        event_type: Optional[str] = Query(
            default=None,
            pattern="^(signal_decision|entry_simulated|mark|exit_simulated)$",
        ),
        limit: int = Query(default=100, ge=1, le=1000),
        offset: int = Query(default=0, ge=0),
        api_key: str = Depends(_api_key),
        correlation_id: str = Depends(_correlation_id),
    ):
        records = list_shadow_observations(
            db,
            account_id=account_id,
            shadow_trade_id=shadow_trade_id,
            symbol=symbol,
            event_type=event_type,
            limit=limit,
            offset=offset,
        )
        return _wrap_response(
            data=[record.model_dump(mode="json") for record in records],
            correlation_id=correlation_id,
        )

    @router.get("/shadow/observations/{event_id}", response_model=dict)
    async def get_shadow_observation_endpoint(
        event_id: str,
        api_key: str = Depends(_api_key),
        correlation_id: str = Depends(_correlation_id),
    ):
        record = get_shadow_observation(db, event_id)
        if not record:
            raise HTTPException(status_code=404, detail="Shadow observation not found")
        return _wrap_response(
            data=record.model_dump(mode="json"),
            correlation_id=correlation_id,
        )

    return router
