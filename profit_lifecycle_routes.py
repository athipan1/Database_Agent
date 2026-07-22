from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Optional

from fastapi import APIRouter, Depends, HTTPException, Security
from fastapi.security import APIKeyHeader

from profit_lifecycle_models import (
    ReserveProfitDecisionBody,
    TransitionProfitDecisionBody,
)
from profit_lifecycle_repository import (
    InvalidProfitDecisionTransition,
    ProfitDecisionNotFound,
    ProfitLifecycleNotFound,
    StalePositionVersion,
    get_profit_decision,
    get_profit_lifecycle,
    list_profit_lifecycles,
    reserve_profit_decision,
    transition_profit_decision,
)


def _wrap(
    data: Any,
    *,
    correlation_id: Optional[str],
    metadata: Optional[dict] = None,
):
    return {
        "status": "success",
        "agent_type": "database",
        "version": "1.1.0",
        "schema_version": "profit-lifecycle.v1",
        "timestamp": datetime.now(timezone.utc),
        "correlation_id": correlation_id,
        "data": data,
        "metadata": metadata or {},
        "error": None,
        "confidence_score": None,
    }


def create_profit_lifecycle_routes(
    db,
    get_api_key_dependency,
    get_correlation_id_dependency,
):
    router = APIRouter()
    api_key_header = APIKeyHeader(name="X-API-KEY", auto_error=False)

    async def _api_key(api_key_header_value: str = Security(api_key_header)):
        return get_api_key_dependency(api_key_header_value)

    async def _correlation_id():
        return await get_correlation_id_dependency()

    @router.get("/accounts/{account_id}/profit-lifecycles", response_model=dict)
    async def list_profit_lifecycles_endpoint(
        account_id: str,
        api_key: str = Depends(_api_key),
        correlation_id: str = Depends(_correlation_id),
    ):
        return _wrap(
            list_profit_lifecycles(db, account_id),
            correlation_id=correlation_id,
        )

    @router.get(
        "/accounts/{account_id}/profit-lifecycles/{position_id:path}",
        response_model=dict,
    )
    async def get_profit_lifecycle_endpoint(
        account_id: str,
        position_id: str,
        api_key: str = Depends(_api_key),
        correlation_id: str = Depends(_correlation_id),
    ):
        try:
            lifecycle = get_profit_lifecycle(db, account_id, position_id)
        except ProfitLifecycleNotFound as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        return _wrap(lifecycle, correlation_id=correlation_id)

    @router.post(
        "/accounts/{account_id}/profit-decisions/reserve",
        response_model=dict,
    )
    async def reserve_profit_decision_endpoint(
        account_id: str,
        body: ReserveProfitDecisionBody,
        api_key: str = Depends(_api_key),
        correlation_id: str = Depends(_correlation_id),
    ):
        try:
            decision = reserve_profit_decision(
                db, account_id, body, correlation_id
            )
        except ProfitLifecycleNotFound as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except StalePositionVersion as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        return _wrap(
            decision,
            correlation_id=correlation_id,
            metadata={"duplicate": bool(decision.get("duplicate"))},
        )

    @router.get(
        "/accounts/{account_id}/profit-decisions/{decision_id:path}",
        response_model=dict,
    )
    async def get_profit_decision_endpoint(
        account_id: str,
        decision_id: str,
        api_key: str = Depends(_api_key),
        correlation_id: str = Depends(_correlation_id),
    ):
        try:
            decision = get_profit_decision(db, account_id, decision_id)
        except ProfitDecisionNotFound as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        return _wrap(decision, correlation_id=correlation_id)

    @router.post(
        "/accounts/{account_id}/profit-decisions/{decision_id:path}/transition",
        response_model=dict,
    )
    async def transition_profit_decision_endpoint(
        account_id: str,
        decision_id: str,
        body: TransitionProfitDecisionBody,
        api_key: str = Depends(_api_key),
        correlation_id: str = Depends(_correlation_id),
    ):
        try:
            decision = transition_profit_decision(
                db, account_id, decision_id, body, correlation_id
            )
        except (ProfitLifecycleNotFound, ProfitDecisionNotFound) as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except (StalePositionVersion, InvalidProfitDecisionTransition) as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        return _wrap(
            decision,
            correlation_id=correlation_id,
            metadata={"duplicate": bool(decision.get("duplicate"))},
        )

    return router
