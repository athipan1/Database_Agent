from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Security
from fastapi.security import APIKeyHeader

from skill_performance_models import CreateSkillExecutionLogBody, CreateSkillTradeOutcomeBody
from skill_performance_repository import (
    create_skill_execution_log,
    create_skill_trade_outcome,
    list_skill_execution_logs,
    rank_skill_performance,
)


def wrap_response(
    data: Any = None,
    status: str = "success",
    error: Optional[dict] = None,
    correlation_id: Optional[str] = None,
    metadata: Optional[dict] = None,
):
    return {
        "status": status,
        "agent_type": "database",
        "version": "1.1.0",
        "schema_version": "1.0",
        "timestamp": datetime.now(timezone.utc),
        "correlation_id": correlation_id,
        "data": data,
        "metadata": metadata or {},
        "error": error,
        "confidence_score": None,
    }


def create_skill_performance_routes(db, get_api_key_dependency, get_correlation_id_dependency):
    router = APIRouter(prefix="/skills", tags=["skill-performance"])
    api_key_header = APIKeyHeader(name="X-API-KEY", auto_error=False)

    async def _api_key(api_key_header_value: str = Security(api_key_header)):
        return get_api_key_dependency(api_key_header_value)

    async def _correlation_id():
        return await get_correlation_id_dependency()

    @router.post("/execution-logs", response_model=dict)
    async def create_skill_execution_log_endpoint(
        body: CreateSkillExecutionLogBody,
        api_key: str = Depends(_api_key),
        correlation_id: str = Depends(_correlation_id),
    ):
        try:
            metadata = dict(body.metadata or {})
            metadata.setdefault("correlation_id", correlation_id)
            body.metadata = metadata
            record = create_skill_execution_log(db, body)
        except Exception as exc:
            raise HTTPException(status_code=500, detail=str(exc))
        return wrap_response(
            data=record.model_dump(mode="json"),
            correlation_id=correlation_id,
            metadata={"write_model": "skill_execution_logs"},
        )

    @router.get("/execution-logs", response_model=dict)
    async def list_skill_execution_logs_endpoint(
        skill_id: Optional[str] = None,
        account_id: Optional[str] = None,
        symbol: Optional[str] = None,
        strategy_bucket: Optional[str] = None,
        market_regime: Optional[str] = None,
        limit: int = Query(default=100, ge=1, le=500),
        offset: int = Query(default=0, ge=0),
        api_key: str = Depends(_api_key),
        correlation_id: str = Depends(_correlation_id),
    ):
        records = list_skill_execution_logs(
            db,
            skill_id=skill_id,
            account_id=account_id,
            symbol=symbol,
            strategy_bucket=strategy_bucket,
            market_regime=market_regime,
            limit=limit,
            offset=offset,
        )
        return wrap_response(
            data=[record.model_dump(mode="json") for record in records],
            correlation_id=correlation_id,
        )

    @router.post("/trade-outcomes", response_model=dict)
    async def create_skill_trade_outcome_endpoint(
        body: CreateSkillTradeOutcomeBody,
        api_key: str = Depends(_api_key),
        correlation_id: str = Depends(_correlation_id),
    ):
        try:
            metadata = dict(body.metadata or {})
            metadata.setdefault("correlation_id", correlation_id)
            body.metadata = metadata
            record = create_skill_trade_outcome(db, body)
        except Exception as exc:
            raise HTTPException(status_code=500, detail=str(exc))
        return wrap_response(
            data=record.model_dump(mode="json"),
            correlation_id=correlation_id,
            metadata={"write_model": "skill_trade_outcomes"},
        )

    @router.get("/performance/rank", response_model=dict)
    async def rank_skill_performance_endpoint(
        account_id: Optional[str] = None,
        symbol: Optional[str] = None,
        strategy_bucket: Optional[str] = None,
        market_regime: Optional[str] = None,
        limit: int = Query(default=20, ge=1, le=100),
        api_key: str = Depends(_api_key),
        correlation_id: str = Depends(_correlation_id),
    ):
        records = rank_skill_performance(
            db,
            account_id=account_id,
            symbol=symbol,
            strategy_bucket=strategy_bucket,
            market_regime=market_regime,
            limit=limit,
        )
        return wrap_response(
            data=[record.model_dump(mode="json") for record in records],
            correlation_id=correlation_id,
            metadata={
                "ranking_model": "conservative_v1",
                "safe_for_trading": False,
                "note": "Ranking is advisory only; Risk_Agent and Execution_Agent remain final gates.",
            },
        )

    @router.get("/{skill_id}/performance", response_model=dict)
    async def get_skill_performance_endpoint(
        skill_id: str,
        account_id: Optional[str] = None,
        symbol: Optional[str] = None,
        strategy_bucket: Optional[str] = None,
        market_regime: Optional[str] = None,
        api_key: str = Depends(_api_key),
        correlation_id: str = Depends(_correlation_id),
    ):
        records = rank_skill_performance(
            db,
            account_id=account_id,
            symbol=symbol,
            strategy_bucket=strategy_bucket,
            market_regime=market_regime,
            limit=100,
        )
        matched = [record for record in records if record.skill_id == skill_id]
        if not matched:
            return wrap_response(
                data=None,
                correlation_id=correlation_id,
                metadata={"skill_id": skill_id, "state": "no_performance_data"},
            )
        return wrap_response(data=matched[0].model_dump(mode="json"), correlation_id=correlation_id)

    return router
