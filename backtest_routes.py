from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Security
from fastapi.security import APIKeyHeader

from backtest_models import CreateBacktestRunBody, UpsertMarketDataBarsBody
from backtest_repository import (
    create_backtest_run_detail,
    get_backtest_run_detail,
    get_skill_backtest_status,
    list_market_data_bars,
    list_skill_backtests,
    setup_backtest_tables,
    upsert_market_data_bars,
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


def create_backtest_routes(db, get_api_key_dependency, get_correlation_id_dependency):
    router = APIRouter(tags=["backtests"])
    api_key_header = APIKeyHeader(name="X-API-KEY", auto_error=False)

    async def _api_key(api_key_header_value: str = Security(api_key_header)):
        return get_api_key_dependency(api_key_header_value)

    async def _correlation_id():
        return await get_correlation_id_dependency()

    def _ensure_tables_ready() -> None:
        setup_backtest_tables(db)

    @router.post("/market-data/bars", response_model=dict)
    async def upsert_market_data_bars_endpoint(
        body: UpsertMarketDataBarsBody,
        api_key: str = Depends(_api_key),
        correlation_id: str = Depends(_correlation_id),
    ):
        try:
            _ensure_tables_ready()
            records = upsert_market_data_bars(db, body.bars)
        except Exception as exc:
            raise HTTPException(status_code=500, detail=str(exc))
        return wrap_response(
            data=[record.model_dump(mode="json") for record in records],
            correlation_id=correlation_id,
            metadata={"write_model": "market_data_bars", "count": len(records)},
        )

    @router.get("/market-data/bars", response_model=dict)
    async def list_market_data_bars_endpoint(
        symbol: str,
        timeframe: str = "1d",
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        limit: int = Query(default=5000, ge=1, le=20000),
        api_key: str = Depends(_api_key),
        correlation_id: str = Depends(_correlation_id),
    ):
        _ensure_tables_ready()
        records = list_market_data_bars(
            db,
            symbol=symbol,
            timeframe=timeframe,
            start_time=start_time,
            end_time=end_time,
            limit=limit,
        )
        return wrap_response(
            data=[record.model_dump(mode="json") for record in records],
            correlation_id=correlation_id,
            metadata={"source": "market_data_bars"},
        )

    @router.post("/backtests/runs", response_model=dict)
    async def create_backtest_run_endpoint(
        body: CreateBacktestRunBody,
        api_key: str = Depends(_api_key),
        correlation_id: str = Depends(_correlation_id),
    ):
        try:
            _ensure_tables_ready()
            metadata = dict(body.metadata or {})
            metadata.setdefault("correlation_id", correlation_id)
            body.metadata = metadata
            detail = create_backtest_run_detail(db, body)
        except Exception as exc:
            raise HTTPException(status_code=500, detail=str(exc))
        return wrap_response(
            data=detail.model_dump(mode="json"),
            correlation_id=correlation_id,
            metadata={"write_model": "backtest_runs", "safe_for_trading": False},
        )

    @router.get("/backtests/runs/{run_id}", response_model=dict)
    async def get_backtest_run_endpoint(
        run_id: str,
        api_key: str = Depends(_api_key),
        correlation_id: str = Depends(_correlation_id),
    ):
        _ensure_tables_ready()
        detail = get_backtest_run_detail(db, run_id)
        if not detail:
            raise HTTPException(status_code=404, detail=f"BacktestRun {run_id} not found")
        return wrap_response(data=detail.model_dump(mode="json"), correlation_id=correlation_id)

    @router.get("/skills/{skill_id}/backtests", response_model=dict)
    async def list_skill_backtests_endpoint(
        skill_id: str,
        limit: int = Query(default=50, ge=1, le=200),
        api_key: str = Depends(_api_key),
        correlation_id: str = Depends(_correlation_id),
    ):
        _ensure_tables_ready()
        records = list_skill_backtests(db, skill_id, limit=limit)
        return wrap_response(
            data=[record.model_dump(mode="json") for record in records],
            correlation_id=correlation_id,
            metadata={"skill_id": skill_id, "safe_for_trading": False},
        )

    @router.get("/skills/{skill_id}/backtest-status", response_model=dict)
    async def get_skill_backtest_status_endpoint(
        skill_id: str,
        api_key: str = Depends(_api_key),
        correlation_id: str = Depends(_correlation_id),
    ):
        _ensure_tables_ready()
        status = get_skill_backtest_status(db, skill_id)
        return wrap_response(
            data=status.model_dump(mode="json"),
            correlation_id=correlation_id,
            metadata={
                "gate": "backtest_foundation_v1",
                "safe_for_trading": False,
                "note": "Backtest status is advisory until Manager/Risk enforce it explicitly.",
            },
        )

    return router
