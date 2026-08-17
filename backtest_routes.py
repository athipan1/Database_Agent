from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Security
from fastapi.security import APIKeyHeader

from backtest_models import CreateBacktestRunBody, UpsertMarketDataBarsBody
from backtest_repository import (
    get_backtest_run_detail,
    get_latest_exact_backtest_run_detail,
    get_skill_backtest_status,
    list_market_data_bars,
    list_skill_backtests,
    setup_backtest_tables,
    upsert_market_data_bars,
)
from backtest_write_repository import create_backtest_run_detail


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

    # Production schema setup is owned by app.startup.setup_runtime_tables().
    # Unit tests use an in-memory SQLite DB without the application lifespan, so
    # initialize that disposable schema once when the router is constructed.
    if db.db_type == "sqlite":
        setup_backtest_tables(db)

    async def _api_key(api_key_header_value: str = Security(api_key_header)):
        return get_api_key_dependency(api_key_header_value)

    async def _correlation_id():
        return await get_correlation_id_dependency()

    @router.post("/market-data/bars", response_model=dict)
    def upsert_market_data_bars_endpoint(
        body: UpsertMarketDataBarsBody,
        api_key: str = Depends(_api_key),
        correlation_id: str = Depends(_correlation_id),
    ):
        try:
            records = upsert_market_data_bars(db, body.bars)
        except Exception as exc:
            raise HTTPException(status_code=500, detail=str(exc))
        return wrap_response(
            data=[record.model_dump(mode="json") for record in records],
            correlation_id=correlation_id,
            metadata={"write_model": "market_data_bars", "count": len(records)},
        )

    @router.get("/market-data/bars", response_model=dict)
    def list_market_data_bars_endpoint(
        symbol: str,
        timeframe: str = "1d",
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        limit: int = Query(default=5000, ge=1, le=20000),
        api_key: str = Depends(_api_key),
        correlation_id: str = Depends(_correlation_id),
    ):
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
    def create_backtest_run_endpoint(
        body: CreateBacktestRunBody,
        api_key: str = Depends(_api_key),
        correlation_id: str = Depends(_correlation_id),
    ):
        try:
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

    @router.get("/backtests/runs/latest", response_model=dict)
    def get_latest_exact_backtest_run_endpoint(
        skill_id: str = Query(..., min_length=1),
        strategy_id: str = Query(..., min_length=1),
        symbol: str = Query(..., min_length=1),
        timeframe: str = Query(..., min_length=1),
        api_key: str = Depends(_api_key),
        correlation_id: str = Depends(_correlation_id),
    ):
        detail = get_latest_exact_backtest_run_detail(
            db,
            skill_id=skill_id,
            strategy_id=strategy_id,
            symbol=symbol,
            timeframe=timeframe,
        )
        if not detail:
            raise HTTPException(
                status_code=404,
                detail=(
                    "No BacktestRun found for exact identity: "
                    f"skill_id={skill_id.strip()} "
                    f"strategy_id={strategy_id.strip()} "
                    f"symbol={symbol.strip().upper()} "
                    f"timeframe={timeframe.strip()}"
                ),
            )
        return wrap_response(
            data=detail.model_dump(mode="json"),
            correlation_id=correlation_id,
            metadata={
                "lookup": "exact_backtest_identity_v1",
                "exact_match": True,
                "skill_id": skill_id.strip(),
                "strategy_id": strategy_id.strip(),
                "symbol": symbol.strip().upper(),
                "timeframe": timeframe.strip(),
                "safe_for_trading": False,
                "note": "Manager/Risk must still enforce pass status and freshness.",
            },
        )

    @router.get("/backtests/runs/{run_id}", response_model=dict)
    def get_backtest_run_endpoint(
        run_id: str,
        api_key: str = Depends(_api_key),
        correlation_id: str = Depends(_correlation_id),
    ):
        detail = get_backtest_run_detail(db, run_id)
        if not detail:
            raise HTTPException(status_code=404, detail=f"BacktestRun {run_id} not found")
        return wrap_response(data=detail.model_dump(mode="json"), correlation_id=correlation_id)

    @router.get("/skills/{skill_id}/backtests", response_model=dict)
    def list_skill_backtests_endpoint(
        skill_id: str,
        limit: int = Query(default=50, ge=1, le=200),
        api_key: str = Depends(_api_key),
        correlation_id: str = Depends(_correlation_id),
    ):
        records = list_skill_backtests(db, skill_id, limit=limit)
        return wrap_response(
            data=[record.model_dump(mode="json") for record in records],
            correlation_id=correlation_id,
            metadata={"skill_id": skill_id, "safe_for_trading": False},
        )

    @router.get("/skills/{skill_id}/backtest-status", response_model=dict)
    def get_skill_backtest_status_endpoint(
        skill_id: str,
        api_key: str = Depends(_api_key),
        correlation_id: str = Depends(_correlation_id),
    ):
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
