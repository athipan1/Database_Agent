"""Execution persistence, risk approval, fill, and broker-sync routes."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Union

from fastapi import APIRouter, Depends, HTTPException

from models import (
    BrokerSyncBody,
    BrokerSyncResult,
    CreateExecutionJobBody,
    CreateFillBody,
    CreateRiskApprovalBody,
    ExecutionJob,
    FillRecord,
    MarkRiskApprovalUsedBody,
    RiskApproval,
    SessionRiskSnapshot,
    StandardAgentResponse,
)


ROUTE_SIGNATURES = frozenset(
    {
        ("/broker-sync", "POST"),
        ("/risk-approvals", "POST"),
        ("/risk-approvals/{approval_id}", "GET"),
        ("/risk-approvals/{approval_id}/use", "POST"),
        ("/execution-jobs", "POST"),
        ("/execution-jobs/{job_id}", "GET"),
        ("/orders/{order_id}/execution-job", "GET"),
        ("/execution-jobs/claim-next", "POST"),
        ("/execution-jobs/{job_id}", "PATCH"),
        ("/accounts/{account_id}/fills", "POST"),
        ("/accounts/{account_id}/fills", "GET"),
        ("/accounts/{account_id}/risk/session", "GET"),
    }
)


def create_execution_router(runtime: Any) -> APIRouter:
    router = APIRouter(tags=["execution-persistence"])

    @router.post(
        "/broker-sync",
        response_model=StandardAgentResponse[BrokerSyncResult],
    )
    async def broker_sync_endpoint(
        body: BrokerSyncBody,
        api_key: str = Depends(runtime.get_api_key),
        correlation_id: str = Depends(runtime.get_correlation_id),
    ):
        logging.info(
            "Broker sync request for account %s from %s.",
            body.account_id,
            body.broker,
        )
        try:
            result = runtime.sync_broker_state(
                runtime.db,
                body.model_dump(mode="json"),
            )
        except Exception as exc:
            logging.error("Broker sync failed: %s", exc, exc_info=True)
            raise HTTPException(status_code=500, detail=str(exc)) from exc
        return runtime.wrap_response(data=BrokerSyncResult(**result))

    @router.post(
        "/risk-approvals",
        response_model=StandardAgentResponse[RiskApproval],
    )
    async def create_risk_approval_endpoint(
        body: CreateRiskApprovalBody,
        api_key: str = Depends(runtime.get_api_key),
        correlation_id: str = Depends(runtime.get_correlation_id),
    ):
        logging.info(
            "Request to create risk approval %s for %s.",
            body.approval_id,
            body.symbol,
        )
        try:
            record = runtime.create_risk_approval(runtime.db, body)
        except Exception as exc:
            logging.error(
                "Risk approval creation failed: %s",
                exc,
                exc_info=True,
            )
            raise HTTPException(status_code=500, detail=str(exc)) from exc
        return runtime.wrap_response(data=record)

    @router.get(
        "/risk-approvals/{approval_id}",
        response_model=StandardAgentResponse[RiskApproval],
    )
    async def get_risk_approval_endpoint(
        approval_id: str,
        api_key: str = Depends(runtime.get_api_key),
        correlation_id: str = Depends(runtime.get_correlation_id),
    ):
        record = runtime.get_risk_approval(runtime.db, approval_id)
        if not record:
            raise HTTPException(
                status_code=404,
                detail=f"Risk approval {approval_id} not found",
            )
        return runtime.wrap_response(data=record)

    @router.post(
        "/risk-approvals/{approval_id}/use",
        response_model=StandardAgentResponse[RiskApproval],
    )
    async def mark_risk_approval_used_endpoint(
        approval_id: str,
        body: MarkRiskApprovalUsedBody,
        api_key: str = Depends(runtime.get_api_key),
        correlation_id: str = Depends(runtime.get_correlation_id),
    ):
        record = runtime.mark_risk_approval_used(
            runtime.db,
            approval_id,
            body.order_id,
        )
        return runtime.wrap_response(data=record)

    @router.post(
        "/execution-jobs",
        response_model=StandardAgentResponse[ExecutionJob],
    )
    async def create_execution_job_endpoint(
        body: CreateExecutionJobBody,
        api_key: str = Depends(runtime.get_api_key),
        correlation_id: str = Depends(runtime.get_correlation_id),
    ):
        job = runtime.create_execution_job(
            runtime.db,
            body.order_id,
            body.trade_id,
            body.max_attempts,
        )
        return runtime.wrap_response(data=ExecutionJob(**job))

    @router.get(
        "/execution-jobs/{job_id}",
        response_model=StandardAgentResponse[ExecutionJob],
    )
    async def get_execution_job_endpoint(
        job_id: Union[int, str],
        api_key: str = Depends(runtime.get_api_key),
        correlation_id: str = Depends(runtime.get_correlation_id),
    ):
        job = runtime.get_execution_job(runtime.db, job_id)
        if not job:
            raise HTTPException(
                status_code=404,
                detail=f"Execution job {job_id} not found",
            )
        return runtime.wrap_response(data=ExecutionJob(**job))

    @router.get(
        "/orders/{order_id}/execution-job",
        response_model=StandardAgentResponse[ExecutionJob],
    )
    async def get_execution_job_by_order_id_endpoint(
        order_id: int,
        api_key: str = Depends(runtime.get_api_key),
        correlation_id: str = Depends(runtime.get_correlation_id),
    ):
        job = runtime.get_execution_job_by_order_id(runtime.db, order_id)
        if not job:
            raise HTTPException(
                status_code=404,
                detail=f"Execution job for order {order_id} not found",
            )
        return runtime.wrap_response(data=ExecutionJob(**job))

    @router.post(
        "/execution-jobs/claim-next",
        response_model=StandardAgentResponse[Optional[ExecutionJob]],
    )
    async def claim_next_execution_job_endpoint(
        api_key: str = Depends(runtime.get_api_key),
        correlation_id: str = Depends(runtime.get_correlation_id),
    ):
        job = runtime.claim_next_execution_job(runtime.db)
        if not job:
            raise HTTPException(
                status_code=404,
                detail="No queued execution jobs available",
            )
        return runtime.wrap_response(data=ExecutionJob(**job))

    @router.patch(
        "/execution-jobs/{job_id}",
        response_model=StandardAgentResponse[ExecutionJob],
    )
    async def update_execution_job_endpoint(
        job_id: Union[int, str],
        updates: Dict[str, Any],
        api_key: str = Depends(runtime.get_api_key),
        correlation_id: str = Depends(runtime.get_correlation_id),
    ):
        job = runtime.update_execution_job(runtime.db, job_id, updates)
        if not job:
            raise HTTPException(
                status_code=404,
                detail=f"Execution job {job_id} not found",
            )
        return runtime.wrap_response(data=ExecutionJob(**job))

    @router.post(
        "/accounts/{account_id}/fills",
        response_model=StandardAgentResponse[FillRecord],
    )
    async def create_fill_for_account(
        account_id: Union[int, str],
        body: CreateFillBody,
        api_key: str = Depends(runtime.get_api_key),
        correlation_id: str = Depends(runtime.get_correlation_id),
    ):
        logging.info(
            "Request to create fill for account %s, symbol %s.",
            account_id,
            body.symbol,
        )
        try:
            fill = runtime.create_fill_record(
                runtime.db,
                account_id=account_id,
                order_id=body.order_id,
                trade_id=body.trade_id,
                symbol=body.symbol,
                side=(
                    body.side.value
                    if hasattr(body.side, "value")
                    else str(body.side)
                ),
                quantity=body.quantity,
                fill_price=body.fill_price,
                average_entry_price=body.average_entry_price,
                fees=body.fees,
                realized_pnl=body.realized_pnl,
                broker_fill_id=body.broker_fill_id,
                broker_order_id=body.broker_order_id,
                liquidity=body.liquidity,
                filled_at=body.filled_at,
                correlation_id=correlation_id,
                metadata=body.metadata,
            )
        except Exception as exc:
            logging.error("Fill creation failed: %s", exc, exc_info=True)
            raise HTTPException(status_code=500, detail=str(exc)) from exc
        return runtime.wrap_response(data=FillRecord(**fill))

    @router.get(
        "/accounts/{account_id}/fills",
        response_model=StandardAgentResponse[List[FillRecord]],
    )
    async def get_fills_for_account(
        account_id: Union[int, str],
        symbol: Optional[str] = None,
        limit: int = 100,
        api_key: str = Depends(runtime.get_api_key),
        correlation_id: str = Depends(runtime.get_correlation_id),
    ):
        logging.info(
            "Request to get fills for account %s, symbol=%s.",
            account_id,
            symbol,
        )
        try:
            fills = runtime.get_fill_records(
                runtime.db,
                account_id,
                symbol=symbol,
                limit=limit,
            )
        except Exception as exc:
            logging.warning("Fill lookup failed for %s: %s", account_id, exc)
            fills = []
        return runtime.wrap_response(data=fills or [])

    @router.get(
        "/accounts/{account_id}/risk/session",
        response_model=StandardAgentResponse[SessionRiskSnapshot],
    )
    async def get_session_risk_snapshot_endpoint(
        account_id: Union[int, str],
        symbol: Optional[str] = None,
        api_key: str = Depends(runtime.get_api_key),
        correlation_id: str = Depends(runtime.get_correlation_id),
    ):
        logging.info(
            "Request to get session risk snapshot for account %s, symbol=%s.",
            account_id,
            symbol,
        )
        try:
            snapshot = runtime.build_session_risk_snapshot(
                runtime.db,
                account_id,
                symbol=symbol,
                emergency_halt=runtime.DATABASE_EMERGENCY_HALT,
            )
        except Exception as exc:
            logging.error(
                "Session risk snapshot failed for account %s: %s",
                account_id,
                exc,
                exc_info=True,
            )
            if runtime.TRADING_MODE == "LIVE":
                raise HTTPException(status_code=500, detail=str(exc)) from exc
            snapshot = {
                "account_id": account_id,
                "symbol": symbol.upper() if symbol else None,
                "daily_realized_pnl": 0.0,
                "weekly_realized_pnl": 0.0,
                "consecutive_losses": 0,
                "trades_today": 0,
                "symbol_trades_today": 0,
                "minutes_since_last_loss": None,
                "minutes_since_last_symbol_trade": None,
                "emergency_halt": runtime.DATABASE_EMERGENCY_HALT,
                "source": "database_agent_fallback",
                "generated_at": datetime.now(timezone.utc).isoformat(),
            }
        return runtime.wrap_response(data=SessionRiskSnapshot(**snapshot))

    return router
