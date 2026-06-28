import logging
from datetime import datetime, timezone
from typing import Any, Optional

from fastapi import APIRouter, Depends, HTTPException, Security
from fastapi.security import APIKeyHeader

from plan_record_repository import create_plan_record, get_plan_record, update_plan_record_status
from trade_plan_models import CreateTradePlanBody, TradePlanRecord, UpdateTradePlanStatusBody

router = APIRouter()


def wrap_response(data: Any = None, status: str = "success", error: Optional[dict] = None):
    return {
        "status": status,
        "agent_type": "database",
        "version": "1.1.0",
        "timestamp": datetime.now(timezone.utc),
        "data": data,
        "error": error,
        "confidence_score": None,
    }


def create_plan_record_routes(db, get_api_key_dependency, get_correlation_id_dependency):
    api_key_header = APIKeyHeader(name="X-API-KEY", auto_error=False)

    async def _api_key(api_key_header_value: str = Security(api_key_header)):
        return get_api_key_dependency(api_key_header_value)

    async def _correlation_id():
        return await get_correlation_id_dependency()

    @router.post("/trade-plans", response_model=dict)
    async def create_trade_plan_endpoint(
        body: CreateTradePlanBody,
        api_key: str = Depends(_api_key),
        correlation_id: str = Depends(_correlation_id),
    ):
        logging.info(f"Request to create TradePlan {body.trade_plan_id} for {body.symbol}.")
        try:
            record = create_plan_record(db, body)
        except Exception as exc:
            logging.error(f"TradePlan creation failed: {exc}", exc_info=True)
            raise HTTPException(status_code=500, detail=str(exc))
        return wrap_response(data=record)

    @router.get("/trade-plans/{trade_plan_id}", response_model=dict)
    async def get_trade_plan_endpoint(
        trade_plan_id: str,
        api_key: str = Depends(_api_key),
        correlation_id: str = Depends(_correlation_id),
    ):
        record = get_plan_record(db, trade_plan_id)
        if not record:
            raise HTTPException(status_code=404, detail=f"TradePlan {trade_plan_id} not found")
        return wrap_response(data=record)

    @router.post("/trade-plans/{trade_plan_id}/status", response_model=dict)
    async def update_trade_plan_status_endpoint(
        trade_plan_id: str,
        body: UpdateTradePlanStatusBody,
        api_key: str = Depends(_api_key),
        correlation_id: str = Depends(_correlation_id),
    ):
        logging.info(f"Request to update TradePlan {trade_plan_id} to {body.status}.")
        try:
            record = update_plan_record_status(db, trade_plan_id, body)
        except HTTPException:
            raise
        except Exception as exc:
            logging.error(f"TradePlan status update failed: {exc}", exc_info=True)
            raise HTTPException(status_code=500, detail=str(exc))
        return wrap_response(data=record)

    return router
