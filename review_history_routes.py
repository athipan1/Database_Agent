import logging
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Security
from fastapi.security import APIKeyHeader
from pydantic import BaseModel, Field

from review_history_repository import (
    create_review_history,
    get_latest_review_history_summary,
    get_review_history,
    list_review_history,
)

router = APIRouter()


class CreateReviewHistoryBody(BaseModel):
    account_id: str | int = 1
    review_run_id: Optional[str] = None
    bucket: Optional[str] = None
    source: str = "manager-agent"
    status: str = "completed"
    report: Dict[str, Any] = Field(default_factory=dict)


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


def create_review_history_routes(db, get_api_key_dependency, get_correlation_id_dependency):
    api_key_header = APIKeyHeader(name="X-API-KEY", auto_error=False)

    async def _api_key(api_key_header_value: str = Security(api_key_header)):
        return get_api_key_dependency(api_key_header_value)

    async def _correlation_id():
        return await get_correlation_id_dependency()

    @router.post("/review-history", response_model=dict)
    async def create_review_history_endpoint(
        body: CreateReviewHistoryBody,
        api_key: str = Depends(_api_key),
        correlation_id: str = Depends(_correlation_id),
    ):
        logging.info(f"Request to store review history for bucket={body.bucket}.")
        try:
            record = create_review_history(db, body.model_dump(mode="json"), correlation_id=correlation_id)
        except Exception as exc:
            logging.error(f"Review history creation failed: {exc}", exc_info=True)
            raise HTTPException(status_code=500, detail=str(exc))
        return wrap_response(data=record)

    @router.get("/review-history", response_model=dict)
    async def list_review_history_endpoint(
        account_id: Optional[str] = None,
        bucket: Optional[str] = None,
        limit: int = Query(default=100, ge=1, le=500),
        api_key: str = Depends(_api_key),
        correlation_id: str = Depends(_correlation_id),
    ):
        records = list_review_history(db, account_id=account_id, bucket=bucket, limit=limit)
        return wrap_response(data=records)

    @router.get("/review-history/latest", response_model=dict)
    async def get_latest_review_history_summary_endpoint(
        account_id: Optional[str] = None,
        bucket: Optional[str] = None,
        api_key: str = Depends(_api_key),
        correlation_id: str = Depends(_correlation_id),
    ):
        record = get_latest_review_history_summary(db, account_id=account_id, bucket=bucket)
        if not record:
            raise HTTPException(status_code=404, detail="Latest review history summary not found")
        return wrap_response(data=record)

    @router.get("/review-history/{review_run_id}", response_model=dict)
    async def get_review_history_endpoint(
        review_run_id: str,
        api_key: str = Depends(_api_key),
        correlation_id: str = Depends(_correlation_id),
    ):
        record = get_review_history(db, review_run_id)
        if not record:
            raise HTTPException(status_code=404, detail=f"Review history {review_run_id} not found")
        return wrap_response(data=record)

    return router
