from datetime import datetime, timezone
from typing import Any, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Security
from fastapi.security import APIKeyHeader

from order_review_ticket_routes import create_order_review_ticket_routes
from policy_review_models import CreatePolicyReviewAuditBody, ListPolicyReviewAuditsQuery
from policy_review_repository import create_policy_review_audit, get_policy_review_audit, list_policy_review_audits
from review_history_routes import create_review_history_routes
from system_contract_routes import create_system_contract_routes


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


def create_policy_review_routes(db, get_api_key_dependency, get_correlation_id_dependency):
    router = APIRouter()
    api_key_header = APIKeyHeader(name="X-API-KEY", auto_error=False)

    async def _api_key(api_key_header_value: str = Security(api_key_header)):
        return get_api_key_dependency(api_key_header_value)

    async def _correlation_id():
        return await get_correlation_id_dependency()

    router.include_router(
        create_system_contract_routes(
            trading_mode=getattr(db, "trading_mode", "PAPER"),
            database_dev_mode=False,
            database_emergency_halt=False,
            database_agent_api_key_configured=True,
            get_correlation_id_dependency=get_correlation_id_dependency,
        )
    )

    @router.post("/policy-reviews", response_model=dict)
    async def create_policy_review_endpoint(
        body: CreatePolicyReviewAuditBody,
        api_key: str = Depends(_api_key),
        correlation_id: str = Depends(_correlation_id),
    ):
        try:
            record = create_policy_review_audit(db, body)
        except Exception as exc:
            raise HTTPException(status_code=500, detail=str(exc))
        return wrap_response(data=record, correlation_id=correlation_id)

    @router.get("/policy-reviews", response_model=dict)
    async def list_policy_reviews_endpoint(
        account_id: Optional[str] = None,
        symbol: Optional[str] = None,
        status: Optional[str] = None,
        source: Optional[str] = None,
        advisory_only: Optional[bool] = None,
        auto_apply: Optional[bool] = None,
        limit: int = Query(default=100, ge=1, le=500),
        offset: int = Query(default=0, ge=0),
        sort: str = Query(default="updated_at", pattern="^(created_at|updated_at)$"),
        order: str = Query(default="desc", pattern="^(asc|desc)$"),
        api_key: str = Depends(_api_key),
        correlation_id: str = Depends(_correlation_id),
    ):
        query = ListPolicyReviewAuditsQuery(
            account_id=account_id,
            symbol=symbol,
            status=status,
            source=source,
            advisory_only=advisory_only,
            auto_apply=auto_apply,
            limit=limit,
            offset=offset,
            sort=sort,
            order=order,
        )
        records = list_policy_review_audits(db, query)
        return wrap_response(data=records, correlation_id=correlation_id)

    @router.get("/policy-reviews/{policy_review_id}", response_model=dict)
    async def get_policy_review_endpoint(
        policy_review_id: str,
        api_key: str = Depends(_api_key),
        correlation_id: str = Depends(_correlation_id),
    ):
        record = get_policy_review_audit(db, policy_review_id)
        if not record:
            raise HTTPException(status_code=404, detail=f"PolicyReview {policy_review_id} not found")
        return wrap_response(data=record, correlation_id=correlation_id)

    router.include_router(create_review_history_routes(db, get_api_key_dependency, get_correlation_id_dependency))
    router.include_router(create_order_review_ticket_routes(db, get_api_key_dependency, get_correlation_id_dependency))
    return router
