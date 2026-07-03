from datetime import datetime, timezone
from typing import Any, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Security
from fastapi.security import APIKeyHeader

from order_review_ticket_models import CreateOrderReviewTicketBody, ListOrderReviewTicketsQuery
from order_review_ticket_repository import (
    create_order_review_ticket_audit,
    get_order_review_ticket_audit,
    list_order_review_ticket_audits,
)


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


def create_order_review_ticket_routes(db, get_api_key_dependency, get_correlation_id_dependency):
    router = APIRouter()
    api_key_header = APIKeyHeader(name="X-API-KEY", auto_error=False)

    async def _api_key(api_key_header_value: str = Security(api_key_header)):
        return get_api_key_dependency(api_key_header_value)

    async def _correlation_id():
        return await get_correlation_id_dependency()

    @router.post("/order-review-tickets", response_model=dict)
    async def create_order_review_ticket_endpoint(
        body: CreateOrderReviewTicketBody,
        api_key: str = Depends(_api_key),
        correlation_id: str = Depends(_correlation_id),
    ):
        try:
            if body.correlation_id is None:
                body.correlation_id = correlation_id
            record = create_order_review_ticket_audit(db, body)
        except Exception as exc:
            raise HTTPException(status_code=500, detail=str(exc))
        return wrap_response(data=record)

    @router.get("/order-review-tickets", response_model=dict)
    async def list_order_review_tickets_endpoint(
        account_id: Optional[str] = None,
        ticket_id: Optional[str] = None,
        status: Optional[str] = None,
        source: Optional[str] = None,
        approval_required: Optional[bool] = None,
        execution_enabled: Optional[bool] = None,
        limit: int = Query(default=100, ge=1, le=500),
        offset: int = Query(default=0, ge=0),
        sort: str = Query(default="updated_at", pattern="^(created_at|updated_at)$"),
        order: str = Query(default="desc", pattern="^(asc|desc)$"),
        api_key: str = Depends(_api_key),
        correlation_id: str = Depends(_correlation_id),
    ):
        query = ListOrderReviewTicketsQuery(
            account_id=account_id,
            ticket_id=ticket_id,
            status=status,
            source=source,
            approval_required=approval_required,
            execution_enabled=execution_enabled,
            limit=limit,
            offset=offset,
            sort=sort,
            order=order,
        )
        records = list_order_review_ticket_audits(db, query)
        return wrap_response(data=records)

    @router.get("/order-review-tickets/{ticket_id}", response_model=dict)
    async def get_order_review_ticket_endpoint(
        ticket_id: str,
        api_key: str = Depends(_api_key),
        correlation_id: str = Depends(_correlation_id),
    ):
        record = get_order_review_ticket_audit(db, ticket_id)
        if not record:
            raise HTTPException(status_code=404, detail=f"OrderReviewTicket {ticket_id} not found")
        return wrap_response(data=record)

    return router
