from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Literal, Optional, Union

from pydantic import BaseModel, ConfigDict, Field


class OrderReviewTicketBaseModel(BaseModel):
    model_config = ConfigDict(use_enum_values=True)


class OrderReviewTicketRecord(OrderReviewTicketBaseModel):
    ticket_id: str
    account_id: Union[int, str]
    correlation_id: Optional[str] = None
    source: str = "manager-agent"
    mode: str = "manual_approval_ticket"
    safety: str = "read_only_no_orders_submitted_no_orders_cancelled"
    status: Literal["created", "ready_for_manual_approval", "blocked", "executed", "rejected"] = "created"
    approval_required: bool = True
    execution_enabled: bool = False
    manual_confirmation_phrase: Optional[str] = None
    requested_symbols: List[str] = Field(default_factory=list)
    ready_count: int = 0
    blocked_count: int = 0
    orders_submitted: bool = False
    orders_cancelled: bool = False
    ticket_payload: Dict[str, Any] = Field(default_factory=dict)
    metadata: Dict[str, Any] = Field(default_factory=dict)
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


class CreateOrderReviewTicketBody(OrderReviewTicketBaseModel):
    ticket_id: Optional[str] = None
    account_id: Union[int, str]
    correlation_id: Optional[str] = None
    source: str = "manager-agent"
    mode: Optional[str] = None
    safety: Optional[str] = None
    status: Optional[str] = None
    approval_required: Optional[bool] = None
    execution_enabled: Optional[bool] = None
    manual_confirmation_phrase: Optional[str] = None
    requested_symbols: List[str] = Field(default_factory=list)
    ready_count: Optional[int] = None
    blocked_count: Optional[int] = None
    orders_submitted: Optional[bool] = None
    orders_cancelled: Optional[bool] = None
    ticket_payload: Dict[str, Any] = Field(default_factory=dict)
    metadata: Dict[str, Any] = Field(default_factory=dict)


class ListOrderReviewTicketsQuery(OrderReviewTicketBaseModel):
    account_id: Optional[Union[int, str]] = None
    ticket_id: Optional[str] = None
    status: Optional[str] = None
    source: Optional[str] = None
    approval_required: Optional[bool] = None
    execution_enabled: Optional[bool] = None
    limit: int = Field(default=100, ge=1, le=500)
    offset: int = Field(default=0, ge=0)
    sort: Literal["created_at", "updated_at"] = "updated_at"
    order: Literal["asc", "desc"] = "desc"


def now_utc() -> datetime:
    return datetime.now(timezone.utc)
