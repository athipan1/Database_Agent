import datetime
from enum import Enum
from typing import Any, Dict, List, Literal, Optional, Union

from pydantic import BaseModel, ConfigDict, Field

from models import OrderSide, StrategyBucket


class TradePlanLifecycleStatus(str, Enum):
    CREATED = "created"
    RISK_PENDING = "risk_pending"
    RISK_APPROVED = "risk_approved"
    EXECUTION_SUBMITTED = "execution_submitted"
    QUEUED = "queued"
    PLACED = "placed"
    PARTIALLY_FILLED = "partially_filled"
    FILLED = "filled"
    REJECTED = "rejected"
    CANCELLED = "cancelled"
    CLOSED = "closed"


class TradePlanBaseModel(BaseModel):
    model_config = ConfigDict(from_attributes=True, populate_by_name=True)


class TradePlanRecord(TradePlanBaseModel):
    trade_plan_id: str
    account_id: Union[int, str]
    symbol: str
    side: OrderSide
    status: TradePlanLifecycleStatus = TradePlanLifecycleStatus.CREATED
    correlation_id: Optional[str] = None
    source: str = "manager-agent"
    strategy: str = "unassigned"
    strategy_bucket: StrategyBucket = "unassigned"
    risk_approval_id: Optional[str] = None
    order_id: Optional[int] = None
    execution_job_id: Optional[Union[int, str]] = None
    broker_order_id: Optional[str] = None
    plan: Dict[str, Any] = Field(default_factory=dict)
    lifecycle: List[Dict[str, Any]] = Field(default_factory=list)
    metadata: Dict[str, Any] = Field(default_factory=dict)
    created_at: Optional[datetime.datetime] = None
    updated_at: Optional[datetime.datetime] = None


class CreateTradePlanBody(TradePlanBaseModel):
    trade_plan_id: str
    account_id: Union[int, str]
    symbol: str
    side: OrderSide
    status: TradePlanLifecycleStatus = TradePlanLifecycleStatus.CREATED
    correlation_id: Optional[str] = None
    source: str = "manager-agent"
    strategy: str = "unassigned"
    strategy_bucket: StrategyBucket = "unassigned"
    risk_approval_id: Optional[str] = None
    order_id: Optional[int] = None
    execution_job_id: Optional[Union[int, str]] = None
    broker_order_id: Optional[str] = None
    plan: Dict[str, Any] = Field(default_factory=dict)
    metadata: Dict[str, Any] = Field(default_factory=dict)


class UpdateTradePlanStatusBody(TradePlanBaseModel):
    status: TradePlanLifecycleStatus
    reason: Optional[str] = None
    risk_approval_id: Optional[str] = None
    order_id: Optional[int] = None
    execution_job_id: Optional[Union[int, str]] = None
    broker_order_id: Optional[str] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)
