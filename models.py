from pydantic import BaseModel, Field, ConfigDict, field_validator

from typing import Literal, Optional, Any, TypeVar, Generic, List, Union, Dict

from decimal import Decimal

from uuid import UUID

from enum import Enum

import datetime

DEFAULT_SCHEMA_VERSION = "1.0"


class OrderSide(str, Enum):

    BUY = "buy"

    SELL = "sell"

class OrderType(str, Enum):

    MARKET = "market"

    LIMIT = "limit"

class TimeInForce(str, Enum):

    GTC = "GTC"  # Good 'til Canceled

    IOC = "IOC"  # Immediate or Cancel

    FOK = "FOK"  # Fill or Kill

class OrderStatus(str, Enum):

    PENDING = "pending"

    PLACED = "placed"

    PARTIALLY_FILLED = "partially_filled"

    EXECUTED = "executed"

    FAILED = "failed"

    CANCELLED = "cancelled"

class ExecutionJobStatus(str, Enum):

    QUEUED = "queued"

    RUNNING = "running"

    SUCCEEDED = "succeeded"

    FAILED = "failed"

class RiskApprovalStatus(str, Enum):

    APPROVED = "approved"

    USED = "used"

    REVOKED = "revoked"

    EXPIRED = "expired"

StrategyBucket = Literal["core_dividend", "value_rebound", "news_momentum", "unassigned"]

class CustomBaseModel(BaseModel):

    model_config = ConfigDict(

        json_encoders = {

            datetime.datetime: lambda v: v.isoformat(),

            Decimal: lambda v: float(v) if v is not None else None

        },

        from_attributes=True,

        populate_by_name=True

    )

T = TypeVar("T")

class StandardAgentResponse(CustomBaseModel, Generic[T]):

    status: Literal["success", "error"]

    agent_type: str = "database"

    version: str = "1.1.0"

    schema_version: str = DEFAULT_SCHEMA_VERSION

    timestamp: datetime.datetime

    correlation_id: Optional[str] = None

    data: Optional[T] = None

    metadata: Dict[str, Any] = Field(default_factory=dict)

    error: Optional[dict] = None

    confidence_score: Optional[float] = None

    @field_validator("schema_version")
    @classmethod
    def schema_version_must_be_semantic(cls, value):

        parts = value.split(".")

        if not all(part.isdigit() for part in parts):

            raise ValueError('Schema version must be in semantic format (e.g., "1.0")')

        return value

class AccountBalance(CustomBaseModel):

    account_id: Union[int, str]

    cash_balance: Decimal

class Position(CustomBaseModel):

    account_id: Union[int, str]

    symbol: str

    quantity: int

    average_cost: Decimal

    current_market_price: Optional[Decimal] = None

    market_value: Optional[Decimal] = None

    strategy_bucket: StrategyBucket = "unassigned"

class BrokerSyncBody(CustomBaseModel):

    source: str = "execution_agent"

    account_id: Union[int, str] = 1

    broker: Optional[str] = None

    paper: Optional[bool] = None

    captured_at: Optional[datetime.datetime] = None

    account: Dict[str, Any] = Field(default_factory=dict)

    positions: List[Dict[str, Any]] = Field(default_factory=list)

    open_orders: List[Dict[str, Any]] = Field(default_factory=list)

    summary: Dict[str, Any] = Field(default_factory=dict)

    order_classification: Dict[str, Any] = Field(default_factory=dict)

class BrokerSyncResult(CustomBaseModel):

    account_id: Union[int, str]

    cash_balance: Decimal

    positions_synced: int

    open_orders_synced: int

    missing_open_orders_marked_cancelled: int = 0

    synced_at: datetime.datetime

class PortfolioAudit(CustomBaseModel):

    portfolio_audit_id: str

    account_id: Union[int, str]

    correlation_id: Optional[str] = None

    policy_name: Optional[str] = None

    mode: str = "portfolio_allocation"

    status: str = "created"

    allocation_plan: Dict[str, Any] = Field(default_factory=dict)

    portfolio_snapshot: Dict[str, Any] = Field(default_factory=dict)

    selected_positions: List[Dict[str, Any]] = Field(default_factory=list)

    risk_approvals: List[Dict[str, Any]] = Field(default_factory=list)

    execution_orders: List[Dict[str, Any]] = Field(default_factory=list)

    metadata: Dict[str, Any] = Field(default_factory=dict)

    created_at: Optional[datetime.datetime] = None

class CreatePortfolioAuditBody(CustomBaseModel):

    portfolio_audit_id: Optional[str] = None

    account_id: Union[int, str]

    correlation_id: Optional[str] = None

    allocation_plan: Dict[str, Any] = Field(default_factory=dict)

    portfolio_snapshot: Dict[str, Any] = Field(default_factory=dict)

    selected_positions: List[Dict[str, Any]] = Field(default_factory=list)

    risk_approvals: List[Dict[str, Any]] = Field(default_factory=list)

    execution_orders: List[Dict[str, Any]] = Field(default_factory=list)

    metadata: Dict[str, Any] = Field(default_factory=dict)

    status: str = "created"

class PortfolioMetrics(CustomBaseModel):

    win_rate: float = 0.0

    average_return: float = 0.0

    max_drawdown: float = 0.0

    sharpe_ratio: float = 0.0

class Order(CustomBaseModel):

    order_id: int

    trade_id: Union[int, str]

    account_id: Union[int, str]

    symbol: str

    side: OrderSide

    order_type: OrderType

    price: Optional[Decimal] = None

    quantity: int

    time_in_force: TimeInForce = TimeInForce.GTC

    strategy_bucket: StrategyBucket = "unassigned"

    risk_approval_id: Optional[str] = None

    final_quantity: Optional[int] = None

    guard_plan: Optional[Dict[str, Any]] = None

    protective_exit: Optional[Dict[str, Any]] = None

    status: OrderStatus = OrderStatus.PENDING

    broker_order_id: Optional[str] = None

    broker_status: Optional[str] = None

    reason: Optional[str] = None
