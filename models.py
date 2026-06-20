from pydantic import BaseModel, Field, ConfigDict, field_validator
from typing import Literal, Optional, Any, TypeVar, Generic, List, Union, Dict
from decimal import Decimal
from uuid import UUID
from enum import Enum
import datetime

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

class RiskApprovalStatus(str, Enum):
    APPROVED = "approved"
    USED = "used"
    REVOKED = "revoked"
    EXPIRED = "expired"

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
    version: str = "1.0.0"
    timestamp: datetime.datetime
    data: Optional[T] = None
    error: Optional[dict] = None
    confidence_score: Optional[float] = None

class AccountBalance(CustomBaseModel):
    account_id: Union[int, str]
    cash_balance: Decimal

class Position(CustomBaseModel):
    account_id: Union[int, str]
    symbol: str
    quantity: int
    average_cost: Decimal

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

    # --- State Fields ---
    status: OrderStatus = OrderStatus.PENDING
    broker_order_id: Optional[str] = None
    reason: Optional[str] = None
    executed_quantity: int = 0
    avg_execution_price: Optional[Decimal] = None
    executed_at: Optional[datetime.datetime] = None

    # Backward compatibility
    client_order_id: Optional[Union[UUID, str]] = None
    failure_reason: Optional[str] = None
    timestamp: Optional[datetime.datetime] = None

class CreateOrderBody(CustomBaseModel):
    trade_id: Union[int, str] = Field(..., description="Globally unique trade ID")
    account_id: Union[int, str]
    symbol: str
    side: OrderSide
    order_type: OrderType
    price: Optional[Decimal] = None
    quantity: int
    time_in_force: TimeInForce = TimeInForce.GTC

    # Backward compatibility
    client_order_id: Optional[Union[UUID, str]] = None

class CreateOrderResponse(CustomBaseModel):
    order_id: int
    trade_id: Union[int, str]
    account_id: Union[int, str]
    status: OrderStatus
    client_order_id: Optional[Union[UUID, str]] = None
    reason: Optional[str] = None

class OrderExecutionResponse(CustomBaseModel):
    order_id: int
    trade_id: Optional[Union[int, str]] = None
    account_id: Union[int, str]
    status: OrderStatus
    reason: Optional[str] = None

class ExecutionTrade(CustomBaseModel):
    trade_id: Union[int, str]
    account_id: Union[int, str]
    asset_id: Optional[str] = None
    symbol: str
    side: str
    quantity: int
    price: Decimal
    notional: Decimal
    executed_at: datetime.datetime
    source_agent: Optional[str] = None

class RiskApproval(CustomBaseModel):
    approval_id: str
    account_id: Union[int, str]
    symbol: str
    side: OrderSide
    approved_quantity: int
    status: RiskApprovalStatus = RiskApprovalStatus.APPROVED
    expires_at: datetime.datetime
    created_at: Optional[datetime.datetime] = None
    used_at: Optional[datetime.datetime] = None
    order_id: Optional[int] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)

class CreateRiskApprovalBody(CustomBaseModel):
    approval_id: str
    account_id: Union[int, str]
    symbol: str
    side: OrderSide
    approved_quantity: int = Field(gt=0)
    expires_at: datetime.datetime
    metadata: Dict[str, Any] = Field(default_factory=dict)

class MarkRiskApprovalUsedBody(CustomBaseModel):
    order_id: int

class SignalHistory(CustomBaseModel):
    signal_id: str
    account_id: Union[int, str]
    symbol: str
    timestamp: datetime.datetime
    source_agent: str = "manager-agent"
    candidate_score: Optional[float] = None
    technical_score: Optional[float] = None
    fundamental_score: Optional[float] = None
    final_verdict: Optional[str] = None
    market_regime: Optional[str] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)

class CreateSignalHistoryBody(CustomBaseModel):
    signal_id: Optional[str] = None
    account_id: Union[int, str]
    symbol: str
    source_agent: str = "manager-agent"
    candidate_score: Optional[float] = None
    technical_score: Optional[float] = None
    fundamental_score: Optional[float] = None
    final_verdict: Optional[str] = None
    market_regime: Optional[str] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)

class PerformanceMetric(CustomBaseModel):
    metric_id: str
    account_id: Union[int, str]
    symbol: str
    timestamp: datetime.datetime
    source_agent: str = "learning-agent"
    entry_price: Optional[Decimal] = None
    exit_price: Optional[Decimal] = None
    current_price: Optional[Decimal] = None
    return_pct: Optional[float] = None
    holding_days: Optional[int] = None
    outcome: Optional[str] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)

class CreatePerformanceMetricBody(CustomBaseModel):
    metric_id: Optional[str] = None
    account_id: Union[int, str]
    symbol: str
    source_agent: str = "learning-agent"
    entry_price: Optional[Decimal] = None
    exit_price: Optional[Decimal] = None
    current_price: Optional[Decimal] = None
    return_pct: Optional[float] = None
    holding_days: Optional[int] = None
    outcome: Optional[str] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)

class Price(CustomBaseModel):
    symbol: str
    timestamp: datetime.datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: int
