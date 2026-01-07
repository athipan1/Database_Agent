from pydantic import BaseModel, Field
from typing import Literal, Optional
from decimal import Decimal
from uuid import UUID
import datetime

class AccountBalance(BaseModel):
    cash_balance: Decimal

class Position(BaseModel):
    symbol: str
    quantity: int
    average_cost: Decimal

class Order(BaseModel):
    order_id: int
    client_order_id: UUID
    symbol: str
    order_type: Literal["BUY", "SELL"]
    quantity: int
    price: Optional[Decimal]
    status: Literal["pending", "executed", "cancelled", "failed"]
    failure_reason: Optional[str] = None
    timestamp: datetime.datetime

class CreateOrderBody(BaseModel):
    client_order_id: UUID = Field(..., description="A unique client-generated ID for idempotency.")
    symbol: str
    order_type: Literal["BUY", "SELL"]
    quantity: int
    price: Decimal

class CreateOrderResponse(BaseModel):
    order_id: int
    status: str
    client_order_id: UUID


class OrderExecutionResponse(BaseModel):
    order_id: int
    status: Literal["executed", "failed"]
    reason: Optional[str] = None

class Trade(BaseModel):
    trade_id: int
    account_id: int
    asset_id: Optional[str] = None
    symbol: str
    side: str
    quantity: int
    price: Decimal
    notional: Decimal
    executed_at: str
    source_agent: Optional[str] = None

class PositionMetrics(BaseModel):
    asset_id: Optional[str] = None
    symbol: str
    quantity: int
    avg_cost: Decimal
    market_price: Decimal
    market_value: Decimal
    unrealized_pnl: Decimal

class PortfolioMetrics(BaseModel):
    account_id: int
    as_of: str
    total_portfolio_value: Decimal
    cash_balance: Decimal
    unrealized_pnl: Decimal
    realized_pnl: Decimal
    positions: List[PositionMetrics]

class Price(BaseModel):
    symbol: str
    timestamp: str
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: int
