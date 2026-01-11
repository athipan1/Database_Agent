from pydantic import BaseModel, Field
from typing import Literal, Optional, List
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

    class Config:
        orm_mode = True

class CreateOrderBody(BaseModel):
    client_order_id: Optional[UUID] = Field(None, description="A unique client-generated ID for idempotency. If not provided, one will be generated.")
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

from pydantic import validator

class Price(BaseModel):
    symbol: str
    timestamp: str
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: int

class PriceInput(BaseModel):
    symbol: str
    timestamp: datetime.datetime
    open: Decimal = Field(..., gt=0)
    high: Decimal = Field(..., gt=0)
    low: Decimal = Field(..., gt=0)
    close: Decimal = Field(..., gt=0)
    volume: int = Field(..., gt=0)

    @validator('high')
    def high_must_be_the_highest(cls, v, values):
        if 'open' in values and v < values['open']:
            raise ValueError('high must be greater than or equal to open')
        if 'low' in values and v < values['low']:
            raise ValueError('high must be greater than or equal to low')
        if 'close' in values and v < values['close']:
            raise ValueError('high must be greater than or equal to close')
        return v

    @validator('low')
    def low_must_be_the_lowest(cls, v, values):
        if 'open' in values and v > values['open']:
            raise ValueError('low must be less than or equal to open')
        if 'high' in values and v > values['high']:
            # This case is already covered by the high validator, but we keep it for completeness
            raise ValueError('low must be less than or equal to high')
        if 'close' in values and v > values['close']:
            raise ValueError('low must be less than or equal to close')
        return v
