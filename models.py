from pydantic import BaseModel, Field
from typing import Literal, Optional
from decimal import Decimal
from uuid import UUID
import datetime
from typing import List

class CustomBaseModel(BaseModel):
    class Config:
        json_encoders = {
            datetime.datetime: lambda v: v.isoformat()
        }

class AccountBalance(CustomBaseModel):
    cash_balance: Decimal

class Position(CustomBaseModel):
    symbol: str
    quantity: int
    average_cost: Decimal

class Order(CustomBaseModel):
    order_id: int
    client_order_id: UUID
    symbol: str
    order_type: Literal["BUY", "SELL"]
    quantity: int
    price: Optional[Decimal]
    status: Literal["pending", "executed", "cancelled", "failed"]
    failure_reason: Optional[str] = None
    timestamp: datetime.datetime

class CreateOrderBody(CustomBaseModel):
    client_order_id: Optional[UUID] = Field(None, description="A unique client-generated ID for idempotency. If not provided, one will be generated.")
    symbol: str
    order_type: Literal["BUY", "SELL"]
    quantity: int
    price: Decimal

class CreateOrderResponse(CustomBaseModel):
    order_id: int
    status: str
    client_order_id: UUID


class OrderExecutionResponse(CustomBaseModel):
    order_id: int
    status: Literal["executed", "failed"]
    reason: Optional[str] = None

class ExecutionTrade(CustomBaseModel):
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

class Price(CustomBaseModel):
    symbol: str
    timestamp: str
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: int
