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
