# In Database_Agent/models.py
from pydantic import BaseModel
from typing import Literal, List

class AccountBalance(BaseModel):
    cash_balance: float

class Position(BaseModel):
    symbol: str
    quantity: int
    average_cost: float

class Order(BaseModel):
    order_id: int
    symbol: str
    order_type: Literal["BUY", "SELL"]
    quantity: int
    price: float
    status: Literal["pending", "executed", "cancelled", "failed"]
    timestamp: str

class CreateOrderBody(BaseModel):
    symbol: str
    order_type: Literal["BUY", "SELL"]
    quantity: int
    price: float

class CreateOrderResponse(BaseModel):
    order_id: int
    status: str
