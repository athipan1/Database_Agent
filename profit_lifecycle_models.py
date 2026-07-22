from __future__ import annotations

from decimal import Decimal
from typing import Any, Dict, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator


ProfitDecisionStatus = Literal[
    "PROPOSED",
    "RISK_APPROVED",
    "EXECUTION_PENDING",
    "EXECUTED",
    "REJECTED",
    "FAILED",
    "EXPIRED",
]


class StrictLifecycleModel(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)


class ProfitLifecycle(StrictLifecycleModel):
    account_id: str
    position_id: str
    position_version: int = Field(ge=1)
    symbol: str
    first_target_executed: bool = False
    second_target_executed: bool = False
    total_exited_quantity: Decimal = Field(default=Decimal("0"), ge=0)
    remaining_quantity: Decimal = Field(gt=0)
    highest_price_since_entry: Optional[Decimal] = None
    last_profit_decision_id: Optional[str] = None
    last_profit_decision_status: Optional[str] = None
    last_profit_decision_at: Optional[str] = None


class ReserveProfitDecisionBody(StrictLifecycleModel):
    position_id: str
    position_version: int = Field(ge=1)
    decision_id: str = Field(min_length=1, max_length=512)
    decision_type: str = Field(min_length=1, max_length=64)
    proposed_quantity: Decimal = Field(gt=0)
    next_lifecycle_state: Dict[str, bool] = Field(default_factory=dict)
    metadata: Dict[str, Any] = Field(default_factory=dict)

    @field_validator("next_lifecycle_state")
    @classmethod
    def validate_next_lifecycle_state(cls, value: Dict[str, bool]) -> Dict[str, bool]:
        allowed = {"first_target_executed", "second_target_executed"}
        unknown = set(value) - allowed
        if unknown:
            raise ValueError(
                f"unknown lifecycle transition fields: {', '.join(sorted(unknown))}"
            )
        return value


class TransitionProfitDecisionBody(StrictLifecycleModel):
    expected_status: ProfitDecisionStatus
    status: ProfitDecisionStatus
    executed_quantity: Decimal = Field(default=Decimal("0"), ge=0)
    error: Optional[str] = Field(default=None, max_length=1000)
    metadata: Dict[str, Any] = Field(default_factory=dict)


class ProfitDecision(StrictLifecycleModel):
    account_id: str
    position_id: str
    position_version: int
    decision_id: str
    decision_type: str
    status: ProfitDecisionStatus
    proposed_quantity: Decimal
    executed_quantity: Decimal
    correlation_id: Optional[str] = None
    next_lifecycle_state: Dict[str, bool] = Field(default_factory=dict)
    metadata: Dict[str, Any] = Field(default_factory=dict)
    error: Optional[str] = None
    duplicate: bool = False
    created_at: Optional[str] = None
    updated_at: Optional[str] = None
