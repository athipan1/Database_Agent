from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Literal, Optional, Union

from pydantic import BaseModel, ConfigDict, Field, field_validator


ShadowEventType = Literal[
    "signal_decision",
    "entry_simulated",
    "mark",
    "exit_simulated",
]
ShadowSide = Literal["buy", "sell"]


class ShadowObservationBase(BaseModel):
    model_config = ConfigDict(extra="forbid")

    event_id: Optional[str] = Field(default=None, min_length=1, max_length=200)
    shadow_trade_id: Optional[str] = Field(default=None, min_length=1, max_length=200)
    account_id: Union[int, str]
    correlation_id: Optional[str] = Field(default=None, max_length=200)
    signal_id: str = Field(min_length=1, max_length=200)
    strategy_id: str = Field(default="unassigned", min_length=1, max_length=200)
    strategy_version: Optional[str] = Field(default=None, max_length=200)
    symbol: str = Field(min_length=1, max_length=32)
    side: ShadowSide = "buy"
    event_type: ShadowEventType
    event_time: Optional[datetime] = None
    decision_price: Optional[float] = Field(default=None, gt=0)
    bid: Optional[float] = Field(default=None, gt=0)
    ask: Optional[float] = Field(default=None, gt=0)
    spread_bps: Optional[float] = Field(default=None, ge=0)
    simulated_fill_price: Optional[float] = Field(default=None, gt=0)
    simulated_slippage_bps: Optional[float] = None
    stop_loss: Optional[float] = Field(default=None, gt=0)
    take_profit: Optional[float] = Field(default=None, gt=0)
    market_regime: Optional[str] = Field(default=None, max_length=80)
    scanner_score: Optional[float] = Field(default=None, ge=0, le=1)
    opportunity_score: Optional[float] = Field(default=None, ge=0, le=1)
    mfe_pct: Optional[float] = None
    mae_pct: Optional[float] = None
    exit_price: Optional[float] = Field(default=None, gt=0)
    exit_reason: Optional[str] = Field(default=None, max_length=200)
    gross_return_pct: Optional[float] = None
    estimated_cost_pct: Optional[float] = Field(default=None, ge=0)
    net_return_pct: Optional[float] = None
    holding_period_seconds: Optional[float] = Field(default=None, ge=0)
    source_commit_sha: Optional[str] = Field(default=None, max_length=80)
    execution_mode: Literal["shadow"] = "shadow"
    broker_order_authorized: Literal[False] = False
    metadata: Dict[str, Any] = Field(default_factory=dict)

    @field_validator("symbol")
    @classmethod
    def normalize_symbol(cls, value: str) -> str:
        return value.strip().upper()


class CreateShadowObservationBody(ShadowObservationBase):
    pass


class ShadowObservation(ShadowObservationBase):
    event_id: str
    shadow_trade_id: str
    event_time: datetime
    created_at: datetime
