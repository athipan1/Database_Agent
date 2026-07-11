from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Literal, Optional, Union

from pydantic import BaseModel, ConfigDict, Field, field_validator


CuratorSignal = Literal["buy", "hold", "sell", "unknown"]
CuratorObservationMode = Literal["shadow_ensemble", "single_skill"]


class CuratorObservationBase(BaseModel):
    model_config = ConfigDict(extra="forbid")

    observation_id: Optional[str] = Field(default=None, min_length=1, max_length=200)
    account_id: Union[int, str]
    correlation_id: Optional[str] = Field(default=None, max_length=200)
    symbol: str = Field(min_length=1, max_length=32)
    observed_at: Optional[datetime] = None
    mode: CuratorObservationMode = "shadow_ensemble"
    status: str = Field(default="success", min_length=1, max_length=80)
    available: bool = True
    signal: CuratorSignal = "unknown"
    agreement: Optional[float] = Field(default=None, ge=0.0, le=1.0)
    contract_valid: Optional[bool] = None
    would_pass_required_gate: Optional[bool] = None
    selected_skill_count: int = Field(default=0, ge=0, le=100)
    execution_count: int = Field(default=0, ge=0, le=100)
    minimum_agreement: Optional[float] = Field(default=None, ge=0.0, le=1.0)
    rejection_codes: List[str] = Field(default_factory=list, max_length=100)
    metadata: Dict[str, Any] = Field(default_factory=dict)

    @field_validator("symbol")
    @classmethod
    def normalize_symbol(cls, value: str) -> str:
        return value.strip().upper()

    @field_validator("signal")
    @classmethod
    def normalize_signal(cls, value: str) -> str:
        return value.strip().lower()

    @field_validator("rejection_codes")
    @classmethod
    def normalize_rejection_codes(cls, values: List[str]) -> List[str]:
        normalized: List[str] = []
        seen: set[str] = set()
        for value in values:
            code = str(value).strip()
            if not code or code in seen:
                continue
            seen.add(code)
            normalized.append(code)
        return normalized


class CreateCuratorObservationBody(CuratorObservationBase):
    pass


class CreateCuratorObservationBatchBody(BaseModel):
    model_config = ConfigDict(extra="forbid")

    observations: List[CreateCuratorObservationBody] = Field(min_length=1, max_length=100)


class CuratorObservation(CuratorObservationBase):
    observation_id: str
    observed_at: datetime
    created_at: datetime


class CuratorObservationReadiness(BaseModel):
    account_id: Optional[str] = None
    mode: str = "shadow_ensemble"
    observations: int = 0
    observation_target: int = 50
    available: int = 0
    unavailable: int = 0
    availability_rate: Optional[float] = None
    contract_valid: int = 0
    contract_invalid: int = 0
    contract_valid_rate: Optional[float] = None
    unsafe_contract_count: int = 0
    buy_count: int = 0
    hold_count: int = 0
    sell_count: int = 0
    unknown_count: int = 0
    average_agreement: Optional[float] = None
    would_pass_required_gate: int = 0
    would_be_blocked: int = 0
    required_mode_eligible: bool = False
    blockers: List[str] = Field(default_factory=list)
