from __future__ import annotations

import re
from datetime import datetime
from typing import Any, Dict, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator


ObservedPromotionState = Literal["APPROVED_FOR_PAPER", "PAPER_OBSERVING"]
ObservationAction = Literal[
    "START_OBSERVING",
    "HEARTBEAT",
    "EXPIRE",
    "REVOKE",
]

_OBSERVATION_KEY_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:/-]{0,255}$")


class StrictObservationModel(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        strict=True,
        str_strip_whitespace=True,
        validate_assignment=True,
        allow_inf_nan=False,
    )


class ObserveBacktestPromotionBody(StrictObservationModel):
    expected_state: ObservedPromotionState
    expected_version: int = Field(ge=1)
    observation_key: str = Field(min_length=1, max_length=256)
    observed_at: datetime
    paper_drawdown_pct: float = Field(default=0.0, ge=0.0, le=1.0)
    reconciliation_ok: bool = True
    duplicate_order_count: int = Field(default=0, ge=0)
    broker_order_count: int = Field(default=0, ge=0)
    database_order_count: int = Field(default=0, ge=0)
    filled_order_count: int = Field(default=0, ge=0)
    strategy_drift: bool = False
    emergency_halt: bool = False
    notes: list[str] = Field(default_factory=list, max_length=50)
    correlation_id: Optional[str] = Field(default=None, min_length=1, max_length=256)
    metadata: Dict[str, Any] = Field(default_factory=dict)

    @field_validator("observation_key")
    @classmethod
    def validate_observation_key(cls, value: str) -> str:
        if _OBSERVATION_KEY_RE.fullmatch(value) is None:
            raise ValueError("observation_key contains unsupported characters")
        return value

    @field_validator("observed_at", mode="before")
    @classmethod
    def parse_observed_at(cls, value: Any) -> Any:
        if isinstance(value, str):
            normalized = value.strip()
            if normalized.endswith("Z"):
                normalized = f"{normalized[:-1]}+00:00"
            try:
                return datetime.fromisoformat(normalized)
            except ValueError as exc:
                raise ValueError("observed_at must be ISO-8601") from exc
        return value

    @field_validator("observed_at")
    @classmethod
    def validate_observed_at(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            raise ValueError("observed_at must include a timezone")
        return value

    @field_validator("notes")
    @classmethod
    def validate_notes(cls, value: list[str]) -> list[str]:
        normalized = []
        for item in value:
            text = item.strip()
            if not text or len(text) > 500:
                raise ValueError("notes must contain non-empty strings up to 500 characters")
            normalized.append(text)
        return normalized


class BacktestPromotionObservationRecord(StrictObservationModel):
    observation_id: str
    promotion_id: str
    observation_key: str
    action: ObservationAction
    reason_code: str
    from_state: ObservedPromotionState
    to_state: str
    from_version: int
    to_version: int
    observed_at: datetime
    created_at: datetime
    correlation_id: Optional[str] = None
    paper_drawdown_pct: float
    reconciliation_ok: bool
    duplicate_order_count: int
    broker_order_count: int
    database_order_count: int
    filled_order_count: int
    strategy_drift: bool
    emergency_halt: bool
    metadata: Dict[str, Any] = Field(default_factory=dict)
    promotion: Dict[str, Any]
    idempotent_replay: bool = False
