from __future__ import annotations

import math
import re
from datetime import datetime
from typing import Any, Dict, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


PromotionState = Literal[
    "GENERATED",
    "VALIDATED",
    "OOS_PASSED",
    "ROBUSTNESS_PASSED",
    "APPROVED_FOR_PAPER",
    "PAPER_OBSERVING",
    "REJECTED",
    "FAILED",
    "EXPIRED",
    "REVOKED",
]

NonTerminalPromotionState = Literal[
    "GENERATED",
    "VALIDATED",
    "OOS_PASSED",
    "ROBUSTNESS_PASSED",
    "APPROVED_FOR_PAPER",
    "PAPER_OBSERVING",
]

_SYMBOL_RE = re.compile(r"^[A-Z0-9][A-Z0-9.-]{0,19}$")
_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:/-]{0,255}$")
_FINGERPRINT_RE = re.compile(r"^[A-Fa-f0-9]{32,128}$")


def _validate_json_tree(value: Any, *, path: str = "metadata") -> Any:
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError(f"{path} contains NaN or Infinity")
        return value
    if isinstance(value, list):
        return [
            _validate_json_tree(item, path=f"{path}[{index}]")
            for index, item in enumerate(value)
        ]
    if isinstance(value, dict):
        normalized: dict[str, Any] = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise ValueError(f"{path} keys must be strings")
            normalized[key] = _validate_json_tree(item, path=f"{path}.{key}")
        return normalized
    raise ValueError(f"{path} contains unsupported value type {type(value).__name__}")


def _validate_metadata_object(value: Dict[str, Any]) -> Dict[str, Any]:
    validated = _validate_json_tree(value)
    if not isinstance(validated, dict):
        raise ValueError("metadata must be an object")
    return validated


class StrictPromotionModel(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        strict=True,
        str_strip_whitespace=True,
        validate_assignment=True,
    )


class CreateBacktestPromotionBody(StrictPromotionModel):
    account_id: str = Field(min_length=1, max_length=128)
    run_id: str = Field(min_length=1, max_length=256)
    skill_id: str = Field(min_length=1, max_length=256)
    strategy_id: str = Field(min_length=1, max_length=256)
    symbol: str = Field(min_length=1, max_length=20)
    timeframe: str = Field(min_length=1, max_length=32)
    dataset_fingerprint: str = Field(min_length=32, max_length=128)
    engine_version: str = Field(min_length=1, max_length=128)
    validation_profile: Literal["nested_walk_forward_v2"]
    evidence_version: int = Field(default=1, ge=1)
    expires_at: Optional[datetime] = None
    reason_code: str = Field(
        default="backtest_evidence_published",
        min_length=1,
        max_length=128,
    )
    reason: str = Field(
        default="Backtest evidence was stored and registered for promotion review",
        min_length=1,
        max_length=2000,
    )
    correlation_id: Optional[str] = Field(default=None, min_length=1, max_length=256)
    metadata: Dict[str, Any] = Field(default_factory=dict)

    @field_validator("symbol")
    @classmethod
    def canonicalize_symbol(cls, value: str) -> str:
        normalized = value.strip().upper()
        if _SYMBOL_RE.fullmatch(normalized) is None:
            raise ValueError("symbol must be canonical uppercase market notation")
        return normalized

    @field_validator("run_id", "skill_id", "strategy_id", "engine_version")
    @classmethod
    def validate_identifier(cls, value: str) -> str:
        if _ID_RE.fullmatch(value) is None:
            raise ValueError("identifier contains unsupported characters")
        return value

    @field_validator("timeframe")
    @classmethod
    def normalize_timeframe(cls, value: str) -> str:
        normalized = value.strip().lower()
        if re.fullmatch(r"^[0-9]+(?:min|m|h|d|w)$", normalized) is None:
            raise ValueError(
                "timeframe must use a canonical value such as 15m, 1h, or 1d"
            )
        return normalized

    @field_validator("dataset_fingerprint")
    @classmethod
    def validate_fingerprint(cls, value: str) -> str:
        normalized = value.lower()
        if _FINGERPRINT_RE.fullmatch(normalized) is None:
            raise ValueError("dataset_fingerprint must be a hexadecimal digest")
        return normalized

    @field_validator("expires_at", mode="before")
    @classmethod
    def parse_expires_at(cls, value: Any) -> Any:
        if isinstance(value, str):
            normalized = value.strip()
            if normalized.endswith("Z"):
                normalized = f"{normalized[:-1]}+00:00"
            try:
                return datetime.fromisoformat(normalized)
            except ValueError as exc:
                raise ValueError("expires_at must be ISO-8601") from exc
        return value

    @field_validator("expires_at")
    @classmethod
    def validate_expires_at(cls, value: Optional[datetime]) -> Optional[datetime]:
        if value is not None and value.tzinfo is None:
            raise ValueError("expires_at must include a timezone")
        return value

    @field_validator("metadata")
    @classmethod
    def validate_metadata(cls, value: Dict[str, Any]) -> Dict[str, Any]:
        return _validate_metadata_object(value)


class TransitionBacktestPromotionBody(StrictPromotionModel):
    expected_state: PromotionState
    expected_version: int = Field(ge=1)
    next_state: PromotionState
    reason_code: str = Field(min_length=1, max_length=128)
    reason: str = Field(min_length=1, max_length=2000)
    evidence_run_id: str = Field(min_length=1, max_length=256)
    correlation_id: Optional[str] = Field(default=None, min_length=1, max_length=256)
    evidence_version: Optional[int] = Field(default=None, ge=1)
    approver: Optional[str] = Field(default=None, min_length=1, max_length=256)
    metadata: Dict[str, Any] = Field(default_factory=dict)

    @field_validator("evidence_run_id")
    @classmethod
    def validate_evidence_run_id(cls, value: str) -> str:
        if _ID_RE.fullmatch(value) is None:
            raise ValueError("evidence_run_id contains unsupported characters")
        return value

    @field_validator("metadata")
    @classmethod
    def validate_metadata(cls, value: Dict[str, Any]) -> Dict[str, Any]:
        return _validate_metadata_object(value)

    @model_validator(mode="after")
    def reject_noop(self) -> "TransitionBacktestPromotionBody":
        if self.expected_state == self.next_state:
            raise ValueError("next_state must differ from expected_state")
        return self


class RevokeBacktestPromotionBody(StrictPromotionModel):
    expected_version: int = Field(ge=1)
    reason_code: Literal[
        "paper_drawdown_exceeded",
        "broker_reconciliation_failed",
        "duplicate_order_detected",
        "strategy_drift",
        "evidence_superseded",
        "emergency_halt",
        "data_quality_failed",
        "manual_revoke",
    ]
    reason: str = Field(min_length=1, max_length=2000)
    correlation_id: Optional[str] = Field(default=None, min_length=1, max_length=256)
    approver: Optional[str] = Field(default=None, min_length=1, max_length=256)


class BacktestPromotionRecord(StrictPromotionModel):
    promotion_id: str
    account_id: str
    run_id: str
    skill_id: str
    strategy_id: str
    symbol: str
    timeframe: str
    dataset_fingerprint: str
    engine_version: str
    validation_profile: str
    state: PromotionState
    version: int
    evidence_version: int
    created_at: datetime
    updated_at: datetime
    validated_at: Optional[datetime] = None
    oos_passed_at: Optional[datetime] = None
    robustness_passed_at: Optional[datetime] = None
    approved_for_paper_at: Optional[datetime] = None
    paper_observing_at: Optional[datetime] = None
    expires_at: Optional[datetime] = None
    revoked_at: Optional[datetime] = None
    rejected_at: Optional[datetime] = None
    failed_at: Optional[datetime] = None
    last_observed_at: Optional[datetime] = None
    reason_code: Optional[str] = None
    reason: Optional[str] = None
    correlation_id: Optional[str] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)
    idempotent_replay: bool = False


class BacktestPromotionTransitionRecord(StrictPromotionModel):
    transition_id: str
    promotion_id: str
    from_state: PromotionState
    to_state: PromotionState
    from_version: int
    to_version: int
    status: Literal["COMPLETED", "REJECTED"]
    reason_code: str
    reason: str
    evidence_run_id: str
    correlation_id: Optional[str] = None
    created_at: datetime
