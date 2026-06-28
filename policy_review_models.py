from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Literal, Optional, Union

from pydantic import BaseModel, ConfigDict, Field


class PolicyReviewBaseModel(BaseModel):
    model_config = ConfigDict(use_enum_values=True)


class PolicyReviewAuditRecord(PolicyReviewBaseModel):
    policy_review_id: str
    account_id: Union[int, str]
    symbol: Optional[str] = None
    correlation_id: Optional[str] = None
    source: str = "manager-agent"
    status: Literal["created", "review_required", "approved_for_review", "observation_only", "rejected"] = "created"
    advisory_only: bool = True
    auto_apply: bool = False
    performance_summary: Dict[str, Any] = Field(default_factory=dict)
    learning_result: Dict[str, Any] = Field(default_factory=dict)
    curated_policy: Dict[str, Any] = Field(default_factory=dict)
    metadata: Dict[str, Any] = Field(default_factory=dict)
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


class CreatePolicyReviewAuditBody(PolicyReviewBaseModel):
    policy_review_id: Optional[str] = None
    account_id: Union[int, str]
    symbol: Optional[str] = None
    correlation_id: Optional[str] = None
    source: str = "manager-agent"
    status: Optional[str] = None
    advisory_only: bool = True
    auto_apply: bool = False
    performance_summary: Dict[str, Any] = Field(default_factory=dict)
    learning_result: Dict[str, Any] = Field(default_factory=dict)
    curated_policy: Dict[str, Any] = Field(default_factory=dict)
    metadata: Dict[str, Any] = Field(default_factory=dict)


class ListPolicyReviewAuditsQuery(PolicyReviewBaseModel):
    account_id: Optional[Union[int, str]] = None
    symbol: Optional[str] = None
    status: Optional[str] = None
    source: Optional[str] = None
    advisory_only: Optional[bool] = None
    auto_apply: Optional[bool] = None
    limit: int = Field(default=100, ge=1, le=500)
    offset: int = Field(default=0, ge=0)
    sort: Literal["created_at", "updated_at"] = "updated_at"
    order: Literal["asc", "desc"] = "desc"


def now_utc() -> datetime:
    return datetime.now(timezone.utc)
