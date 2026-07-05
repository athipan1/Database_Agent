from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional, Union

from pydantic import BaseModel, Field


class SkillPerformanceBaseModel(BaseModel):
    class Config:
        populate_by_name = True


class CreateSkillExecutionLogBody(SkillPerformanceBaseModel):
    execution_log_id: Optional[str] = None
    account_id: Union[int, str] = "1"
    skill_id: str = Field(..., min_length=1)
    skill_name: Optional[str] = None
    symbol: Optional[str] = None
    strategy_bucket: Optional[str] = None
    market_regime: Optional[str] = None
    signal: Optional[str] = None
    confidence: Optional[float] = Field(default=None, ge=0.0, le=1.0)
    reason: Optional[str] = None
    input_payload: Dict[str, Any] = Field(default_factory=dict)
    output_payload: Dict[str, Any] = Field(default_factory=dict)
    execution_status: str = "success"
    error: Optional[str] = None
    elapsed_ms: Optional[float] = None
    source_agent: str = "curator-agent"
    run_id: Optional[str] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)
    created_at: Optional[datetime] = None


class SkillExecutionLog(CreateSkillExecutionLogBody):
    execution_log_id: str
    created_at: datetime


class CreateSkillTradeOutcomeBody(SkillPerformanceBaseModel):
    outcome_id: Optional[str] = None
    execution_log_id: str = Field(..., min_length=1)
    skill_id: Optional[str] = None
    account_id: Union[int, str] = "1"
    symbol: str = Field(..., min_length=1)
    strategy_bucket: Optional[str] = None
    market_regime: Optional[str] = None
    entry_price: Optional[float] = None
    exit_price: Optional[float] = None
    realized_pl: Optional[float] = None
    realized_pl_pct: Optional[float] = None
    holding_minutes: Optional[int] = None
    max_favorable_excursion: Optional[float] = None
    max_adverse_excursion: Optional[float] = None
    outcome: Optional[str] = None
    source_agent: str = "execution-agent"
    metadata: Dict[str, Any] = Field(default_factory=dict)
    closed_at: Optional[datetime] = None


class SkillTradeOutcome(CreateSkillTradeOutcomeBody):
    outcome_id: str
    closed_at: Optional[datetime] = None
    created_at: datetime


class SkillPerformanceSummary(SkillPerformanceBaseModel):
    skill_id: str
    skill_name: Optional[str] = None
    account_id: Optional[str] = None
    symbol: Optional[str] = None
    strategy_bucket: Optional[str] = None
    market_regime: Optional[str] = None
    execution_count: int = 0
    completed_outcomes: int = 0
    wins: int = 0
    losses: int = 0
    win_rate: float = 0.0
    avg_confidence: float = 0.0
    avg_realized_pl_pct: float = 0.0
    total_realized_pl: float = 0.0
    expectancy: float = 0.0
    skill_score: float = 0.0
    last_execution_at: Optional[datetime] = None
    reasons: List[str] = Field(default_factory=list)
