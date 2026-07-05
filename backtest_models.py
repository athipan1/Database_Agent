from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class MarketDataBar(BaseModel):
    symbol: str
    timeframe: str = "1d"
    bar_time: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float = 0.0
    source: str = "database_agent"
    metadata: Dict[str, Any] = Field(default_factory=dict)
    created_at: Optional[datetime] = None


class UpsertMarketDataBarsBody(BaseModel):
    bars: List[MarketDataBar] = Field(default_factory=list)


class BacktestTrade(BaseModel):
    trade_id: Optional[str] = None
    run_id: Optional[str] = None
    symbol: str
    side: str
    quantity: float = 0.0
    entry_time: Optional[datetime] = None
    entry_price: Optional[float] = None
    exit_time: Optional[datetime] = None
    exit_price: Optional[float] = None
    realized_pl: Optional[float] = None
    realized_pl_pct: Optional[float] = None
    fees: float = 0.0
    outcome: Optional[str] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)
    created_at: Optional[datetime] = None


class BacktestEquityPoint(BaseModel):
    point_id: Optional[str] = None
    run_id: Optional[str] = None
    timestamp: datetime
    equity: float
    drawdown: Optional[float] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)


class SkillBacktestResult(BaseModel):
    result_id: Optional[str] = None
    skill_id: str
    run_id: str
    passed: bool = False
    status: str = "failed"
    win_rate: Optional[float] = None
    profit_factor: Optional[float] = None
    expectancy: Optional[float] = None
    max_drawdown: Optional[float] = None
    total_trades: Optional[int] = None
    score: Optional[float] = None
    reasons: List[str] = Field(default_factory=list)
    metadata: Dict[str, Any] = Field(default_factory=dict)
    created_at: Optional[datetime] = None


class BacktestRun(BaseModel):
    run_id: Optional[str] = None
    account_id: Optional[str] = None
    skill_id: Optional[str] = None
    strategy_id: Optional[str] = None
    symbol: Optional[str] = None
    timeframe: str = "1d"
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    status: str = "completed"
    engine_version: Optional[str] = None
    parameters: Dict[str, Any] = Field(default_factory=dict)
    metrics: Dict[str, Any] = Field(default_factory=dict)
    source_agent: str = "backtest-agent"
    metadata: Dict[str, Any] = Field(default_factory=dict)
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


class CreateBacktestRunBody(BacktestRun):
    trades: List[BacktestTrade] = Field(default_factory=list)
    equity_curve: List[BacktestEquityPoint] = Field(default_factory=list)
    skill_result: Optional[SkillBacktestResult] = None


class BacktestRunDetail(BaseModel):
    run: BacktestRun
    trades: List[BacktestTrade] = Field(default_factory=list)
    equity_curve: List[BacktestEquityPoint] = Field(default_factory=list)
    skill_result: Optional[SkillBacktestResult] = None


class SkillBacktestStatus(BaseModel):
    skill_id: str
    status: str
    passed: bool
    latest_run_id: Optional[str] = None
    latest_score: Optional[float] = None
    latest_profit_factor: Optional[float] = None
    latest_win_rate: Optional[float] = None
    latest_max_drawdown: Optional[float] = None
    total_runs: int = 0
    reasons: List[str] = Field(default_factory=list)
    updated_at: Optional[datetime] = None
