from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from backtest_models import (
    BacktestEquityPoint,
    BacktestRun,
    BacktestRunDetail,
    BacktestTrade,
    MarketDataBar,
    SkillBacktestResult,
    SkillBacktestStatus,
)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _json_dumps(value: Dict[str, Any] | List[Any] | None) -> str:
    return json.dumps(value or {}, default=str, sort_keys=True)


def _json_loads(value: Any, default: Any = None) -> Any:
    if default is None:
        default = {}
    if value is None:
        return default
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(value)
    except Exception:
        return default


def _dt(value: Any) -> Optional[datetime]:
    if value is None or isinstance(value, datetime):
        return value
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except Exception:
        return None


def setup_backtest_tables(db) -> None:
    with db.connection_scope() as conn:
        cursor = db.get_cursor(conn)
        try:
            json_type = "TEXT" if db.db_type == "sqlite" else "JSONB"
            timestamp_type = "TEXT" if db.db_type == "sqlite" else "TIMESTAMPTZ"
            numeric_type = "REAL" if db.db_type == "sqlite" else "DOUBLE PRECISION"
            cursor.execute(
                f"""
                CREATE TABLE IF NOT EXISTS market_data_bars (
                    symbol TEXT NOT NULL,
                    timeframe TEXT NOT NULL,
                    bar_time {timestamp_type} NOT NULL,
                    open {numeric_type} NOT NULL,
                    high {numeric_type} NOT NULL,
                    low {numeric_type} NOT NULL,
                    close {numeric_type} NOT NULL,
                    volume {numeric_type} DEFAULT 0,
                    source TEXT NOT NULL,
                    metadata {json_type} NOT NULL,
                    created_at {timestamp_type} NOT NULL,
                    PRIMARY KEY (symbol, timeframe, bar_time)
                );
                """
            )
            cursor.execute(
                f"""
                CREATE TABLE IF NOT EXISTS backtest_runs (
                    run_id TEXT PRIMARY KEY,
                    account_id TEXT,
                    skill_id TEXT,
                    strategy_id TEXT,
                    symbol TEXT,
                    timeframe TEXT NOT NULL,
                    start_time {timestamp_type},
                    end_time {timestamp_type},
                    status TEXT NOT NULL,
                    engine_version TEXT,
                    parameters {json_type} NOT NULL,
                    metrics {json_type} NOT NULL,
                    source_agent TEXT NOT NULL,
                    metadata {json_type} NOT NULL,
                    created_at {timestamp_type} NOT NULL,
                    updated_at {timestamp_type} NOT NULL
                );
                """
            )
            cursor.execute(
                f"""
                CREATE TABLE IF NOT EXISTS backtest_trades (
                    trade_id TEXT PRIMARY KEY,
                    run_id TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    side TEXT NOT NULL,
                    quantity {numeric_type},
                    entry_time {timestamp_type},
                    entry_price {numeric_type},
                    exit_time {timestamp_type},
                    exit_price {numeric_type},
                    realized_pl {numeric_type},
                    realized_pl_pct {numeric_type},
                    fees {numeric_type},
                    outcome TEXT,
                    metadata {json_type} NOT NULL,
                    created_at {timestamp_type} NOT NULL
                );
                """
            )
            cursor.execute(
                f"""
                CREATE TABLE IF NOT EXISTS backtest_equity_curve (
                    point_id TEXT PRIMARY KEY,
                    run_id TEXT NOT NULL,
                    timestamp {timestamp_type} NOT NULL,
                    equity {numeric_type} NOT NULL,
                    drawdown {numeric_type},
                    metadata {json_type} NOT NULL
                );
                """
            )
            cursor.execute(
                f"""
                CREATE TABLE IF NOT EXISTS skill_backtest_results (
                    result_id TEXT PRIMARY KEY,
                    skill_id TEXT NOT NULL,
                    run_id TEXT NOT NULL,
                    passed INTEGER NOT NULL,
                    status TEXT NOT NULL,
                    win_rate {numeric_type},
                    profit_factor {numeric_type},
                    expectancy {numeric_type},
                    max_drawdown {numeric_type},
                    total_trades INTEGER,
                    score {numeric_type},
                    reasons {json_type} NOT NULL,
                    metadata {json_type} NOT NULL,
                    created_at {timestamp_type} NOT NULL
                );
                """
            )
            if db.db_type == "postgres":
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_market_data_bars_lookup ON market_data_bars(symbol, timeframe, bar_time)")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_backtest_runs_skill ON backtest_runs(skill_id, symbol, timeframe, created_at DESC)")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_backtest_trades_run ON backtest_trades(run_id)")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_skill_backtest_results_skill ON skill_backtest_results(skill_id, created_at DESC)")
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cursor.close()


def _bar_from_row(row: Any) -> MarketDataBar:
    item = dict(row)
    item["metadata"] = _json_loads(item.get("metadata"), {})
    item["bar_time"] = _dt(item.get("bar_time")) or _utc_now()
    item["created_at"] = _dt(item.get("created_at")) or _utc_now()
    return MarketDataBar(**item)


def _run_from_row(row: Any) -> BacktestRun:
    item = dict(row)
    item["parameters"] = _json_loads(item.get("parameters"), {})
    item["metrics"] = _json_loads(item.get("metrics"), {})
    item["metadata"] = _json_loads(item.get("metadata"), {})
    item["start_time"] = _dt(item.get("start_time"))
    item["end_time"] = _dt(item.get("end_time"))
    item["created_at"] = _dt(item.get("created_at")) or _utc_now()
    item["updated_at"] = _dt(item.get("updated_at")) or item["created_at"]
    return BacktestRun(**item)


def _trade_from_row(row: Any) -> BacktestTrade:
    item = dict(row)
    item["metadata"] = _json_loads(item.get("metadata"), {})
    item["entry_time"] = _dt(item.get("entry_time"))
    item["exit_time"] = _dt(item.get("exit_time"))
    item["created_at"] = _dt(item.get("created_at")) or _utc_now()
    return BacktestTrade(**item)


def _equity_from_row(row: Any) -> BacktestEquityPoint:
    item = dict(row)
    item["metadata"] = _json_loads(item.get("metadata"), {})
    item["timestamp"] = _dt(item.get("timestamp")) or _utc_now()
    return BacktestEquityPoint(**item)


def _result_from_row(row: Any) -> SkillBacktestResult:
    item = dict(row)
    item["passed"] = bool(item.get("passed"))
    item["reasons"] = _json_loads(item.get("reasons"), [])
    item["metadata"] = _json_loads(item.get("metadata"), {})
    item["created_at"] = _dt(item.get("created_at")) or _utc_now()
    return SkillBacktestResult(**item)


def upsert_market_data_bars(db, bars: List[MarketDataBar]) -> List[MarketDataBar]:
    now = _utc_now()
    normalized = [bar.model_copy(update={"symbol": bar.symbol.upper(), "created_at": bar.created_at or now}) for bar in bars]
    if not normalized:
        return []
    with db.connection_scope() as conn:
        cursor = db.get_cursor(conn)
        try:
            for bar in normalized:
                if db.db_type == "sqlite":
                    cursor.execute(
                        """
                        INSERT OR REPLACE INTO market_data_bars
                        (symbol, timeframe, bar_time, open, high, low, close, volume, source, metadata, created_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (bar.symbol, bar.timeframe, bar.bar_time, bar.open, bar.high, bar.low, bar.close, bar.volume, bar.source, _json_dumps(bar.metadata), bar.created_at),
                    )
                else:
                    cursor.execute(
                        """
                        INSERT INTO market_data_bars
                        (symbol, timeframe, bar_time, open, high, low, close, volume, source, metadata, created_at)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (symbol, timeframe, bar_time) DO UPDATE SET
                            open = EXCLUDED.open,
                            high = EXCLUDED.high,
                            low = EXCLUDED.low,
                            close = EXCLUDED.close,
                            volume = EXCLUDED.volume,
                            source = EXCLUDED.source,
                            metadata = EXCLUDED.metadata,
                            created_at = EXCLUDED.created_at
                        """,
                        (bar.symbol, bar.timeframe, bar.bar_time, bar.open, bar.high, bar.low, bar.close, bar.volume, bar.source, _json_dumps(bar.metadata), bar.created_at),
                    )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cursor.close()
    return normalized


def list_market_data_bars(db, *, symbol: str, timeframe: str = "1d", start_time: Optional[datetime] = None, end_time: Optional[datetime] = None, limit: int = 5000) -> List[MarketDataBar]:
    query = f"SELECT * FROM market_data_bars WHERE symbol = {db.param_style} AND timeframe = {db.param_style}"
    params: List[Any] = [symbol.upper(), timeframe]
    if start_time:
        query += f" AND bar_time >= {db.param_style}"
        params.append(start_time)
    if end_time:
        query += f" AND bar_time <= {db.param_style}"
        params.append(end_time)
    query += f" ORDER BY bar_time ASC LIMIT {db.param_style}"
    params.append(limit)
    with db.connection_scope() as conn:
        cursor = db.get_cursor(conn)
        try:
            cursor.execute(query, tuple(params))
            return [_bar_from_row(row) for row in (cursor.fetchall() or [])]
        finally:
            cursor.close()


def create_backtest_run_detail(db, body) -> BacktestRunDetail:
    now = _utc_now()
    run_id = body.run_id or str(uuid.uuid4())
    run = BacktestRun(
        **body.model_dump(exclude={"run_id", "trades", "equity_curve", "skill_result", "created_at", "updated_at"}),
        run_id=run_id,
        symbol=body.symbol.upper() if body.symbol else None,
        created_at=body.created_at or now,
        updated_at=body.updated_at or now,
    )
    trades = [trade.model_copy(update={"trade_id": trade.trade_id or str(uuid.uuid4()), "run_id": run_id, "symbol": trade.symbol.upper(), "created_at": trade.created_at or now}) for trade in body.trades]
    equity_curve = [point.model_copy(update={"point_id": point.point_id or str(uuid.uuid4()), "run_id": run_id}) for point in body.equity_curve]
    skill_result = None
    if body.skill_result:
        skill_result = body.skill_result.model_copy(update={"result_id": body.skill_result.result_id or str(uuid.uuid4()), "run_id": run_id, "created_at": body.skill_result.created_at or now})
    elif body.skill_id:
        skill_result = _default_skill_result(run, trades)

    with db.connection_scope() as conn:
        cursor = db.get_cursor(conn)
        try:
            cursor.execute(
                f"""
                INSERT INTO backtest_runs
                (run_id, account_id, skill_id, strategy_id, symbol, timeframe, start_time, end_time,
                 status, engine_version, parameters, metrics, source_agent, metadata, created_at, updated_at)
                VALUES ({db.param_style}, {db.param_style}, {db.param_style}, {db.param_style}, {db.param_style},
                        {db.param_style}, {db.param_style}, {db.param_style}, {db.param_style}, {db.param_style},
                        {db.param_style}, {db.param_style}, {db.param_style}, {db.param_style}, {db.param_style}, {db.param_style})
                """,
                (
                    run.run_id, run.account_id, run.skill_id, run.strategy_id, run.symbol, run.timeframe,
                    run.start_time, run.end_time, run.status, run.engine_version, _json_dumps(run.parameters),
                    _json_dumps(run.metrics), run.source_agent, _json_dumps(run.metadata), run.created_at, run.updated_at,
                ),
            )
            for trade in trades:
                cursor.execute(
                    f"""
                    INSERT INTO backtest_trades
                    (trade_id, run_id, symbol, side, quantity, entry_time, entry_price, exit_time, exit_price,
                     realized_pl, realized_pl_pct, fees, outcome, metadata, created_at)
                    VALUES ({db.param_style}, {db.param_style}, {db.param_style}, {db.param_style}, {db.param_style},
                            {db.param_style}, {db.param_style}, {db.param_style}, {db.param_style}, {db.param_style},
                            {db.param_style}, {db.param_style}, {db.param_style}, {db.param_style}, {db.param_style})
                    """,
                    (trade.trade_id, trade.run_id, trade.symbol, trade.side, trade.quantity, trade.entry_time, trade.entry_price, trade.exit_time, trade.exit_price, trade.realized_pl, trade.realized_pl_pct, trade.fees, trade.outcome, _json_dumps(trade.metadata), trade.created_at),
                )
            for point in equity_curve:
                cursor.execute(
                    f"""
                    INSERT INTO backtest_equity_curve
                    (point_id, run_id, timestamp, equity, drawdown, metadata)
                    VALUES ({db.param_style}, {db.param_style}, {db.param_style}, {db.param_style}, {db.param_style}, {db.param_style})
                    """,
                    (point.point_id, point.run_id, point.timestamp, point.equity, point.drawdown, _json_dumps(point.metadata)),
                )
            if skill_result:
                cursor.execute(
                    f"""
                    INSERT INTO skill_backtest_results
                    (result_id, skill_id, run_id, passed, status, win_rate, profit_factor, expectancy,
                     max_drawdown, total_trades, score, reasons, metadata, created_at)
                    VALUES ({db.param_style}, {db.param_style}, {db.param_style}, {db.param_style}, {db.param_style},
                            {db.param_style}, {db.param_style}, {db.param_style}, {db.param_style}, {db.param_style},
                            {db.param_style}, {db.param_style}, {db.param_style}, {db.param_style})
                    """,
                    (skill_result.result_id, skill_result.skill_id, skill_result.run_id, int(skill_result.passed), skill_result.status, skill_result.win_rate, skill_result.profit_factor, skill_result.expectancy, skill_result.max_drawdown, skill_result.total_trades, skill_result.score, _json_dumps(skill_result.reasons), _json_dumps(skill_result.metadata), skill_result.created_at),
                )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cursor.close()
    return BacktestRunDetail(run=run, trades=trades, equity_curve=equity_curve, skill_result=skill_result)


def _default_skill_result(run: BacktestRun, trades: List[BacktestTrade]) -> SkillBacktestResult:
    metrics = run.metrics or {}
    total_trades = int(metrics.get("total_trades") or len(trades))
    win_rate = _float_or_none(metrics.get("win_rate"))
    profit_factor = _float_or_none(metrics.get("profit_factor"))
    expectancy = _float_or_none(metrics.get("expectancy"))
    max_drawdown = _float_or_none(metrics.get("max_drawdown"))
    score = _score(win_rate=win_rate, profit_factor=profit_factor, expectancy=expectancy, max_drawdown=max_drawdown, total_trades=total_trades)
    passed = bool(total_trades >= 20 and (profit_factor or 0) >= 1.2 and (max_drawdown is None or max_drawdown <= 0.25) and (win_rate is None or win_rate >= 0.45))
    reasons = [
        f"total_trades={total_trades}",
        f"profit_factor={profit_factor}",
        f"max_drawdown={max_drawdown}",
        "auto-generated from backtest run metrics",
    ]
    return SkillBacktestResult(
        result_id=str(uuid.uuid4()),
        skill_id=run.skill_id or "unknown",
        run_id=run.run_id or "unknown",
        passed=passed,
        status="backtest_passed" if passed else "backtest_failed",
        win_rate=win_rate,
        profit_factor=profit_factor,
        expectancy=expectancy,
        max_drawdown=max_drawdown,
        total_trades=total_trades,
        score=score,
        reasons=reasons,
        metadata={"source": "database_agent_auto_gate_v1"},
        created_at=run.created_at,
    )


def _float_or_none(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _score(*, win_rate: Optional[float], profit_factor: Optional[float], expectancy: Optional[float], max_drawdown: Optional[float], total_trades: int) -> float:
    win_rate_score = max(0.0, min(1.0, win_rate if win_rate is not None else 0.0))
    profit_score = max(0.0, min(1.0, ((profit_factor or 0.0) - 0.8) / 1.2))
    expectancy_score = max(0.0, min(1.0, ((expectancy or 0.0) + 1.0) / 2.0))
    drawdown_score = 1.0 - max(0.0, min(1.0, (max_drawdown or 0.0) / 0.5))
    sample_score = max(0.0, min(1.0, total_trades / 50.0))
    return round((0.25 * win_rate_score) + (0.30 * profit_score) + (0.20 * expectancy_score) + (0.15 * drawdown_score) + (0.10 * sample_score), 4)


def get_backtest_run_detail(db, run_id: str) -> Optional[BacktestRunDetail]:
    with db.connection_scope() as conn:
        cursor = db.get_cursor(conn)
        try:
            cursor.execute(f"SELECT * FROM backtest_runs WHERE run_id = {db.param_style}", (run_id,))
            row = cursor.fetchone()
            if not row:
                return None
            run = _run_from_row(row)
            cursor.execute(f"SELECT * FROM backtest_trades WHERE run_id = {db.param_style} ORDER BY entry_time ASC", (run_id,))
            trades = [_trade_from_row(item) for item in (cursor.fetchall() or [])]
            cursor.execute(f"SELECT * FROM backtest_equity_curve WHERE run_id = {db.param_style} ORDER BY timestamp ASC", (run_id,))
            equity_curve = [_equity_from_row(item) for item in (cursor.fetchall() or [])]
            cursor.execute(f"SELECT * FROM skill_backtest_results WHERE run_id = {db.param_style} ORDER BY created_at DESC LIMIT 1", (run_id,))
            result_row = cursor.fetchone()
            skill_result = _result_from_row(result_row) if result_row else None
            return BacktestRunDetail(run=run, trades=trades, equity_curve=equity_curve, skill_result=skill_result)
        finally:
            cursor.close()


def list_skill_backtests(db, skill_id: str, *, limit: int = 50) -> List[SkillBacktestResult]:
    with db.connection_scope() as conn:
        cursor = db.get_cursor(conn)
        try:
            cursor.execute(
                f"SELECT * FROM skill_backtest_results WHERE skill_id = {db.param_style} ORDER BY created_at DESC LIMIT {db.param_style}",
                (skill_id, limit),
            )
            return [_result_from_row(row) for row in (cursor.fetchall() or [])]
        finally:
            cursor.close()


def get_skill_backtest_status(db, skill_id: str) -> SkillBacktestStatus:
    results = list_skill_backtests(db, skill_id, limit=100)
    if not results:
        return SkillBacktestStatus(
            skill_id=skill_id,
            status="not_backtested",
            passed=False,
            total_runs=0,
            reasons=["No backtest result found for this skill."],
        )
    latest = results[0]
    return SkillBacktestStatus(
        skill_id=skill_id,
        status=latest.status,
        passed=latest.passed,
        latest_run_id=latest.run_id,
        latest_score=latest.score,
        latest_profit_factor=latest.profit_factor,
        latest_win_rate=latest.win_rate,
        latest_max_drawdown=latest.max_drawdown,
        total_runs=len(results),
        reasons=latest.reasons,
        updated_at=latest.created_at,
    )
