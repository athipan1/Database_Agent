from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Union

from skill_performance_models import (
    CreateSkillExecutionLogBody,
    CreateSkillTradeOutcomeBody,
    SkillExecutionLog,
    SkillPerformanceSummary,
    SkillTradeOutcome,
)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _json_dumps(value: Dict[str, Any]) -> str:
    return json.dumps(value or {}, default=str, sort_keys=True)


def _json_loads(value: Any) -> Dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, dict):
        return value
    try:
        return json.loads(value)
    except Exception:
        return {}


def _dt(value: Any) -> Optional[datetime]:
    if value is None or isinstance(value, datetime):
        return value
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except Exception:
        return None


def _float_or_zero(value: Any) -> float:
    try:
        if value is None:
            return 0.0
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _existing_columns(cursor, db_type: str, table_name: str) -> set[str]:
    if db_type == "sqlite":
        cursor.execute(f"PRAGMA table_info({table_name})")
        return {str(row[1]) for row in (cursor.fetchall() or [])}

    cursor.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = current_schema() AND table_name = %s
        """,
        (table_name,),
    )
    return {str(row[0]) for row in (cursor.fetchall() or [])}


def _ensure_columns(cursor, db_type: str, table_name: str, columns: Dict[str, str]) -> None:
    existing = _existing_columns(cursor, db_type, table_name)
    for column_name, column_type in columns.items():
        if column_name in existing:
            continue
        cursor.execute(
            f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type}"
        )
        existing.add(column_name)


def setup_skill_performance_tables(db) -> None:
    with db.connection_scope() as conn:
        cursor = db.get_cursor(conn)
        try:
            json_type = "TEXT" if db.db_type == "sqlite" else "JSONB"
            timestamp_type = "TEXT" if db.db_type == "sqlite" else "TIMESTAMPTZ"
            numeric_type = "REAL" if db.db_type == "sqlite" else "DOUBLE PRECISION"

            cursor.execute(
                f"""
                CREATE TABLE IF NOT EXISTS skill_execution_logs (
                    execution_log_id TEXT PRIMARY KEY,
                    account_id TEXT NOT NULL,
                    skill_id TEXT NOT NULL,
                    skill_name TEXT,
                    symbol TEXT,
                    strategy_bucket TEXT,
                    market_regime TEXT,
                    signal TEXT,
                    confidence {numeric_type},
                    reason TEXT,
                    input_payload {json_type} NOT NULL,
                    output_payload {json_type} NOT NULL,
                    execution_status TEXT NOT NULL,
                    error TEXT,
                    elapsed_ms {numeric_type},
                    source_agent TEXT NOT NULL,
                    run_id TEXT,
                    metadata {json_type} NOT NULL,
                    created_at {timestamp_type} NOT NULL
                );
                """
            )
            cursor.execute(
                f"""
                CREATE TABLE IF NOT EXISTS skill_trade_outcomes (
                    outcome_id TEXT PRIMARY KEY,
                    execution_log_id TEXT NOT NULL,
                    skill_id TEXT,
                    account_id TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    strategy_bucket TEXT,
                    market_regime TEXT,
                    entry_price {numeric_type},
                    exit_price {numeric_type},
                    realized_pl {numeric_type},
                    realized_pl_pct {numeric_type},
                    holding_minutes INTEGER,
                    max_favorable_excursion {numeric_type},
                    max_adverse_excursion {numeric_type},
                    outcome TEXT,
                    source_agent TEXT NOT NULL,
                    metadata {json_type} NOT NULL,
                    closed_at {timestamp_type},
                    created_at {timestamp_type} NOT NULL
                );
                """
            )

            _ensure_columns(
                cursor,
                db.db_type,
                "skill_execution_logs",
                {
                    "execution_log_id": "TEXT",
                    "account_id": "TEXT",
                    "skill_id": "TEXT",
                    "skill_name": "TEXT",
                    "symbol": "TEXT",
                    "strategy_bucket": "TEXT",
                    "market_regime": "TEXT",
                    "signal": "TEXT",
                    "confidence": numeric_type,
                    "reason": "TEXT",
                    "input_payload": json_type,
                    "output_payload": json_type,
                    "execution_status": "TEXT",
                    "error": "TEXT",
                    "elapsed_ms": numeric_type,
                    "source_agent": "TEXT",
                    "run_id": "TEXT",
                    "metadata": json_type,
                    "created_at": timestamp_type,
                },
            )
            _ensure_columns(
                cursor,
                db.db_type,
                "skill_trade_outcomes",
                {
                    "outcome_id": "TEXT",
                    "execution_log_id": "TEXT",
                    "skill_id": "TEXT",
                    "account_id": "TEXT",
                    "symbol": "TEXT",
                    "strategy_bucket": "TEXT",
                    "market_regime": "TEXT",
                    "entry_price": numeric_type,
                    "exit_price": numeric_type,
                    "realized_pl": numeric_type,
                    "realized_pl_pct": numeric_type,
                    "holding_minutes": "INTEGER",
                    "max_favorable_excursion": numeric_type,
                    "max_adverse_excursion": numeric_type,
                    "outcome": "TEXT",
                    "source_agent": "TEXT",
                    "metadata": json_type,
                    "closed_at": timestamp_type,
                    "created_at": timestamp_type,
                },
            )

            if db.db_type == "postgres":
                cursor.execute(
                    "CREATE INDEX IF NOT EXISTS idx_skill_execution_logs_skill_context "
                    "ON skill_execution_logs(skill_id, account_id, symbol, strategy_bucket, market_regime, created_at DESC)"
                )
                cursor.execute(
                    "CREATE INDEX IF NOT EXISTS idx_skill_trade_outcomes_log "
                    "ON skill_trade_outcomes(execution_log_id)"
                )
                cursor.execute(
                    "CREATE INDEX IF NOT EXISTS idx_skill_trade_outcomes_skill_context "
                    "ON skill_trade_outcomes(skill_id, account_id, symbol, strategy_bucket, market_regime, created_at DESC)"
                )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cursor.close()


def _execution_log_from_row(row: Any) -> SkillExecutionLog:
    item = dict(row)
    item["input_payload"] = _json_loads(item.get("input_payload"))
    item["output_payload"] = _json_loads(item.get("output_payload"))
    item["metadata"] = _json_loads(item.get("metadata"))
    item["created_at"] = _dt(item.get("created_at")) or _utc_now()
    return SkillExecutionLog(**item)


def _trade_outcome_from_row(row: Any) -> SkillTradeOutcome:
    item = dict(row)
    item["metadata"] = _json_loads(item.get("metadata"))
    item["closed_at"] = _dt(item.get("closed_at"))
    item["created_at"] = _dt(item.get("created_at")) or _utc_now()
    return SkillTradeOutcome(**item)


def create_skill_execution_log(db, body: CreateSkillExecutionLogBody) -> SkillExecutionLog:
    created_at = body.created_at or _utc_now()
    record = SkillExecutionLog(
        **body.model_dump(
            exclude={"execution_log_id", "created_at", "symbol"}
        ),
        execution_log_id=body.execution_log_id or str(uuid.uuid4()),
        symbol=body.symbol.upper() if body.symbol else None,
        created_at=created_at,
    )
    with db.connection_scope() as conn:
        cursor = db.get_cursor(conn)
        try:
            cursor.execute(
                f"""
                INSERT INTO skill_execution_logs
                (execution_log_id, account_id, skill_id, skill_name, symbol, strategy_bucket,
                 market_regime, signal, confidence, reason, input_payload, output_payload,
                 execution_status, error, elapsed_ms, source_agent, run_id, metadata, created_at)
                VALUES ({db.param_style}, {db.param_style}, {db.param_style}, {db.param_style}, {db.param_style},
                        {db.param_style}, {db.param_style}, {db.param_style}, {db.param_style}, {db.param_style},
                        {db.param_style}, {db.param_style}, {db.param_style}, {db.param_style}, {db.param_style},
                        {db.param_style}, {db.param_style}, {db.param_style}, {db.param_style})
                """,
                (
                    record.execution_log_id,
                    str(record.account_id),
                    record.skill_id,
                    record.skill_name,
                    record.symbol,
                    record.strategy_bucket,
                    record.market_regime,
                    record.signal,
                    record.confidence,
                    record.reason,
                    _json_dumps(record.input_payload),
                    _json_dumps(record.output_payload),
                    record.execution_status,
                    record.error,
                    record.elapsed_ms,
                    record.source_agent,
                    record.run_id,
                    _json_dumps(record.metadata),
                    record.created_at,
                ),
            )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cursor.close()
    return record


def list_skill_execution_logs(
    db,
    *,
    skill_id: Optional[str] = None,
    account_id: Optional[Union[int, str]] = None,
    symbol: Optional[str] = None,
    strategy_bucket: Optional[str] = None,
    market_regime: Optional[str] = None,
    limit: int = 100,
    offset: int = 0,
) -> List[SkillExecutionLog]:
    query = "SELECT * FROM skill_execution_logs WHERE 1=1"
    params: List[Any] = []
    if skill_id:
        query += f" AND skill_id = {db.param_style}"
        params.append(skill_id)
    if account_id is not None:
        query += f" AND account_id = {db.param_style}"
        params.append(str(account_id))
    if symbol:
        query += f" AND symbol = {db.param_style}"
        params.append(symbol.upper())
    if strategy_bucket:
        query += f" AND strategy_bucket = {db.param_style}"
        params.append(strategy_bucket)
    if market_regime:
        query += f" AND market_regime = {db.param_style}"
        params.append(market_regime)
    query += f" ORDER BY created_at DESC LIMIT {db.param_style} OFFSET {db.param_style}"
    params.extend([limit, offset])
    with db.connection_scope() as conn:
        cursor = db.get_cursor(conn)
        try:
            cursor.execute(query, tuple(params))
            return [_execution_log_from_row(row) for row in (cursor.fetchall() or [])]
        finally:
            cursor.close()


def create_skill_trade_outcome(db, body: CreateSkillTradeOutcomeBody) -> SkillTradeOutcome:
    created_at = _utc_now()
    record = SkillTradeOutcome(
        **body.model_dump(
            exclude={"outcome_id", "symbol", "closed_at"}
        ),
        outcome_id=body.outcome_id or str(uuid.uuid4()),
        symbol=body.symbol.upper(),
        closed_at=body.closed_at,
        created_at=created_at,
    )
    with db.connection_scope() as conn:
        cursor = db.get_cursor(conn)
        try:
            cursor.execute(
                f"""
                INSERT INTO skill_trade_outcomes
                (outcome_id, execution_log_id, skill_id, account_id, symbol, strategy_bucket,
                 market_regime, entry_price, exit_price, realized_pl, realized_pl_pct,
                 holding_minutes, max_favorable_excursion, max_adverse_excursion, outcome,
                 source_agent, metadata, closed_at, created_at)
                VALUES ({db.param_style}, {db.param_style}, {db.param_style}, {db.param_style}, {db.param_style},
                        {db.param_style}, {db.param_style}, {db.param_style}, {db.param_style}, {db.param_style},
                        {db.param_style}, {db.param_style}, {db.param_style}, {db.param_style}, {db.param_style},
                        {db.param_style}, {db.param_style}, {db.param_style}, {db.param_style})
                """,
                (
                    record.outcome_id,
                    record.execution_log_id,
                    record.skill_id,
                    str(record.account_id),
                    record.symbol,
                    record.strategy_bucket,
                    record.market_regime,
                    record.entry_price,
                    record.exit_price,
                    record.realized_pl,
                    record.realized_pl_pct,
                    record.holding_minutes,
                    record.max_favorable_excursion,
                    record.max_adverse_excursion,
                    record.outcome,
                    record.source_agent,
                    _json_dumps(record.metadata),
                    record.closed_at,
                    record.created_at,
                ),
            )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cursor.close()
    return record


def _skill_score(*, win_rate: float, avg_return: float, avg_confidence: float, completed: int) -> float:
    sample_factor = min(1.0, completed / 20.0) if completed else 0.15
    return round(
        max(0.0, min(1.0, (0.45 * win_rate) + (0.25 * max(0.0, min(1.0, avg_return / 10.0 + 0.5))) + (0.20 * avg_confidence) + (0.10 * sample_factor))),
        4,
    )


def rank_skill_performance(
    db,
    *,
    account_id: Optional[Union[int, str]] = None,
    symbol: Optional[str] = None,
    strategy_bucket: Optional[str] = None,
    market_regime: Optional[str] = None,
    limit: int = 20,
) -> List[SkillPerformanceSummary]:
    logs = list_skill_execution_logs(
        db,
        account_id=account_id,
        symbol=symbol,
        strategy_bucket=strategy_bucket,
        market_regime=market_regime,
        limit=5000,
    )
    if not logs:
        return []

    log_ids = {log.execution_log_id for log in logs}
    outcomes: List[SkillTradeOutcome] = []
    with db.connection_scope() as conn:
        cursor = db.get_cursor(conn)
        try:
            if log_ids:
                placeholders = ", ".join([db.param_style] * len(log_ids))
                cursor.execute(
                    f"SELECT * FROM skill_trade_outcomes WHERE execution_log_id IN ({placeholders})",
                    tuple(log_ids),
                )
                outcomes = [_trade_outcome_from_row(row) for row in (cursor.fetchall() or [])]
        finally:
            cursor.close()

    outcomes_by_log: Dict[str, SkillTradeOutcome] = {item.execution_log_id: item for item in outcomes}
    grouped: Dict[str, Dict[str, Any]] = {}
    for log in logs:
        group = grouped.setdefault(
            log.skill_id,
            {
                "skill_id": log.skill_id,
                "skill_name": log.skill_name,
                "executions": [],
                "outcomes": [],
            },
        )
        group["skill_name"] = group.get("skill_name") or log.skill_name
        group["executions"].append(log)
        outcome = outcomes_by_log.get(log.execution_log_id)
        if outcome:
            group["outcomes"].append(outcome)

    summaries: List[SkillPerformanceSummary] = []
    for group in grouped.values():
        executions: List[SkillExecutionLog] = group["executions"]
        completed: List[SkillTradeOutcome] = group["outcomes"]
        wins = sum(1 for item in completed if (item.realized_pl or 0) > 0 or str(item.outcome or "").lower() in {"win", "take_profit_hit"})
        losses = sum(1 for item in completed if (item.realized_pl or 0) < 0 or str(item.outcome or "").lower() in {"loss", "stopped_out"})
        completed_count = len(completed)
        win_rate = round(wins / completed_count, 4) if completed_count else 0.0
        avg_confidence = round(sum(_float_or_zero(item.confidence) for item in executions) / len(executions), 4)
        avg_return = round(sum(_float_or_zero(item.realized_pl_pct) for item in completed) / completed_count, 4) if completed_count else 0.0
        total_pl = round(sum(_float_or_zero(item.realized_pl) for item in completed), 4)
        expectancy = round(total_pl / completed_count, 4) if completed_count else 0.0
        summaries.append(
            SkillPerformanceSummary(
                skill_id=group["skill_id"],
                skill_name=group.get("skill_name"),
                account_id=str(account_id) if account_id is not None else None,
                symbol=symbol.upper() if symbol else None,
                strategy_bucket=strategy_bucket,
                market_regime=market_regime,
                execution_count=len(executions),
                completed_outcomes=completed_count,
                wins=wins,
                losses=losses,
                win_rate=win_rate,
                avg_confidence=avg_confidence,
                avg_realized_pl_pct=avg_return,
                total_realized_pl=total_pl,
                expectancy=expectancy,
                skill_score=_skill_score(
                    win_rate=win_rate,
                    avg_return=avg_return,
                    avg_confidence=avg_confidence,
                    completed=completed_count,
                ),
                last_execution_at=max(item.created_at for item in executions),
                reasons=[
                    f"{len(executions)} executions",
                    f"{completed_count} completed outcomes",
                    "score is conservative until at least 20 completed outcomes",
                ],
            )
        )
    summaries.sort(key=lambda item: (item.skill_score, item.completed_outcomes, item.execution_count), reverse=True)
    return summaries[:limit]
