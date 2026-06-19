import json
import uuid
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict, List, Optional, Union

from models import SignalHistory, CreateSignalHistoryBody, PerformanceMetric, CreatePerformanceMetricBody


def _json_dumps(value: Dict[str, Any]) -> str:
    return json.dumps(value or {}, default=str)


def _json_loads(value: Any) -> Dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, dict):
        return value
    try:
        return json.loads(value)
    except Exception:
        return {}


def setup_history_tables(db) -> None:
    with db.connection_scope() as conn:
        cursor = db.get_cursor(conn)
        try:
            json_type = "TEXT" if db.db_type == "sqlite" else "JSONB"
            timestamp_type = "TEXT" if db.db_type == "sqlite" else "TIMESTAMPTZ"
            numeric_type = "TEXT" if db.db_type == "sqlite" else "NUMERIC(18, 5)"
            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS signal_history (
                    signal_id TEXT PRIMARY KEY,
                    account_id TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    timestamp {timestamp_type} NOT NULL,
                    source_agent TEXT NOT NULL,
                    candidate_score DOUBLE PRECISION,
                    technical_score DOUBLE PRECISION,
                    fundamental_score DOUBLE PRECISION,
                    final_verdict TEXT,
                    market_regime TEXT,
                    metadata {json_type} NOT NULL
                );
            """)
            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS performance_metrics (
                    metric_id TEXT PRIMARY KEY,
                    account_id TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    timestamp {timestamp_type} NOT NULL,
                    source_agent TEXT NOT NULL,
                    entry_price {numeric_type},
                    exit_price {numeric_type},
                    current_price {numeric_type},
                    return_pct DOUBLE PRECISION,
                    holding_days INTEGER,
                    outcome TEXT,
                    metadata {json_type} NOT NULL
                );
            """)
            if db.db_type == "postgres":
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_signal_history_account_symbol_time ON signal_history(account_id, symbol, timestamp DESC)")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_performance_metrics_account_symbol_time ON performance_metrics(account_id, symbol, timestamp DESC)")
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cursor.close()


def create_signal_record(db, body: CreateSignalHistoryBody) -> SignalHistory:
    record = SignalHistory(
        signal_id=body.signal_id or str(uuid.uuid4()),
        account_id=body.account_id,
        symbol=body.symbol.upper(),
        timestamp=datetime.now(timezone.utc),
        source_agent=body.source_agent,
        candidate_score=body.candidate_score,
        technical_score=body.technical_score,
        fundamental_score=body.fundamental_score,
        final_verdict=body.final_verdict,
        market_regime=body.market_regime,
        metadata=body.metadata,
    )
    with db.connection_scope() as conn:
        cursor = db.get_cursor(conn)
        try:
            cursor.execute(
                f"""
                INSERT INTO signal_history
                (signal_id, account_id, symbol, timestamp, source_agent, candidate_score, technical_score, fundamental_score, final_verdict, market_regime, metadata)
                VALUES ({db.param_style}, {db.param_style}, {db.param_style}, {db.param_style}, {db.param_style}, {db.param_style}, {db.param_style}, {db.param_style}, {db.param_style}, {db.param_style}, {db.param_style})
                """,
                (
                    record.signal_id,
                    str(record.account_id),
                    record.symbol,
                    record.timestamp,
                    record.source_agent,
                    record.candidate_score,
                    record.technical_score,
                    record.fundamental_score,
                    record.final_verdict,
                    record.market_regime,
                    _json_dumps(record.metadata),
                ),
            )
            conn.commit()
            return record
        except Exception:
            conn.rollback()
            raise
        finally:
            cursor.close()


def get_signal_records(db, account_id: Optional[Union[int, str]] = None, symbol: Optional[str] = None, limit: int = 100, offset: int = 0) -> List[SignalHistory]:
    query = "SELECT * FROM signal_history WHERE 1=1"
    params = []
    if account_id is not None:
        query += f" AND account_id = {db.param_style}"
        params.append(str(account_id))
    if symbol:
        query += f" AND symbol = {db.param_style}"
        params.append(symbol.upper())
    query += f" ORDER BY timestamp DESC LIMIT {db.param_style} OFFSET {db.param_style}"
    params.extend([limit, offset])
    with db.connection_scope() as conn:
        cursor = db.get_cursor(conn)
        try:
            cursor.execute(query, tuple(params))
            rows = cursor.fetchall() or []
            return [SignalHistory(**{**dict(row), "metadata": _json_loads(dict(row).get("metadata"))}) for row in rows]
        finally:
            cursor.close()


def _decimal_or_none(value: Any) -> Optional[Decimal]:
    return None if value is None else Decimal(str(value))


def create_performance_record(db, body: CreatePerformanceMetricBody) -> PerformanceMetric:
    record = PerformanceMetric(
        metric_id=body.metric_id or str(uuid.uuid4()),
        account_id=body.account_id,
        symbol=body.symbol.upper(),
        timestamp=datetime.now(timezone.utc),
        source_agent=body.source_agent,
        entry_price=body.entry_price,
        exit_price=body.exit_price,
        current_price=body.current_price,
        return_pct=body.return_pct,
        holding_days=body.holding_days,
        outcome=body.outcome,
        metadata=body.metadata,
    )
    with db.connection_scope() as conn:
        cursor = db.get_cursor(conn)
        try:
            cursor.execute(
                f"""
                INSERT INTO performance_metrics
                (metric_id, account_id, symbol, timestamp, source_agent, entry_price, exit_price, current_price, return_pct, holding_days, outcome, metadata)
                VALUES ({db.param_style}, {db.param_style}, {db.param_style}, {db.param_style}, {db.param_style}, {db.param_style}, {db.param_style}, {db.param_style}, {db.param_style}, {db.param_style}, {db.param_style}, {db.param_style})
                """,
                (
                    record.metric_id,
                    str(record.account_id),
                    record.symbol,
                    record.timestamp,
                    record.source_agent,
                    str(record.entry_price) if record.entry_price is not None else None,
                    str(record.exit_price) if record.exit_price is not None else None,
                    str(record.current_price) if record.current_price is not None else None,
                    record.return_pct,
                    record.holding_days,
                    record.outcome,
                    _json_dumps(record.metadata),
                ),
            )
            conn.commit()
            return record
        except Exception:
            conn.rollback()
            raise
        finally:
            cursor.close()


def get_performance_records(db, account_id: Optional[Union[int, str]] = None, symbol: Optional[str] = None, limit: int = 100, offset: int = 0) -> List[PerformanceMetric]:
    query = "SELECT * FROM performance_metrics WHERE 1=1"
    params = []
    if account_id is not None:
        query += f" AND account_id = {db.param_style}"
        params.append(str(account_id))
    if symbol:
        query += f" AND symbol = {db.param_style}"
        params.append(symbol.upper())
    query += f" ORDER BY timestamp DESC LIMIT {db.param_style} OFFSET {db.param_style}"
    params.extend([limit, offset])
    with db.connection_scope() as conn:
        cursor = db.get_cursor(conn)
        try:
            cursor.execute(query, tuple(params))
            rows = cursor.fetchall() or []
            result = []
            for row in rows:
                item = dict(row)
                item["metadata"] = _json_loads(item.get("metadata"))
                item["entry_price"] = _decimal_or_none(item.get("entry_price"))
                item["exit_price"] = _decimal_or_none(item.get("exit_price"))
                item["current_price"] = _decimal_or_none(item.get("current_price"))
                result.append(PerformanceMetric(**item))
            return result
        finally:
            cursor.close()
