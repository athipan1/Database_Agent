from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Union

from shadow_observation_models import CreateShadowObservationBody, ShadowObservation


_LIFECYCLE_ORDER = (
    "signal_decision",
    "entry_simulated",
    "mark",
    "exit_simulated",
)


def _json_dumps(value: Any) -> str:
    return json.dumps(value if value is not None else {}, default=str, sort_keys=True)


def _json_loads(value: Any) -> dict:
    if isinstance(value, dict):
        return value
    if value is None:
        return {}
    try:
        return json.loads(value)
    except Exception:
        return {}


def _ensure_event_key_column(db, cursor) -> None:
    if db.db_type == "sqlite":
        cursor.execute("PRAGMA table_info(shadow_observations)")
        columns = {str(row[1]) for row in (cursor.fetchall() or [])}
        if "event_key" not in columns:
            cursor.execute("ALTER TABLE shadow_observations ADD COLUMN event_key TEXT")
    else:
        cursor.execute(
            "ALTER TABLE shadow_observations ADD COLUMN IF NOT EXISTS event_key TEXT"
        )


def setup_shadow_observation_table(db) -> None:
    with db.connection_scope() as conn:
        cursor = db.get_cursor(conn)
        try:
            json_type = "TEXT" if db.db_type == "sqlite" else "JSONB"
            timestamp_type = "TEXT" if db.db_type == "sqlite" else "TIMESTAMPTZ"
            boolean_type = "INTEGER" if db.db_type == "sqlite" else "BOOLEAN"
            cursor.execute(
                f"""
                CREATE TABLE IF NOT EXISTS shadow_observations (
                    event_id TEXT PRIMARY KEY,
                    event_key TEXT,
                    shadow_trade_id TEXT NOT NULL,
                    account_id TEXT NOT NULL,
                    correlation_id TEXT,
                    signal_id TEXT NOT NULL,
                    strategy_id TEXT NOT NULL,
                    strategy_version TEXT,
                    symbol TEXT NOT NULL,
                    side TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    event_time {timestamp_type} NOT NULL,
                    decision_price DOUBLE PRECISION,
                    bid DOUBLE PRECISION,
                    ask DOUBLE PRECISION,
                    spread_bps DOUBLE PRECISION,
                    simulated_fill_price DOUBLE PRECISION,
                    simulated_slippage_bps DOUBLE PRECISION,
                    stop_loss DOUBLE PRECISION,
                    take_profit DOUBLE PRECISION,
                    market_regime TEXT,
                    scanner_score DOUBLE PRECISION,
                    opportunity_score DOUBLE PRECISION,
                    mfe_pct DOUBLE PRECISION,
                    mae_pct DOUBLE PRECISION,
                    exit_price DOUBLE PRECISION,
                    exit_reason TEXT,
                    gross_return_pct DOUBLE PRECISION,
                    estimated_cost_pct DOUBLE PRECISION,
                    net_return_pct DOUBLE PRECISION,
                    holding_period_seconds DOUBLE PRECISION,
                    source_commit_sha TEXT,
                    execution_mode TEXT NOT NULL,
                    broker_order_authorized {boolean_type} NOT NULL,
                    metadata {json_type} NOT NULL,
                    created_at {timestamp_type} NOT NULL
                )
                """
            )
            _ensure_event_key_column(db, cursor)
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_shadow_trade_events "
                "ON shadow_observations(shadow_trade_id, event_time ASC)"
            )
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_shadow_account_time "
                "ON shadow_observations(account_id, event_time DESC)"
            )
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_shadow_symbol_time "
                "ON shadow_observations(symbol, event_time DESC)"
            )
            cursor.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_shadow_trade_event_key "
                "ON shadow_observations(shadow_trade_id, event_type, event_key) "
                "WHERE event_key IS NOT NULL"
            )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cursor.close()


def _trade_id(body: CreateShadowObservationBody, correlation_id: Optional[str]) -> str:
    if body.shadow_trade_id:
        return body.shadow_trade_id
    seed = "|".join(
        [
            str(body.account_id),
            body.correlation_id or correlation_id or "unknown",
            body.signal_id,
            body.strategy_id,
            body.symbol.upper(),
        ]
    )
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"shadow-trade:{seed}"))


def _event_id(body: CreateShadowObservationBody, trade_id: str) -> str:
    if body.event_id:
        return body.event_id
    discriminator = body.event_key
    if not discriminator:
        discriminator = body.event_time.isoformat() if body.event_time else "initial"
    seed = f"{trade_id}|{body.event_type}|{discriminator}"
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"shadow-event:{seed}"))


def _row_to_model(row: Any) -> ShadowObservation:
    item = dict(row)
    item["broker_order_authorized"] = bool(item.get("broker_order_authorized"))
    item["metadata"] = _json_loads(item.get("metadata"))
    return ShadowObservation(**item)


def get_shadow_observation(db, event_id: str) -> Optional[ShadowObservation]:
    setup_shadow_observation_table(db)
    with db.connection_scope() as conn:
        cursor = db.get_cursor(conn)
        try:
            cursor.execute(
                f"SELECT * FROM shadow_observations WHERE event_id = {db.param_style}",
                (event_id,),
            )
            row = cursor.fetchone()
            return _row_to_model(row) if row else None
        finally:
            cursor.close()


def _existing_trade_events(db, shadow_trade_id: str) -> List[ShadowObservation]:
    setup_shadow_observation_table(db)
    with db.connection_scope() as conn:
        cursor = db.get_cursor(conn)
        try:
            cursor.execute(
                f"SELECT * FROM shadow_observations WHERE shadow_trade_id = {db.param_style} "
                "ORDER BY event_time ASC, event_id ASC",
                (shadow_trade_id,),
            )
            return [_row_to_model(row) for row in (cursor.fetchall() or [])]
        finally:
            cursor.close()


def _validate_lifecycle_transition(
    db,
    body: CreateShadowObservationBody,
    trade_id: str,
) -> None:
    events = _existing_trade_events(db, trade_id)
    types = [event.event_type for event in events]
    event_type = body.event_type

    if "exit_simulated" in types:
        raise ValueError("shadow lifecycle is already closed")
    if event_type == "signal_decision":
        if events:
            raise ValueError("shadow signal_decision must be the first lifecycle event")
        return
    if "signal_decision" not in types:
        raise ValueError("shadow lifecycle requires signal_decision before later events")
    if event_type == "entry_simulated":
        if "entry_simulated" in types:
            raise ValueError("shadow lifecycle already contains entry_simulated")
        return
    if "entry_simulated" not in types:
        raise ValueError("shadow lifecycle requires entry_simulated before mark/exit")
    if event_type == "mark":
        return
    if event_type == "exit_simulated":
        if "mark" not in types:
            raise ValueError("shadow lifecycle requires at least one mark before exit_simulated")
        return
    raise ValueError(f"unsupported shadow lifecycle event: {event_type}")


def create_shadow_observation(
    db,
    body: CreateShadowObservationBody,
    *,
    correlation_id: Optional[str] = None,
) -> ShadowObservation:
    setup_shadow_observation_table(db)
    trade_id = _trade_id(body, correlation_id)
    event_id = _event_id(body, trade_id)
    existing = get_shadow_observation(db, event_id)
    if existing:
        return existing

    _validate_lifecycle_transition(db, body, trade_id)
    now = datetime.now(timezone.utc)
    record = ShadowObservation(
        **body.model_dump(
            exclude={"event_id", "shadow_trade_id", "correlation_id", "event_time"}
        ),
        event_id=event_id,
        shadow_trade_id=trade_id,
        correlation_id=body.correlation_id or correlation_id,
        event_time=body.event_time or now,
        created_at=now,
    )
    fields = [
        "event_id", "event_key", "shadow_trade_id", "account_id", "correlation_id",
        "signal_id", "strategy_id", "strategy_version", "symbol", "side", "event_type",
        "event_time", "decision_price", "bid", "ask", "spread_bps",
        "simulated_fill_price", "simulated_slippage_bps", "stop_loss", "take_profit",
        "market_regime", "scanner_score", "opportunity_score", "mfe_pct", "mae_pct",
        "exit_price", "exit_reason", "gross_return_pct", "estimated_cost_pct",
        "net_return_pct", "holding_period_seconds", "source_commit_sha", "execution_mode",
        "broker_order_authorized", "metadata", "created_at",
    ]
    values = record.model_dump()
    values["account_id"] = str(record.account_id)
    values["metadata"] = _json_dumps(record.metadata)
    insert_keyword = "INSERT OR IGNORE" if db.db_type == "sqlite" else "INSERT"
    conflict_clause = "" if db.db_type == "sqlite" else " ON CONFLICT DO NOTHING"
    placeholders = ", ".join([db.param_style] * len(fields))
    with db.connection_scope() as conn:
        cursor = db.get_cursor(conn)
        try:
            cursor.execute(
                f"{insert_keyword} INTO shadow_observations ({', '.join(fields)}) "
                f"VALUES ({placeholders}){conflict_clause}",
                tuple(values[field] for field in fields),
            )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cursor.close()

    persisted = get_shadow_observation(db, event_id)
    if not persisted:
        raise RuntimeError("Shadow observation was not persisted")
    return persisted


def list_shadow_observations(
    db,
    *,
    account_id: Optional[Union[int, str]] = None,
    shadow_trade_id: Optional[str] = None,
    symbol: Optional[str] = None,
    event_type: Optional[str] = None,
    limit: int = 100,
    offset: int = 0,
) -> List[ShadowObservation]:
    setup_shadow_observation_table(db)
    query = "SELECT * FROM shadow_observations WHERE 1=1"
    params: List[Any] = []
    for column, value in (
        ("account_id", str(account_id) if account_id is not None else None),
        ("shadow_trade_id", shadow_trade_id),
        ("symbol", symbol.upper() if symbol else None),
        ("event_type", event_type),
    ):
        if value is not None:
            query += f" AND {column} = {db.param_style}"
            params.append(value)
    query += f" ORDER BY event_time DESC, event_id DESC LIMIT {db.param_style} OFFSET {db.param_style}"
    params.extend([max(1, int(limit)), max(0, int(offset))])
    with db.connection_scope() as conn:
        cursor = db.get_cursor(conn)
        try:
            cursor.execute(query, tuple(params))
            return [_row_to_model(row) for row in (cursor.fetchall() or [])]
        finally:
            cursor.close()


def shadow_trade_lifecycle(db, shadow_trade_id: str) -> Dict[str, Any]:
    events = _existing_trade_events(db, shadow_trade_id)
    event_types = [event.event_type for event in events]
    closed = "exit_simulated" in event_types
    return {
        "shadow_trade_id": shadow_trade_id,
        "event_count": len(events),
        "event_types": event_types,
        "closed": closed,
        "next_expected": (
            None
            if closed
            else "signal_decision"
            if not event_types
            else "entry_simulated"
            if "entry_simulated" not in event_types
            else "mark_or_exit"
        ),
        "events": [event.model_dump(mode="json") for event in events],
    }


def list_closed_shadow_outcomes(
    db,
    *,
    account_id: Optional[Union[int, str]] = None,
    limit: int = 1000,
    offset: int = 0,
) -> List[Dict[str, Any]]:
    exits = list_shadow_observations(
        db,
        account_id=account_id,
        event_type="exit_simulated",
        limit=limit,
        offset=offset,
    )
    outcomes: List[Dict[str, Any]] = []
    for event in exits:
        outcomes.append(
            {
                "shadow_trade_id": event.shadow_trade_id,
                "symbol": event.symbol,
                "strategy": event.strategy_id,
                "regime": event.market_regime or "unknown",
                "side": event.side,
                "mfe_pct": float(event.mfe_pct or 0.0),
                "mae_pct": float(event.mae_pct or 0.0),
                "gross_return_pct": float(event.gross_return_pct or 0.0),
                "estimated_cost_pct": float(event.estimated_cost_pct or 0.0),
                "net_return_pct": float(event.net_return_pct or 0.0),
                "holding_period_seconds": event.holding_period_seconds,
            }
        )
    return outcomes
