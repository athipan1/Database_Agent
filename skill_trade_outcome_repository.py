from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Optional

try:
    from psycopg2.extras import Json as PgJson
except Exception:  # pragma: no cover
    PgJson = None


def _param(db) -> str:
    return db.param_style


def _now(db):
    return datetime.now(timezone.utc).isoformat() if db.db_type == "sqlite" else datetime.now(timezone.utc)


def _json_payload(value: Any, db) -> Any:
    payload = value or {}
    if db.db_type == "sqlite":
        return json.dumps(payload, ensure_ascii=False, default=str)
    if PgJson is not None:
        return PgJson(payload, dumps=lambda obj: json.dumps(obj, ensure_ascii=False, default=str))
    return json.dumps(payload, ensure_ascii=False, default=str)


def _json_load(value: Any, default: Any) -> Any:
    if value is None:
        return default
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(value)
    except Exception:
        return default


def _row_to_dict(row: Any) -> Dict[str, Any]:
    if row is None:
        return {}
    return dict(row)


def setup_skill_trade_outcome_table(db) -> None:
    timestamp_type = "TEXT" if db.db_type == "sqlite" else "TIMESTAMPTZ"
    json_type = "TEXT" if db.db_type == "sqlite" else "JSONB"
    with db.connection_scope() as conn:
        cursor = db.get_cursor(conn)
        try:
            cursor.execute(
                f"""
                CREATE TABLE IF NOT EXISTS skill_trade_outcomes (
                    outcome_id TEXT PRIMARY KEY,
                    account_id TEXT,
                    order_id TEXT,
                    trade_id TEXT,
                    symbol TEXT,
                    skill_name TEXT,
                    source_agent TEXT,
                    outcome TEXT,
                    realized_pnl TEXT,
                    return_pct TEXT,
                    payload {json_type} NOT NULL,
                    created_at {timestamp_type} NOT NULL
                )
                """
            )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cursor.close()


def create_skill_trade_outcome(db, payload: Dict[str, Any], correlation_id: Optional[str] = None) -> Dict[str, Any]:
    data = dict(payload or {})
    outcome_id = str(data.get("outcome_id") or data.get("id") or uuid.uuid4())
    created_at = _now(db)

    if correlation_id:
        metadata = data.get("metadata") if isinstance(data.get("metadata"), dict) else {}
        metadata.setdefault("correlation_id", correlation_id)
        data["metadata"] = metadata

    row = {
        "outcome_id": outcome_id,
        "account_id": data.get("account_id"),
        "order_id": data.get("order_id"),
        "trade_id": data.get("trade_id"),
        "symbol": str(data.get("symbol") or "").upper() or None,
        "skill_name": data.get("skill_name") or data.get("skill") or data.get("strategy"),
        "source_agent": data.get("source_agent") or data.get("agent") or "execution-agent",
        "outcome": data.get("outcome") or data.get("status"),
        "realized_pnl": data.get("realized_pnl"),
        "return_pct": data.get("return_pct"),
        "payload": data,
        "created_at": created_at,
    }

    p = _param(db)
    with db.connection_scope() as conn:
        cursor = db.get_cursor(conn)
        try:
            cursor.execute(
                f"""
                INSERT INTO skill_trade_outcomes (
                    outcome_id, account_id, order_id, trade_id, symbol, skill_name,
                    source_agent, outcome, realized_pnl, return_pct, payload, created_at
                )
                VALUES ({p}, {p}, {p}, {p}, {p}, {p}, {p}, {p}, {p}, {p}, {p}, {p})
                """,
                (
                    row["outcome_id"],
                    str(row["account_id"]) if row["account_id"] is not None else None,
                    str(row["order_id"]) if row["order_id"] is not None else None,
                    str(row["trade_id"]) if row["trade_id"] is not None else None,
                    row["symbol"],
                    str(row["skill_name"]) if row["skill_name"] is not None else None,
                    str(row["source_agent"]) if row["source_agent"] is not None else None,
                    str(row["outcome"]) if row["outcome"] is not None else None,
                    str(row["realized_pnl"]) if row["realized_pnl"] is not None else None,
                    str(row["return_pct"]) if row["return_pct"] is not None else None,
                    _json_payload(row["payload"], db),
                    row["created_at"],
                ),
            )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cursor.close()

    return {
        **row,
        "payload": data,
        "created_at": created_at.isoformat() if hasattr(created_at, "isoformat") else created_at,
    }


def get_skill_trade_outcome(db, outcome_id: str) -> Dict[str, Any]:
    p = _param(db)
    with db.connection_scope() as conn:
        cursor = db.get_cursor(conn)
        try:
            cursor.execute(f"SELECT * FROM skill_trade_outcomes WHERE outcome_id = {p}", (outcome_id,))
            row = _row_to_dict(cursor.fetchone())
            if not row:
                return {}
            row["payload"] = _json_load(row.get("payload"), {})
            return row
        finally:
            cursor.close()
