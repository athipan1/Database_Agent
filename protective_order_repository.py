import json
from typing import Any, Dict, Optional


PROTECTIVE_ORDER_COLUMNS = {
    "risk_approval_id": "TEXT",
    "final_quantity": "BIGINT",
    "guard_plan": "TEXT",
    "protective_exit": "TEXT",
    "metadata": "TEXT",
    "strategy_bucket": "TEXT DEFAULT 'unassigned'",
}


def _json_or_none(value: Optional[Dict[str, Any]]) -> Optional[str]:
    if value is None:
        return None
    return json.dumps(value)


def _parse_json(value: Any) -> Optional[Dict[str, Any]]:
    if value is None or isinstance(value, dict):
        return value
    try:
        parsed = json.loads(value)
    except Exception:
        return None
    return parsed if isinstance(parsed, dict) else None


def _strategy_bucket_from_payload(*payloads: Optional[Dict[str, Any]]) -> Optional[str]:
    for payload in payloads:
        if not isinstance(payload, dict):
            continue
        bucket = payload.get("strategy_bucket") or payload.get("bucket")
        if bucket:
            return str(bucket)
    return None


def setup_protective_order_columns(db) -> None:
    """Ensure orders can persist Risk/Manager protective execution metadata."""
    with db.connection_scope() as conn:
        cursor = db.get_cursor(conn)
        try:
            for column, definition in PROTECTIVE_ORDER_COLUMNS.items():
                db._add_column_if_not_exists(cursor, "orders", column, definition)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cursor.close()


def persist_protective_order_metadata(
    db,
    order_id: int,
    *,
    risk_approval_id: Optional[str] = None,
    final_quantity: Optional[int] = None,
    guard_plan: Optional[Dict[str, Any]] = None,
    protective_exit: Optional[Dict[str, Any]] = None,
    metadata: Optional[Dict[str, Any]] = None,
) -> None:
    strategy_bucket = _strategy_bucket_from_payload(metadata, guard_plan, protective_exit)
    updates = {
        "risk_approval_id": risk_approval_id,
        "final_quantity": final_quantity,
        "guard_plan": _json_or_none(guard_plan),
        "protective_exit": _json_or_none(protective_exit),
        "metadata": _json_or_none(metadata),
        "strategy_bucket": strategy_bucket,
    }
    updates = {key: value for key, value in updates.items() if value is not None}
    if not updates:
        return

    with db.connection_scope() as conn:
        cursor = db.get_cursor(conn)
        try:
            set_clause = ", ".join(f"{key} = {db.param_style}" for key in updates)
            params = list(updates.values()) + [order_id]
            cursor.execute(f"UPDATE orders SET {set_clause} WHERE order_id = {db.param_style}", tuple(params))
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cursor.close()


def normalize_order_protective_metadata(order: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not order:
        return order
    normalized = dict(order)
    normalized["guard_plan"] = _parse_json(normalized.get("guard_plan"))
    normalized["protective_exit"] = _parse_json(normalized.get("protective_exit"))
    normalized["metadata"] = _parse_json(normalized.get("metadata")) or {}
    normalized["strategy_bucket"] = (
        normalized.get("strategy_bucket")
        or _strategy_bucket_from_payload(normalized["metadata"], normalized["guard_plan"], normalized["protective_exit"])
        or "unassigned"
    )
    return normalized
