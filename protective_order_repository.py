import json
from typing import Any, Dict, Optional


ALLOWED_STRATEGY_BUCKETS = {
    "core_dividend",
    "value_rebound",
    "news_momentum",
    "unassigned",
}

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


def _normalize_strategy_bucket(value: Any) -> Optional[str]:
    if value is None:
        return None
    bucket = str(value).strip().lower()
    if not bucket:
        return None
    if bucket not in ALLOWED_STRATEGY_BUCKETS:
        raise ValueError(f"unsupported strategy_bucket: {value!r}")
    return bucket


def _strategy_bucket_from_payload(*payloads: Optional[Dict[str, Any]]) -> Optional[str]:
    resolved: Optional[str] = None
    saw_unassigned = False

    for payload in payloads:
        if not isinstance(payload, dict):
            continue
        raw_bucket = payload.get("strategy_bucket") or payload.get("bucket")
        bucket = _normalize_strategy_bucket(raw_bucket)
        if bucket is None:
            continue
        if bucket == "unassigned":
            saw_unassigned = True
            continue
        if resolved is not None and bucket != resolved:
            raise ValueError(
                "strategy_bucket_conflict: "
                f"received both {resolved!r} and {bucket!r}"
            )
        resolved = bucket

    if resolved is not None:
        return resolved
    return "unassigned" if saw_unassigned else None


def _resolve_strategy_bucket(
    explicit_bucket: Any = None,
    *payloads: Optional[Dict[str, Any]],
) -> Optional[str]:
    explicit = _normalize_strategy_bucket(explicit_bucket)
    payload_bucket = _strategy_bucket_from_payload(*payloads)

    explicit_specific = explicit not in (None, "unassigned")
    payload_specific = payload_bucket not in (None, "unassigned")

    if explicit_specific and payload_specific and explicit != payload_bucket:
        raise ValueError(
            "strategy_bucket_conflict: "
            f"explicit={explicit!r}, payload={payload_bucket!r}"
        )
    if explicit_specific:
        return explicit
    if payload_specific:
        return payload_bucket
    return explicit or payload_bucket


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
    strategy_bucket: Optional[str] = None,
) -> None:
    resolved_bucket = _resolve_strategy_bucket(
        strategy_bucket,
        metadata,
        guard_plan,
        protective_exit,
    )
    updates = {
        "risk_approval_id": risk_approval_id,
        "final_quantity": final_quantity,
        "guard_plan": _json_or_none(guard_plan),
        "protective_exit": _json_or_none(protective_exit),
        "metadata": _json_or_none(metadata),
        "strategy_bucket": resolved_bucket,
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
        _resolve_strategy_bucket(
            normalized.get("strategy_bucket"),
            normalized["metadata"],
            normalized["guard_plan"],
            normalized["protective_exit"],
        )
        or "unassigned"
    )
    return normalized
