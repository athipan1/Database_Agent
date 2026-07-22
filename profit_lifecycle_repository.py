from __future__ import annotations

import json
import re
import threading
from contextlib import nullcontext
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict, List, Optional

try:
    from psycopg2.extras import Json as PgJson
except Exception:  # pragma: no cover
    PgJson = None


TERMINAL_STATUSES = {"EXECUTED", "REJECTED", "FAILED", "EXPIRED"}
ALLOWED_TRANSITIONS = {
    "PROPOSED": {"RISK_APPROVED", "REJECTED", "FAILED", "EXPIRED"},
    "RISK_APPROVED": {"EXECUTION_PENDING", "FAILED", "EXPIRED"},
    "EXECUTION_PENDING": {"EXECUTED", "FAILED", "EXPIRED"},
}
POSITION_ID_RE = re.compile(r"^(?:account-)?(?P<account>[^:]+):position-(?P<id>[0-9]+)$")
_SQLITE_WRITE_LOCK = threading.RLock()


class ProfitLifecycleNotFound(LookupError):
    pass


class ProfitDecisionNotFound(LookupError):
    pass


class StalePositionVersion(RuntimeError):
    pass


class InvalidProfitDecisionTransition(RuntimeError):
    pass


def _now(db):
    value = datetime.now(timezone.utc)
    return value.isoformat() if db.db_type == "sqlite" else value


def _row_dict(row: Any) -> Dict[str, Any]:
    return dict(row) if row else {}


def _json_value(value: Any, db) -> Any:
    if db.db_type == "sqlite":
        return json.dumps(value or {}, ensure_ascii=False, default=str)
    if PgJson is not None:
        return PgJson(value or {}, dumps=lambda item: json.dumps(item, default=str))
    return json.dumps(value or {}, default=str)


def _loads(value: Any) -> Dict[str, Any]:
    if isinstance(value, dict):
        return value
    if not value:
        return {}
    try:
        parsed = json.loads(value)
        return parsed if isinstance(parsed, dict) else {}
    except Exception:
        return {}


def external_position_id(account_id: Any, internal_position_id: Any) -> str:
    return f"account-{account_id}:position-{internal_position_id}"


def _internal_position_id(account_id: Any, position_id: str) -> int:
    raw = str(position_id)
    if raw.isdigit():
        return int(raw)
    match = POSITION_ID_RE.fullmatch(raw)
    if not match or str(match.group("account")) != str(account_id):
        raise ProfitLifecycleNotFound(
            f"position lifecycle {position_id} was not found for account {account_id}"
        )
    return int(match.group("id"))


def setup_profit_lifecycle_tables(db) -> None:
    timestamp_type = "TEXT" if db.db_type == "sqlite" else "TIMESTAMPTZ"
    numeric_type = "TEXT" if db.db_type == "sqlite" else "NUMERIC(24, 8)"
    json_type = "TEXT" if db.db_type == "sqlite" else "JSONB"
    pk_type = (
        "INTEGER PRIMARY KEY AUTOINCREMENT"
        if db.db_type == "sqlite"
        else "BIGSERIAL PRIMARY KEY"
    )
    with db.connection_scope() as conn:
        cursor = db.get_cursor(conn)
        try:
            db._add_column_if_not_exists(
                cursor, "positions", "position_version", "INTEGER NOT NULL DEFAULT 1"
            )
            db._add_column_if_not_exists(
                cursor,
                "positions",
                "first_target_executed",
                "BOOLEAN NOT NULL DEFAULT FALSE",
            )
            db._add_column_if_not_exists(
                cursor,
                "positions",
                "second_target_executed",
                "BOOLEAN NOT NULL DEFAULT FALSE",
            )
            db._add_column_if_not_exists(
                cursor,
                "positions",
                "total_exited_quantity",
                f"{numeric_type} NOT NULL DEFAULT 0",
            )
            db._add_column_if_not_exists(
                cursor, "positions", "last_profit_decision_id", "TEXT"
            )
            db._add_column_if_not_exists(
                cursor, "positions", "last_profit_decision_status", "TEXT"
            )
            db._add_column_if_not_exists(
                cursor, "positions", "last_profit_decision_at", timestamp_type
            )
            cursor.execute(
                f"""
                CREATE TABLE IF NOT EXISTS profit_decisions (
                    record_id {pk_type},
                    account_id TEXT NOT NULL,
                    position_id TEXT NOT NULL,
                    position_version INTEGER NOT NULL,
                    decision_id TEXT NOT NULL,
                    decision_type TEXT NOT NULL,
                    status TEXT NOT NULL,
                    proposed_quantity {numeric_type} NOT NULL,
                    executed_quantity {numeric_type} NOT NULL DEFAULT 0,
                    correlation_id TEXT,
                    next_lifecycle_state {json_type} NOT NULL,
                    metadata {json_type} NOT NULL,
                    error TEXT,
                    created_at {timestamp_type} DEFAULT CURRENT_TIMESTAMP,
                    updated_at {timestamp_type} DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE (account_id, position_id, decision_id)
                )
                """
            )
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_profit_decisions_position "
                "ON profit_decisions(account_id, position_id, created_at)"
            )
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_profit_decisions_status "
                "ON profit_decisions(status, updated_at)"
            )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cursor.close()


def _select_position(cursor, db, account_id: Any, position_id: str, *, lock: bool) -> Dict[str, Any]:
    internal_id = _internal_position_id(account_id, position_id)
    lock_clause = " FOR UPDATE" if lock and db.db_type == "postgres" else ""
    cursor.execute(
        f"SELECT * FROM positions WHERE account_id = {db.param_style} "
        f"AND position_id = {db.param_style}{lock_clause}",
        (int(account_id), internal_id),
    )
    row = _row_dict(cursor.fetchone())
    if not row:
        raise ProfitLifecycleNotFound(
            f"position lifecycle {position_id} was not found for account {account_id}"
        )
    return row


def _lifecycle(row: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "account_id": str(row.get("account_id")),
        "position_id": external_position_id(
            row.get("account_id"), row.get("position_id")
        ),
        "position_version": int(row.get("position_version") or 1),
        "symbol": str(row.get("symbol") or "").upper(),
        "first_target_executed": bool(row.get("first_target_executed")),
        "second_target_executed": bool(row.get("second_target_executed")),
        "total_exited_quantity": Decimal(
            str(row.get("total_exited_quantity") or 0)
        ),
        "remaining_quantity": Decimal(str(row.get("quantity") or 0)),
        "highest_price_since_entry": row.get("highest_price_since_entry"),
        "last_profit_decision_id": row.get("last_profit_decision_id"),
        "last_profit_decision_status": row.get("last_profit_decision_status"),
        "last_profit_decision_at": (
            str(row.get("last_profit_decision_at"))
            if row.get("last_profit_decision_at") is not None
            else None
        ),
    }


def get_profit_lifecycle(db, account_id: Any, position_id: str) -> Dict[str, Any]:
    setup_profit_lifecycle_tables(db)
    with db.connection_scope() as conn:
        cursor = db.get_cursor(conn)
        try:
            return _lifecycle(
                _select_position(cursor, db, account_id, position_id, lock=False)
            )
        finally:
            cursor.close()


def list_profit_lifecycles(db, account_id: Any) -> List[Dict[str, Any]]:
    setup_profit_lifecycle_tables(db)
    with db.connection_scope() as conn:
        cursor = db.get_cursor(conn)
        try:
            cursor.execute(
                f"SELECT * FROM positions WHERE account_id = {db.param_style} "
                "ORDER BY symbol",
                (int(account_id),),
            )
            return [_lifecycle(_row_dict(row)) for row in cursor.fetchall()]
        finally:
            cursor.close()


def _decision(row: Any, *, duplicate: bool = False) -> Dict[str, Any]:
    item = _row_dict(row)
    if not item:
        return {}
    return {
        "account_id": str(item.get("account_id")),
        "position_id": item.get("position_id"),
        "position_version": int(item.get("position_version") or 0),
        "decision_id": item.get("decision_id"),
        "decision_type": item.get("decision_type"),
        "status": item.get("status"),
        "proposed_quantity": Decimal(str(item.get("proposed_quantity") or 0)),
        "executed_quantity": Decimal(str(item.get("executed_quantity") or 0)),
        "correlation_id": item.get("correlation_id"),
        "next_lifecycle_state": _loads(item.get("next_lifecycle_state")),
        "metadata": _loads(item.get("metadata")),
        "error": item.get("error"),
        "duplicate": duplicate,
        "created_at": str(item.get("created_at")) if item.get("created_at") else None,
        "updated_at": str(item.get("updated_at")) if item.get("updated_at") else None,
    }


def _select_decision(cursor, db, account_id: Any, decision_id: str, *, lock: bool = False):
    lock_clause = " FOR UPDATE" if lock and db.db_type == "postgres" else ""
    cursor.execute(
        f"SELECT * FROM profit_decisions WHERE account_id = {db.param_style} "
        f"AND decision_id = {db.param_style}{lock_clause}",
        (str(account_id), decision_id),
    )
    return cursor.fetchone()


def get_profit_decision(db, account_id: Any, decision_id: str) -> Dict[str, Any]:
    setup_profit_lifecycle_tables(db)
    with db.connection_scope() as conn:
        cursor = db.get_cursor(conn)
        try:
            row = _select_decision(cursor, db, account_id, decision_id)
            if not row:
                raise ProfitDecisionNotFound(
                    f"profit decision {decision_id} was not found"
                )
            return _decision(row)
        finally:
            cursor.close()


def reserve_profit_decision(
    db,
    account_id: Any,
    body,
    correlation_id: Optional[str],
) -> Dict[str, Any]:
    lock = _SQLITE_WRITE_LOCK if db.db_type == "sqlite" else nullcontext()
    with lock:
        return _reserve_profit_decision_unlocked(
            db, account_id, body, correlation_id
        )


def _reserve_profit_decision_unlocked(
    db,
    account_id: Any,
    body,
    correlation_id: Optional[str],
) -> Dict[str, Any]:
    setup_profit_lifecycle_tables(db)
    with db.connection_scope() as conn:
        cursor = db.get_cursor(conn)
        try:
            position = _select_position(
                cursor, db, account_id, body.position_id, lock=True
            )
            existing = _select_decision(
                cursor, db, account_id, body.decision_id, lock=True
            )
            if existing:
                return _decision(existing, duplicate=True)
            current_version = int(position.get("position_version") or 1)
            if current_version != body.position_version:
                raise StalePositionVersion(
                    f"stale position version: expected {current_version}, "
                    f"received {body.position_version}"
                )
            external_id = external_position_id(
                position.get("account_id"), position.get("position_id")
            )
            now = _now(db)
            cursor.execute(
                f"""
                INSERT INTO profit_decisions (
                    account_id, position_id, position_version, decision_id,
                    decision_type, status, proposed_quantity, executed_quantity,
                    correlation_id, next_lifecycle_state, metadata, updated_at
                ) VALUES (
                    {db.param_style}, {db.param_style}, {db.param_style},
                    {db.param_style}, {db.param_style}, {db.param_style},
                    {db.param_style}, {db.param_style}, {db.param_style},
                    {db.param_style}, {db.param_style}, {db.param_style}
                )
                """,
                (
                    str(account_id),
                    external_id,
                    body.position_version,
                    body.decision_id,
                    body.decision_type,
                    "PROPOSED",
                    str(body.proposed_quantity),
                    "0",
                    correlation_id,
                    _json_value(body.next_lifecycle_state, db),
                    _json_value(body.metadata, db),
                    now,
                ),
            )
            cursor.execute(
                f"UPDATE positions SET last_profit_decision_id = {db.param_style}, "
                f"last_profit_decision_status = {db.param_style}, "
                f"last_profit_decision_at = {db.param_style} "
                f"WHERE position_id = {db.param_style}",
                (body.decision_id, "PROPOSED", now, position.get("position_id")),
            )
            row = _select_decision(cursor, db, account_id, body.decision_id)
            conn.commit()
            return _decision(row)
        except Exception:
            conn.rollback()
            raise
        finally:
            cursor.close()


def transition_profit_decision(
    db,
    account_id: Any,
    decision_id: str,
    body,
    correlation_id: Optional[str],
) -> Dict[str, Any]:
    lock = _SQLITE_WRITE_LOCK if db.db_type == "sqlite" else nullcontext()
    with lock:
        return _transition_profit_decision_unlocked(
            db, account_id, decision_id, body, correlation_id
        )


def _transition_profit_decision_unlocked(
    db,
    account_id: Any,
    decision_id: str,
    body,
    correlation_id: Optional[str],
) -> Dict[str, Any]:
    setup_profit_lifecycle_tables(db)
    with db.connection_scope() as conn:
        cursor = db.get_cursor(conn)
        try:
            row = _select_decision(cursor, db, account_id, decision_id, lock=False)
            if not row:
                raise ProfitDecisionNotFound(
                    f"profit decision {decision_id} was not found"
                )
            preliminary = _decision(row)
            position = None
            try:
                # Keep lock order consistent with reservation: position, then decision.
                position = _select_position(
                    cursor,
                    db,
                    account_id,
                    preliminary["position_id"],
                    lock=True,
                )
            except ProfitLifecycleNotFound:
                is_full_exit = preliminary["decision_type"] in {
                    "hard_stop_exit",
                    "trailing_stop_exit",
                }
                if body.status != "EXECUTED" or not is_full_exit:
                    raise
            row = _select_decision(cursor, db, account_id, decision_id, lock=True)
            current = _decision(row)
            if current["status"] == body.status:
                if (
                    body.status == "EXECUTION_PENDING"
                    and body.executed_quantity > current["executed_quantity"]
                ):
                    if body.executed_quantity > current["proposed_quantity"]:
                        raise InvalidProfitDecisionTransition(
                            "executed_quantity exceeds proposed_quantity"
                        )
                    now = _now(db)
                    metadata = {**current["metadata"], **body.metadata}
                    metadata["partial_fill_recorded"] = True
                    if correlation_id:
                        metadata["last_transition_correlation_id"] = correlation_id
                    cursor.execute(
                        f"""
                        UPDATE profit_decisions
                        SET executed_quantity = {db.param_style},
                            metadata = {db.param_style}, updated_at = {db.param_style}
                        WHERE account_id = {db.param_style}
                          AND decision_id = {db.param_style}
                        """,
                        (
                            str(body.executed_quantity),
                            _json_value(metadata, db),
                            now,
                            str(account_id),
                            decision_id,
                        ),
                    )
                    updated = _select_decision(
                        cursor, db, account_id, decision_id
                    )
                    conn.commit()
                    return _decision(updated)
                current["duplicate"] = True
                return current
            if current["status"] != body.expected_status:
                raise InvalidProfitDecisionTransition(
                    f"decision status is {current['status']}, expected {body.expected_status}"
                )
            if body.status not in ALLOWED_TRANSITIONS.get(current["status"], set()):
                raise InvalidProfitDecisionTransition(
                    f"invalid transition {current['status']} -> {body.status}"
                )
            now = _now(db)
            metadata = {**current["metadata"], **body.metadata}
            if correlation_id:
                metadata["last_transition_correlation_id"] = correlation_id
            if body.status == "EXECUTED":
                if body.executed_quantity <= 0:
                    raise InvalidProfitDecisionTransition(
                        "EXECUTED requires executed_quantity greater than zero"
                    )
                if body.executed_quantity > current["proposed_quantity"]:
                    raise InvalidProfitDecisionTransition(
                        "executed_quantity exceeds proposed_quantity"
                    )
                if position is not None:
                    current_version = int(position.get("position_version") or 1)
                    if current_version != current["position_version"]:
                        raise StalePositionVersion(
                            f"stale position version: expected {current_version}, "
                            f"decision used {current['position_version']}"
                        )
                    next_state = current["next_lifecycle_state"]
                    first_executed = bool(position.get("first_target_executed"))
                    second_executed = bool(position.get("second_target_executed"))
                    if next_state.get("first_target_executed"):
                        first_executed = True
                    if next_state.get("second_target_executed"):
                        if not first_executed:
                            raise InvalidProfitDecisionTransition(
                                "second target cannot execute before first target"
                            )
                        second_executed = True
                    new_total = Decimal(
                        str(position.get("total_exited_quantity") or 0)
                    ) + body.executed_quantity
                    cursor.execute(
                        f"""
                        UPDATE positions
                        SET position_version = position_version + 1,
                            first_target_executed = {db.param_style},
                            second_target_executed = {db.param_style},
                            total_exited_quantity = {db.param_style},
                            last_profit_decision_id = {db.param_style},
                            last_profit_decision_status = {db.param_style},
                            last_profit_decision_at = {db.param_style}
                        WHERE position_id = {db.param_style}
                          AND position_version = {db.param_style}
                        """,
                        (
                            first_executed,
                            second_executed,
                            str(new_total),
                            decision_id,
                            body.status,
                            now,
                            position.get("position_id"),
                            current["position_version"],
                        ),
                    )
                    if cursor.rowcount != 1:
                        raise StalePositionVersion(
                            "position version changed concurrently"
                        )
                else:
                    metadata["position_closed_before_executed_transition"] = True
            else:
                if position is None:  # defensive; non-executed transitions require an open row
                    raise ProfitLifecycleNotFound(
                        f"position lifecycle {current['position_id']} was not found"
                    )
                cursor.execute(
                    f"UPDATE positions SET last_profit_decision_id = {db.param_style}, "
                    f"last_profit_decision_status = {db.param_style}, "
                    f"last_profit_decision_at = {db.param_style} "
                    f"WHERE position_id = {db.param_style}",
                    (decision_id, body.status, now, position.get("position_id")),
                )
            cursor.execute(
                f"""
                UPDATE profit_decisions
                SET status = {db.param_style}, executed_quantity = {db.param_style},
                    metadata = {db.param_style}, error = {db.param_style},
                    updated_at = {db.param_style}
                WHERE account_id = {db.param_style} AND decision_id = {db.param_style}
                """,
                (
                    body.status,
                    str(body.executed_quantity),
                    _json_value(metadata, db),
                    body.error,
                    now,
                    str(account_id),
                    decision_id,
                ),
            )
            updated = _select_decision(cursor, db, account_id, decision_id)
            conn.commit()
            return _decision(updated)
        except Exception:
            conn.rollback()
            raise
        finally:
            cursor.close()
