from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Union


def _json_dumps(value: Any) -> str:
    return json.dumps(value or {}, default=str, sort_keys=True)


def _json_loads(value: Any, default: Any = None) -> Any:
    if value is None:
        return {} if default is None else default
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(value)
    except Exception:
        return {} if default is None else default


def _row_to_dict(row: Any) -> Dict[str, Any]:
    data = dict(row)
    for key in ["allocation_plan", "portfolio_snapshot", "selected_positions", "risk_approvals", "execution_orders", "metadata"]:
        if key in data:
            data[key] = _json_loads(data.get(key), [] if key in {"selected_positions", "risk_approvals", "execution_orders"} else {})
    return data


def setup_portfolio_audit_table(db) -> None:
    timestamp_type = "TEXT" if db.db_type == "sqlite" else "TIMESTAMPTZ"
    pk_type = "TEXT PRIMARY KEY" if db.db_type == "sqlite" else "TEXT PRIMARY KEY"
    with db.connection_scope() as conn:
        cursor = db.get_cursor(conn)
        try:
            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS portfolio_audits (
                    portfolio_audit_id {pk_type},
                    account_id TEXT NOT NULL,
                    correlation_id TEXT,
                    policy_name TEXT,
                    mode TEXT DEFAULT 'portfolio_allocation',
                    status TEXT DEFAULT 'created',
                    allocation_plan TEXT NOT NULL,
                    portfolio_snapshot TEXT NOT NULL,
                    selected_positions TEXT NOT NULL,
                    risk_approvals TEXT NOT NULL,
                    execution_orders TEXT NOT NULL,
                    metadata TEXT NOT NULL,
                    created_at {timestamp_type} DEFAULT CURRENT_TIMESTAMP
                );
            """)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cursor.close()


def create_portfolio_audit(
    db,
    *,
    account_id: Union[int, str],
    correlation_id: Optional[str] = None,
    allocation_plan: Optional[Dict[str, Any]] = None,
    portfolio_snapshot: Optional[Dict[str, Any]] = None,
    selected_positions: Optional[List[Dict[str, Any]]] = None,
    risk_approvals: Optional[List[Dict[str, Any]]] = None,
    execution_orders: Optional[List[Dict[str, Any]]] = None,
    metadata: Optional[Dict[str, Any]] = None,
    status: str = "created",
    portfolio_audit_id: Optional[str] = None,
) -> Dict[str, Any]:
    setup_portfolio_audit_table(db)
    audit_id = portfolio_audit_id or str(uuid.uuid4())
    plan = allocation_plan or {}
    policy_name = str(plan.get("policy_name") or plan.get("name") or "unknown")
    created_at = datetime.now(timezone.utc).isoformat()
    with db.connection_scope() as conn:
        cursor = db.get_cursor(conn)
        try:
            cursor.execute(
                f"""
                INSERT INTO portfolio_audits (
                    portfolio_audit_id, account_id, correlation_id, policy_name, mode, status,
                    allocation_plan, portfolio_snapshot, selected_positions, risk_approvals,
                    execution_orders, metadata, created_at
                ) VALUES (
                    {db.param_style}, {db.param_style}, {db.param_style}, {db.param_style}, {db.param_style}, {db.param_style},
                    {db.param_style}, {db.param_style}, {db.param_style}, {db.param_style},
                    {db.param_style}, {db.param_style}, {db.param_style}
                )
                """,
                (
                    audit_id,
                    str(account_id),
                    correlation_id,
                    policy_name,
                    "portfolio_allocation",
                    status,
                    _json_dumps(plan),
                    _json_dumps(portfolio_snapshot),
                    _json_dumps(selected_positions or []),
                    _json_dumps(risk_approvals or []),
                    _json_dumps(execution_orders or []),
                    _json_dumps(metadata),
                    created_at,
                ),
            )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cursor.close()
    return get_portfolio_audit(db, audit_id) or {}


def get_portfolio_audit(db, portfolio_audit_id: str) -> Optional[Dict[str, Any]]:
    setup_portfolio_audit_table(db)
    with db.connection_scope() as conn:
        cursor = db.get_cursor(conn)
        try:
            cursor.execute(f"SELECT * FROM portfolio_audits WHERE portfolio_audit_id = {db.param_style}", (portfolio_audit_id,))
            row = cursor.fetchone()
            return _row_to_dict(row) if row else None
        finally:
            cursor.close()


def list_portfolio_audits(db, account_id: Union[int, str], limit: int = 50) -> List[Dict[str, Any]]:
    setup_portfolio_audit_table(db)
    safe_limit = max(1, min(int(limit or 50), 500))
    with db.connection_scope() as conn:
        cursor = db.get_cursor(conn)
        try:
            if db.db_type == "sqlite":
                cursor.execute(
                    "SELECT * FROM portfolio_audits WHERE account_id = ? ORDER BY created_at DESC LIMIT ?",
                    (str(account_id), safe_limit),
                )
            else:
                cursor.execute(
                    "SELECT * FROM portfolio_audits WHERE account_id = %s ORDER BY created_at DESC LIMIT %s",
                    (str(account_id), safe_limit),
                )
            return [_row_to_dict(row) for row in cursor.fetchall()]
        finally:
            cursor.close()
