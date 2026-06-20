import json
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Union

from fastapi import HTTPException

from models import CreateRiskApprovalBody, RiskApproval


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _parse_dt(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    text = str(value).replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _row_get(row: Any, key: str, index: int = 0) -> Any:
    try:
        return row[key]
    except Exception:
        return row[index]


def _format_row(row: Any) -> Optional[RiskApproval]:
    if not row:
        return None
    metadata_raw = _row_get(row, "metadata", 9)
    try:
        metadata = json.loads(metadata_raw) if metadata_raw else {}
    except Exception:
        metadata = {}
    return RiskApproval(
        approval_id=_row_get(row, "approval_id", 0),
        account_id=_row_get(row, "account_id", 1),
        symbol=_row_get(row, "symbol", 2),
        side=str(_row_get(row, "side", 3)).lower(),
        approved_quantity=int(_row_get(row, "approved_quantity", 4)),
        status=str(_row_get(row, "status", 5)).lower(),
        expires_at=_parse_dt(_row_get(row, "expires_at", 6)),
        created_at=_parse_dt(_row_get(row, "created_at", 7)),
        used_at=_parse_dt(_row_get(row, "used_at", 8)),
        order_id=_row_get(row, "order_id", 10),
        metadata=metadata,
    )


def setup_risk_approval_table(db) -> None:
    timestamp_type = "TEXT" if db.db_type == "sqlite" else "TIMESTAMPTZ"
    with db.connection_scope() as conn:
        cursor = db.get_cursor(conn)
        try:
            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS risk_approvals (
                    approval_id TEXT PRIMARY KEY,
                    account_id TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    side TEXT NOT NULL,
                    approved_quantity BIGINT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'approved',
                    expires_at {timestamp_type} NOT NULL,
                    created_at {timestamp_type} DEFAULT CURRENT_TIMESTAMP,
                    used_at {timestamp_type},
                    metadata TEXT DEFAULT '{{}}',
                    order_id INTEGER REFERENCES orders(order_id)
                );
            """)
            if db.db_type == "postgres":
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_risk_approvals_status_expires ON risk_approvals(status, expires_at)")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_risk_approvals_order_id ON risk_approvals(order_id)")
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cursor.close()


def create_risk_approval(db, body: CreateRiskApprovalBody) -> RiskApproval:
    setup_risk_approval_table(db)
    with db.connection_scope() as conn:
        cursor = db.get_cursor(conn)
        try:
            params = (
                body.approval_id,
                str(body.account_id),
                body.symbol.upper(),
                body.side.value.lower(),
                int(body.approved_quantity),
                "approved",
                body.expires_at.isoformat(),
                json.dumps(body.metadata or {}),
            )
            cursor.execute(f"""
                INSERT INTO risk_approvals
                    (approval_id, account_id, symbol, side, approved_quantity, status, expires_at, metadata)
                VALUES ({db.param_style}, {db.param_style}, {db.param_style}, {db.param_style}, {db.param_style}, {db.param_style}, {db.param_style}, {db.param_style})
            """, params)
            conn.commit()
            approval = get_risk_approval(db, body.approval_id)
            if not approval:
                raise RuntimeError("Risk approval was inserted but could not be read back")
            return approval
        except Exception:
            conn.rollback()
            raise
        finally:
            cursor.close()


def get_risk_approval(db, approval_id: str) -> Optional[RiskApproval]:
    setup_risk_approval_table(db)
    with db.connection_scope() as conn:
        cursor = db.get_cursor(conn)
        try:
            cursor.execute(f"SELECT * FROM risk_approvals WHERE approval_id = {db.param_style}", (approval_id,))
            return _format_row(cursor.fetchone())
        finally:
            cursor.close()


def mark_risk_approval_used(db, approval_id: str, order_id: Union[int, str]) -> RiskApproval:
    setup_risk_approval_table(db)
    now = datetime.now(timezone.utc)
    with db.connection_scope() as conn:
        cursor = db.get_cursor(conn)
        try:
            lock_clause = " FOR UPDATE" if db.db_type == "postgres" else ""
            cursor.execute(f"SELECT * FROM risk_approvals WHERE approval_id = {db.param_style}{lock_clause}", (approval_id,))
            approval = _format_row(cursor.fetchone())
            if not approval:
                raise HTTPException(status_code=404, detail=f"Risk approval {approval_id} not found")
            expires_at = approval.expires_at if approval.expires_at.tzinfo else approval.expires_at.replace(tzinfo=timezone.utc)
            if approval.status != "approved":
                raise HTTPException(status_code=409, detail=f"Risk approval {approval_id} is already {approval.status}")
            if expires_at <= now:
                cursor.execute(
                    f"UPDATE risk_approvals SET status = 'expired' WHERE approval_id = {db.param_style}",
                    (approval_id,),
                )
                conn.commit()
                raise HTTPException(status_code=409, detail=f"Risk approval {approval_id} has expired")
            cursor.execute(f"""
                UPDATE risk_approvals
                SET status = 'used', used_at = {db.param_style}, order_id = {db.param_style}
                WHERE approval_id = {db.param_style}
            """, (_now_iso(), int(order_id), approval_id))
            conn.commit()
            used = get_risk_approval(db, approval_id)
            if not used:
                raise RuntimeError("Risk approval was used but could not be read back")
            return used
        except HTTPException:
            conn.rollback()
            raise
        except Exception:
            conn.rollback()
            raise
        finally:
            cursor.close()
