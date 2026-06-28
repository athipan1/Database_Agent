import json
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Union

from fastapi import HTTPException

from models import CreateRiskApprovalBody, RiskApproval


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _now_iso() -> str:
    return _now().isoformat()


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


def _metadata_with_event(metadata: Dict[str, Any] | None, event: str, extra: Dict[str, Any] | None = None) -> Dict[str, Any]:
    next_metadata = dict(metadata or {})
    events = list(next_metadata.get("lifecycle_events") or [])
    event_row = {"event": event, "timestamp": _now_iso()}
    if extra:
        event_row.update(extra)
    events.append(event_row)
    next_metadata["lifecycle_events"] = events
    next_metadata["last_lifecycle_event"] = event
    next_metadata["last_lifecycle_event_at"] = event_row["timestamp"]
    return next_metadata


def _dump_metadata(metadata: Dict[str, Any] | None) -> str:
    return json.dumps(metadata or {}, sort_keys=True)


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


def _approval_expires_at(approval: RiskApproval) -> datetime:
    return approval.expires_at if approval.expires_at.tzinfo else approval.expires_at.replace(tzinfo=timezone.utc)


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
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_risk_approvals_account_symbol ON risk_approvals(account_id, symbol)")
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cursor.close()


def create_risk_approval(db, body: CreateRiskApprovalBody) -> RiskApproval:
    setup_risk_approval_table(db)
    expires_at = body.expires_at if body.expires_at.tzinfo else body.expires_at.replace(tzinfo=timezone.utc)
    if expires_at <= _now():
        raise HTTPException(status_code=422, detail="Risk approval expires_at must be in the future")
    with db.connection_scope() as conn:
        cursor = db.get_cursor(conn)
        try:
            metadata = _metadata_with_event(body.metadata or {}, "created", {"source": "database_agent"})
            params = (
                body.approval_id,
                str(body.account_id),
                body.symbol.upper(),
                body.side.value.lower(),
                int(body.approved_quantity),
                "approved",
                expires_at.isoformat(),
                _dump_metadata(metadata),
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
        except HTTPException:
            conn.rollback()
            raise
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
            approval = _format_row(cursor.fetchone())
            if approval and approval.status == "approved" and _approval_expires_at(approval) <= _now():
                return expire_risk_approval(db, approval_id, reason="read_expired")
            return approval
        finally:
            cursor.close()


def mark_risk_approval_used(db, approval_id: str, order_id: Union[int, str]) -> RiskApproval:
    setup_risk_approval_table(db)
    now = _now()
    with db.connection_scope() as conn:
        cursor = db.get_cursor(conn)
        try:
            lock_clause = " FOR UPDATE" if db.db_type == "postgres" else ""
            cursor.execute(f"SELECT * FROM risk_approvals WHERE approval_id = {db.param_style}{lock_clause}", (approval_id,))
            approval = _format_row(cursor.fetchone())
            if not approval:
                raise HTTPException(status_code=404, detail=f"Risk approval {approval_id} not found")
            expires_at = _approval_expires_at(approval)
            if approval.status != "approved":
                raise HTTPException(status_code=409, detail=f"Risk approval {approval_id} is already {approval.status}")
            if expires_at <= now:
                metadata = _metadata_with_event(approval.metadata, "expired", {"reason": "use_after_expiry"})
                cursor.execute(
                    f"UPDATE risk_approvals SET status = 'expired', metadata = {db.param_style} WHERE approval_id = {db.param_style}",
                    (_dump_metadata(metadata), approval_id),
                )
                conn.commit()
                raise HTTPException(status_code=409, detail=f"Risk approval {approval_id} has expired")
            metadata = _metadata_with_event(approval.metadata, "used", {"order_id": int(order_id)})
            cursor.execute(f"""
                UPDATE risk_approvals
                SET status = 'used', used_at = {db.param_style}, order_id = {db.param_style}, metadata = {db.param_style}
                WHERE approval_id = {db.param_style}
            """, (_now_iso(), int(order_id), _dump_metadata(metadata), approval_id))
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


def revoke_risk_approval(db, approval_id: str, reason: str = "manual_revoke") -> RiskApproval:
    setup_risk_approval_table(db)
    with db.connection_scope() as conn:
        cursor = db.get_cursor(conn)
        try:
            lock_clause = " FOR UPDATE" if db.db_type == "postgres" else ""
            cursor.execute(f"SELECT * FROM risk_approvals WHERE approval_id = {db.param_style}{lock_clause}", (approval_id,))
            approval = _format_row(cursor.fetchone())
            if not approval:
                raise HTTPException(status_code=404, detail=f"Risk approval {approval_id} not found")
            if approval.status != "approved":
                raise HTTPException(status_code=409, detail=f"Risk approval {approval_id} is already {approval.status}")
            metadata = _metadata_with_event(approval.metadata, "revoked", {"reason": reason})
            cursor.execute(
                f"UPDATE risk_approvals SET status = 'revoked', metadata = {db.param_style} WHERE approval_id = {db.param_style}",
                (_dump_metadata(metadata), approval_id),
            )
            conn.commit()
            revoked = get_risk_approval(db, approval_id)
            if not revoked:
                raise RuntimeError("Risk approval was revoked but could not be read back")
            return revoked
        except HTTPException:
            conn.rollback()
            raise
        except Exception:
            conn.rollback()
            raise
        finally:
            cursor.close()


def expire_risk_approval(db, approval_id: str, reason: str = "manual_expire") -> RiskApproval:
    setup_risk_approval_table(db)
    with db.connection_scope() as conn:
        cursor = db.get_cursor(conn)
        try:
            cursor.execute(f"SELECT * FROM risk_approvals WHERE approval_id = {db.param_style}", (approval_id,))
            approval = _format_row(cursor.fetchone())
            if not approval:
                raise HTTPException(status_code=404, detail=f"Risk approval {approval_id} not found")
            if approval.status != "approved":
                return approval
            metadata = _metadata_with_event(approval.metadata, "expired", {"reason": reason})
            cursor.execute(
                f"UPDATE risk_approvals SET status = 'expired', metadata = {db.param_style} WHERE approval_id = {db.param_style}",
                (_dump_metadata(metadata), approval_id),
            )
            conn.commit()
            expired = get_risk_approval(db, approval_id)
            if not expired:
                raise RuntimeError("Risk approval was expired but could not be read back")
            return expired
        except HTTPException:
            conn.rollback()
            raise
        except Exception:
            conn.rollback()
            raise
        finally:
            cursor.close()
