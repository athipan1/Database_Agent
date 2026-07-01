import json
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _row_get(row: Any, key: str, index: int = 0) -> Any:
    try:
        return row[key]
    except Exception:
        return row[index]


def _loads(raw: Any, default):
    if raw is None:
        return default
    if isinstance(raw, (dict, list)):
        return raw
    try:
        return json.loads(raw)
    except Exception:
        return default


def setup_review_history_tables(db) -> None:
    timestamp_type = "TEXT" if db.db_type == "sqlite" else "TIMESTAMPTZ"
    with db.connection_scope() as conn:
        cursor = db.get_cursor(conn)
        try:
            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS review_runs (
                    review_run_id TEXT PRIMARY KEY,
                    account_id TEXT NOT NULL,
                    bucket TEXT NOT NULL,
                    mode TEXT NOT NULL DEFAULT 'review_report',
                    source TEXT NOT NULL DEFAULT 'manager-agent',
                    status TEXT NOT NULL DEFAULT 'created',
                    generated_at {timestamp_type},
                    correlation_id TEXT,
                    summary TEXT DEFAULT '{{}}',
                    safety TEXT DEFAULT '{{}}',
                    raw_report TEXT DEFAULT '{{}}',
                    created_at {timestamp_type} DEFAULT CURRENT_TIMESTAMP,
                    updated_at {timestamp_type} DEFAULT CURRENT_TIMESTAMP
                );
            """)
            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS review_decisions (
                    decision_id TEXT PRIMARY KEY,
                    review_run_id TEXT NOT NULL,
                    account_id TEXT NOT NULL,
                    bucket TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    profit_action TEXT,
                    risk_status TEXT,
                    preview_status TEXT,
                    final_decision TEXT,
                    reason TEXT,
                    position_snapshot TEXT DEFAULT '{{}}',
                    profit_plan TEXT DEFAULT '{{}}',
                    risk_result TEXT DEFAULT '{{}}',
                    preview_result TEXT DEFAULT '{{}}',
                    metadata TEXT DEFAULT '{{}}',
                    created_at {timestamp_type} DEFAULT CURRENT_TIMESTAMP
                );
            """)
            if db.db_type == "postgres":
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_review_runs_account_bucket ON review_runs(account_id, bucket)")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_review_decisions_run ON review_decisions(review_run_id)")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_review_decisions_symbol ON review_decisions(symbol)")
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cursor.close()


def _final_decision(row: Dict[str, Any]) -> str:
    plan = row.get("profit_plan") or {}
    action = str(plan.get("primary_action") or "hold").lower()
    risk_status = str(row.get("risk_status") or "not_submitted").lower()
    preview_status = str(row.get("execution_preview_status") or "not_submitted").lower()
    if risk_status != "approved":
        return "BLOCKED_BY_RISK"
    if preview_status == "blocked":
        return "BLOCKED_BY_PREVIEW"
    if action == "hold":
        return "HOLD"
    if action == "move_stop":
        return "REVIEW_STOP_CHANGE"
    if action == "partial_exit":
        return "REVIEW_PARTIAL_EXIT"
    if action == "exit_all":
        return "MANUAL_REVIEW_REQUIRED"
    return "REVIEW_REQUIRED"


def _reason(row: Dict[str, Any]) -> Optional[str]:
    plan = row.get("profit_plan") or {}
    actions = plan.get("actions") or []
    if actions and isinstance(actions[0], dict):
        return actions[0].get("reason")
    return plan.get("reason")


def _format_run(row: Any) -> Optional[Dict[str, Any]]:
    if not row:
        return None
    return {
        "review_run_id": _row_get(row, "review_run_id", 0),
        "account_id": _row_get(row, "account_id", 1),
        "bucket": _row_get(row, "bucket", 2),
        "mode": _row_get(row, "mode", 3),
        "source": _row_get(row, "source", 4),
        "status": _row_get(row, "status", 5),
        "generated_at": _row_get(row, "generated_at", 6),
        "correlation_id": _row_get(row, "correlation_id", 7),
        "summary": _loads(_row_get(row, "summary", 8), {}),
        "safety": _loads(_row_get(row, "safety", 9), {}),
        "raw_report": _loads(_row_get(row, "raw_report", 10), {}),
        "created_at": _row_get(row, "created_at", 11),
        "updated_at": _row_get(row, "updated_at", 12),
    }


def _format_decision(row: Any) -> Optional[Dict[str, Any]]:
    if not row:
        return None
    return {
        "decision_id": _row_get(row, "decision_id", 0),
        "review_run_id": _row_get(row, "review_run_id", 1),
        "account_id": _row_get(row, "account_id", 2),
        "bucket": _row_get(row, "bucket", 3),
        "symbol": _row_get(row, "symbol", 4),
        "profit_action": _row_get(row, "profit_action", 5),
        "risk_status": _row_get(row, "risk_status", 6),
        "preview_status": _row_get(row, "preview_status", 7),
        "final_decision": _row_get(row, "final_decision", 8),
        "reason": _row_get(row, "reason", 9),
        "position_snapshot": _loads(_row_get(row, "position_snapshot", 10), {}),
        "profit_plan": _loads(_row_get(row, "profit_plan", 11), {}),
        "risk_result": _loads(_row_get(row, "risk_result", 12), {}),
        "preview_result": _loads(_row_get(row, "preview_result", 13), {}),
        "metadata": _loads(_row_get(row, "metadata", 14), {}),
        "created_at": _row_get(row, "created_at", 15),
    }


def create_review_history(db, body: Dict[str, Any], correlation_id: Optional[str] = None) -> Dict[str, Any]:
    setup_review_history_tables(db)
    report = body.get("report") or {}
    account_id = str(body.get("account_id") or report.get("account_id") or "1")
    bucket = body.get("bucket") or report.get("bucket") or "unassigned"
    review_run_id = body.get("review_run_id") or f"review-{bucket}-{uuid.uuid4().hex[:12]}"
    rows = report.get("reviewed_positions") or []
    now = _now_iso()

    with db.connection_scope() as conn:
        cursor = db.get_cursor(conn)
        try:
            cursor.execute(f"""
                INSERT INTO review_runs
                    (review_run_id, account_id, bucket, mode, source, status, generated_at,
                     correlation_id, summary, safety, raw_report, updated_at)
                VALUES ({db.param_style}, {db.param_style}, {db.param_style}, {db.param_style}, {db.param_style},
                        {db.param_style}, {db.param_style}, {db.param_style}, {db.param_style}, {db.param_style},
                        {db.param_style}, {db.param_style})
            """, (
                review_run_id,
                account_id,
                str(bucket),
                str(report.get("mode") or "review_report"),
                str(body.get("source") or "manager-agent"),
                str(body.get("status") or "completed"),
                report.get("generated_at"),
                correlation_id,
                json.dumps(report.get("summary") or {}),
                json.dumps(report.get("safety") or {}),
                json.dumps(report),
                now,
            ))
            for row in rows:
                plan = row.get("profit_plan") or {}
                cursor.execute(f"""
                    INSERT INTO review_decisions
                        (decision_id, review_run_id, account_id, bucket, symbol, profit_action,
                         risk_status, preview_status, final_decision, reason, position_snapshot,
                         profit_plan, risk_result, preview_result, metadata)
                    VALUES ({db.param_style}, {db.param_style}, {db.param_style}, {db.param_style}, {db.param_style},
                            {db.param_style}, {db.param_style}, {db.param_style}, {db.param_style}, {db.param_style},
                            {db.param_style}, {db.param_style}, {db.param_style}, {db.param_style}, {db.param_style})
                """, (
                    f"decision-{uuid.uuid4().hex}",
                    review_run_id,
                    account_id,
                    str(row.get("bucket") or bucket),
                    str(row.get("symbol") or "").upper(),
                    plan.get("primary_action"),
                    row.get("risk_status"),
                    row.get("execution_preview_status"),
                    _final_decision(row),
                    _reason(row),
                    json.dumps({
                        "quantity": row.get("quantity"),
                        "entry_price": row.get("entry_price"),
                        "current_price": row.get("current_price"),
                        "stop_loss": row.get("stop_loss"),
                        "has_protective_stop": row.get("has_protective_stop"),
                    }),
                    json.dumps(plan),
                    json.dumps(row.get("risk_result") or {}),
                    json.dumps(row.get("execution_preview_result") or {}),
                    json.dumps({"bucket_source": row.get("bucket_source"), "profit_source": row.get("profit_source")}),
                ))
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cursor.close()
    return get_review_history(db, review_run_id) or {"review_run_id": review_run_id, "decisions": []}


def get_review_history(db, review_run_id: str) -> Optional[Dict[str, Any]]:
    setup_review_history_tables(db)
    with db.connection_scope() as conn:
        cursor = db.get_cursor(conn)
        try:
            cursor.execute(f"SELECT * FROM review_runs WHERE review_run_id = {db.param_style}", (review_run_id,))
            record = _format_run(cursor.fetchone())
            if not record:
                return None
            cursor.execute(f"SELECT * FROM review_decisions WHERE review_run_id = {db.param_style} ORDER BY symbol ASC", (review_run_id,))
            record["decisions"] = [item for item in (_format_decision(row) for row in cursor.fetchall()) if item]
            return record
        finally:
            cursor.close()


def list_review_history(db, account_id: Optional[str] = None, bucket: Optional[str] = None, limit: int = 100) -> List[Dict[str, Any]]:
    setup_review_history_tables(db)
    where = []
    params: List[Any] = []
    if account_id:
        where.append(f"account_id = {db.param_style}")
        params.append(str(account_id))
    if bucket:
        where.append(f"bucket = {db.param_style}")
        params.append(str(bucket))
    where_sql = f" WHERE {' AND '.join(where)}" if where else ""
    params.append(limit)
    with db.connection_scope() as conn:
        cursor = db.get_cursor(conn)
        try:
            cursor.execute(f"SELECT * FROM review_runs{where_sql} ORDER BY created_at DESC LIMIT {db.param_style}", tuple(params))
            return [item for item in (_format_run(row) for row in cursor.fetchall()) if item]
        finally:
            cursor.close()
