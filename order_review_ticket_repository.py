import json
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from order_review_ticket_models import (
    CreateOrderReviewTicketBody,
    ListOrderReviewTicketsQuery,
    OrderReviewTicketRecord,
)


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


def _loads(raw: Any, default):
    if raw is None:
        return default
    if isinstance(raw, (dict, list)):
        return raw
    try:
        return json.loads(raw)
    except Exception:
        return default


def _row_get(row: Any, key: str, index: int = 0) -> Any:
    try:
        return row[key]
    except Exception:
        return row[index]


def _bool_from_db(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        return value == 1
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def _db_bool(value: bool, db_type: str) -> Any:
    return bool(value) if db_type == "postgres" else int(bool(value))


def _data_from_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    if isinstance(payload, dict) and isinstance(payload.get("data"), dict):
        return payload["data"]
    return payload if isinstance(payload, dict) else {}


def _summary_from_ticket(ticket: Dict[str, Any]) -> Dict[str, Any]:
    summary = ticket.get("summary")
    return summary if isinstance(summary, dict) else {}


def _first_present(*values: Any, default: Any = None) -> Any:
    for value in values:
        if value is not None:
            return value
    return default


def _int_value(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _symbols_from_ticket(ticket: Dict[str, Any]) -> List[str]:
    symbols = ticket.get("requested_symbols")
    if not symbols:
        symbols = ticket.get("symbols")
    if not symbols:
        items = ticket.get("approval_items") or ticket.get("items") or ticket.get("ticket_items") or []
        symbols = [item.get("symbol") for item in items if isinstance(item, dict) and item.get("symbol")]
    if isinstance(symbols, str):
        symbols = [symbols]
    if not isinstance(symbols, list):
        return []
    return [str(symbol).upper() for symbol in symbols if symbol]


def _status_from_values(body: CreateOrderReviewTicketBody, ready_count: int, blocked_count: int) -> str:
    if body.status:
        return str(body.status)
    if blocked_count > 0:
        return "blocked"
    if ready_count > 0:
        return "ready_for_manual_approval"
    return "created"


def _format_row(row: Any) -> Optional[OrderReviewTicketRecord]:
    if not row:
        return None
    return OrderReviewTicketRecord(
        ticket_id=_row_get(row, "ticket_id", 0),
        account_id=_row_get(row, "account_id", 1),
        correlation_id=_row_get(row, "correlation_id", 2),
        source=_row_get(row, "source", 3) or "manager-agent",
        mode=_row_get(row, "mode", 4) or "manual_approval_ticket",
        safety=_row_get(row, "safety", 5) or "read_only_no_orders_submitted_no_orders_cancelled",
        status=_row_get(row, "status", 6) or "created",
        approval_required=_bool_from_db(_row_get(row, "approval_required", 7)),
        execution_enabled=_bool_from_db(_row_get(row, "execution_enabled", 8)),
        manual_confirmation_phrase=_row_get(row, "manual_confirmation_phrase", 9),
        requested_symbols=_loads(_row_get(row, "requested_symbols", 10), []),
        ready_count=_int_value(_row_get(row, "ready_count", 11), 0),
        blocked_count=_int_value(_row_get(row, "blocked_count", 12), 0),
        orders_submitted=_bool_from_db(_row_get(row, "orders_submitted", 13)),
        orders_cancelled=_bool_from_db(_row_get(row, "orders_cancelled", 14)),
        ticket_payload=_loads(_row_get(row, "ticket_payload", 15), {}),
        metadata=_loads(_row_get(row, "metadata", 16), {}),
        created_at=_parse_dt(_row_get(row, "created_at", 17)),
        updated_at=_parse_dt(_row_get(row, "updated_at", 18)),
    )


def setup_order_review_ticket_table(db) -> None:
    timestamp_type = "TEXT" if db.db_type == "sqlite" else "TIMESTAMPTZ"
    bool_type = "INTEGER" if db.db_type == "sqlite" else "BOOLEAN"
    bool_true = "1" if db.db_type == "sqlite" else "TRUE"
    bool_false = "0" if db.db_type == "sqlite" else "FALSE"
    with db.connection_scope() as conn:
        cursor = db.get_cursor(conn)
        try:
            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS order_review_ticket_audits (
                    ticket_id TEXT PRIMARY KEY,
                    account_id TEXT NOT NULL,
                    correlation_id TEXT,
                    source TEXT NOT NULL DEFAULT 'manager-agent',
                    mode TEXT NOT NULL DEFAULT 'manual_approval_ticket',
                    safety TEXT NOT NULL DEFAULT 'read_only_no_orders_submitted_no_orders_cancelled',
                    status TEXT NOT NULL DEFAULT 'created',
                    approval_required {bool_type} NOT NULL DEFAULT {bool_true},
                    execution_enabled {bool_type} NOT NULL DEFAULT {bool_false},
                    manual_confirmation_phrase TEXT,
                    requested_symbols TEXT DEFAULT '[]',
                    ready_count INTEGER NOT NULL DEFAULT 0,
                    blocked_count INTEGER NOT NULL DEFAULT 0,
                    orders_submitted {bool_type} NOT NULL DEFAULT {bool_false},
                    orders_cancelled {bool_type} NOT NULL DEFAULT {bool_false},
                    ticket_payload TEXT DEFAULT '{{}}',
                    metadata TEXT DEFAULT '{{}}',
                    created_at {timestamp_type} DEFAULT CURRENT_TIMESTAMP,
                    updated_at {timestamp_type} DEFAULT CURRENT_TIMESTAMP
                );
            """)
            if db.db_type == "postgres":
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_order_review_ticket_account_status ON order_review_ticket_audits(account_id, status)")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_order_review_ticket_source ON order_review_ticket_audits(source)")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_order_review_ticket_created_at ON order_review_ticket_audits(created_at)")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_order_review_ticket_execution_enabled ON order_review_ticket_audits(execution_enabled)")
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cursor.close()


def create_order_review_ticket_audit(db, body: CreateOrderReviewTicketBody) -> OrderReviewTicketRecord:
    setup_order_review_ticket_table(db)
    ticket = _data_from_payload(body.ticket_payload or {})
    summary = _summary_from_ticket(ticket)
    ticket_id = body.ticket_id or ticket.get("ticket_id") or f"order-review-ticket-{uuid.uuid4()}"
    requested_symbols = body.requested_symbols or _symbols_from_ticket(ticket)
    ready_count = _int_value(_first_present(
        body.ready_count,
        summary.get("ready_for_manual_approval_count"),
        summary.get("ready_count"),
        ticket.get("ready_for_manual_approval_count"),
        ticket.get("ready_count"),
    ), 0)
    blocked_count = _int_value(_first_present(
        body.blocked_count,
        summary.get("blocked_count"),
        ticket.get("blocked_count"),
    ), 0)
    status = _status_from_values(body, ready_count=ready_count, blocked_count=blocked_count)
    now = _now_iso()

    with db.connection_scope() as conn:
        cursor = db.get_cursor(conn)
        try:
            cursor.execute(f"""
                INSERT INTO order_review_ticket_audits
                    (ticket_id, account_id, correlation_id, source, mode, safety, status,
                     approval_required, execution_enabled, manual_confirmation_phrase, requested_symbols,
                     ready_count, blocked_count, orders_submitted, orders_cancelled, ticket_payload, metadata, updated_at)
                VALUES ({db.param_style}, {db.param_style}, {db.param_style}, {db.param_style}, {db.param_style},
                        {db.param_style}, {db.param_style}, {db.param_style}, {db.param_style}, {db.param_style},
                        {db.param_style}, {db.param_style}, {db.param_style}, {db.param_style}, {db.param_style},
                        {db.param_style}, {db.param_style}, {db.param_style})
            """, (
                str(ticket_id),
                str(body.account_id),
                body.correlation_id,
                body.source,
                body.mode or ticket.get("mode") or "manual_approval_ticket",
                body.safety or ticket.get("safety") or "read_only_no_orders_submitted_no_orders_cancelled",
                status,
                _db_bool(bool(_first_present(body.approval_required, ticket.get("approval_required"), default=True)), db.db_type),
                _db_bool(bool(_first_present(body.execution_enabled, ticket.get("execution_enabled"), default=False)), db.db_type),
                body.manual_confirmation_phrase or ticket.get("manual_confirmation_phrase"),
                json.dumps(requested_symbols),
                ready_count,
                blocked_count,
                _db_bool(bool(_first_present(body.orders_submitted, summary.get("orders_submitted"), ticket.get("orders_submitted"), default=False)), db.db_type),
                _db_bool(bool(_first_present(body.orders_cancelled, summary.get("orders_cancelled"), ticket.get("orders_cancelled"), default=False)), db.db_type),
                json.dumps(body.ticket_payload or {}),
                json.dumps(body.metadata or {}),
                now,
            ))
            conn.commit()
            record = get_order_review_ticket_audit(db, str(ticket_id))
            if not record:
                raise RuntimeError("OrderReview ticket audit was inserted but could not be read back")
            return record
        except Exception:
            conn.rollback()
            raise
        finally:
            cursor.close()


def get_order_review_ticket_audit(db, ticket_id: str) -> Optional[OrderReviewTicketRecord]:
    setup_order_review_ticket_table(db)
    with db.connection_scope() as conn:
        cursor = db.get_cursor(conn)
        try:
            cursor.execute(f"SELECT * FROM order_review_ticket_audits WHERE ticket_id = {db.param_style}", (ticket_id,))
            return _format_row(cursor.fetchone())
        finally:
            cursor.close()


def list_order_review_ticket_audits(db, query: ListOrderReviewTicketsQuery) -> List[OrderReviewTicketRecord]:
    setup_order_review_ticket_table(db)
    where_clauses: List[str] = []
    params: List[Any] = []

    if query.account_id is not None:
        where_clauses.append(f"account_id = {db.param_style}")
        params.append(str(query.account_id))
    if query.ticket_id:
        where_clauses.append(f"ticket_id = {db.param_style}")
        params.append(query.ticket_id)
    if query.status:
        where_clauses.append(f"status = {db.param_style}")
        params.append(query.status)
    if query.source:
        where_clauses.append(f"source = {db.param_style}")
        params.append(query.source)
    if query.approval_required is not None:
        where_clauses.append(f"approval_required = {db.param_style}")
        params.append(_db_bool(query.approval_required, db.db_type))
    if query.execution_enabled is not None:
        where_clauses.append(f"execution_enabled = {db.param_style}")
        params.append(_db_bool(query.execution_enabled, db.db_type))

    where_sql = f" WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
    sort_column = "created_at" if query.sort == "created_at" else "updated_at"
    sort_order = "ASC" if query.order == "asc" else "DESC"
    sql = f"SELECT * FROM order_review_ticket_audits{where_sql} ORDER BY {sort_column} {sort_order} LIMIT {db.param_style} OFFSET {db.param_style}"
    params.extend([query.limit, query.offset])

    with db.connection_scope() as conn:
        cursor = db.get_cursor(conn)
        try:
            cursor.execute(sql, tuple(params))
            return [record for record in (_format_row(row) for row in cursor.fetchall()) if record is not None]
        finally:
            cursor.close()
