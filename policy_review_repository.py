import json
import uuid
from datetime import datetime, timezone
from typing import Any, List, Optional

from policy_review_models import (
    CreatePolicyReviewAuditBody,
    ListPolicyReviewAuditsQuery,
    PolicyReviewAuditRecord,
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


def _status_from_body(body: CreatePolicyReviewAuditBody) -> str:
    if body.status:
        return str(body.status)
    curated_state = (body.curated_policy or {}).get("curation_state")
    return str(curated_state or "created")


def _format_row(row: Any) -> Optional[PolicyReviewAuditRecord]:
    if not row:
        return None
    return PolicyReviewAuditRecord(
        policy_review_id=_row_get(row, "policy_review_id", 0),
        account_id=_row_get(row, "account_id", 1),
        symbol=_row_get(row, "symbol", 2),
        correlation_id=_row_get(row, "correlation_id", 3),
        source=_row_get(row, "source", 4) or "manager-agent",
        status=_row_get(row, "status", 5) or "created",
        advisory_only=_bool_from_db(_row_get(row, "advisory_only", 6)),
        auto_apply=_bool_from_db(_row_get(row, "auto_apply", 7)),
        performance_summary=_loads(_row_get(row, "performance_summary", 8), {}),
        learning_result=_loads(_row_get(row, "learning_result", 9), {}),
        curated_policy=_loads(_row_get(row, "curated_policy", 10), {}),
        metadata=_loads(_row_get(row, "metadata", 11), {}),
        created_at=_parse_dt(_row_get(row, "created_at", 12)),
        updated_at=_parse_dt(_row_get(row, "updated_at", 13)),
    )


def setup_policy_review_table(db) -> None:
    timestamp_type = "TEXT" if db.db_type == "sqlite" else "TIMESTAMPTZ"
    bool_type = "INTEGER" if db.db_type == "sqlite" else "BOOLEAN"
    bool_true = "1" if db.db_type == "sqlite" else "TRUE"
    bool_false = "0" if db.db_type == "sqlite" else "FALSE"
    with db.connection_scope() as conn:
        cursor = db.get_cursor(conn)
        try:
            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS policy_review_audits (
                    policy_review_id TEXT PRIMARY KEY,
                    account_id TEXT NOT NULL,
                    symbol TEXT,
                    correlation_id TEXT,
                    source TEXT NOT NULL DEFAULT 'manager-agent',
                    status TEXT NOT NULL DEFAULT 'created',
                    advisory_only {bool_type} NOT NULL DEFAULT {bool_true},
                    auto_apply {bool_type} NOT NULL DEFAULT {bool_false},
                    performance_summary TEXT DEFAULT '{{}}',
                    learning_result TEXT DEFAULT '{{}}',
                    curated_policy TEXT DEFAULT '{{}}',
                    metadata TEXT DEFAULT '{{}}',
                    created_at {timestamp_type} DEFAULT CURRENT_TIMESTAMP,
                    updated_at {timestamp_type} DEFAULT CURRENT_TIMESTAMP
                );
            """)
            if db.db_type == "postgres":
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_policy_review_account_status ON policy_review_audits(account_id, status)")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_policy_review_symbol ON policy_review_audits(symbol)")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_policy_review_correlation ON policy_review_audits(correlation_id)")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_policy_review_created_at ON policy_review_audits(created_at)")
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cursor.close()


def create_policy_review_audit(db, body: CreatePolicyReviewAuditBody) -> PolicyReviewAuditRecord:
    setup_policy_review_table(db)
    policy_review_id = body.policy_review_id or f"policy-review-{uuid.uuid4()}"
    now = _now_iso()
    status = _status_from_body(body)
    with db.connection_scope() as conn:
        cursor = db.get_cursor(conn)
        try:
            cursor.execute(f"""
                INSERT INTO policy_review_audits
                    (policy_review_id, account_id, symbol, correlation_id, source, status, advisory_only,
                     auto_apply, performance_summary, learning_result, curated_policy, metadata, updated_at)
                VALUES ({db.param_style}, {db.param_style}, {db.param_style}, {db.param_style}, {db.param_style},
                        {db.param_style}, {db.param_style}, {db.param_style}, {db.param_style}, {db.param_style},
                        {db.param_style}, {db.param_style}, {db.param_style})
            """, (
                policy_review_id,
                str(body.account_id),
                body.symbol.upper() if body.symbol else None,
                body.correlation_id,
                body.source,
                status,
                _db_bool(body.advisory_only, db.db_type),
                _db_bool(body.auto_apply, db.db_type),
                json.dumps(body.performance_summary or {}),
                json.dumps(body.learning_result or {}),
                json.dumps(body.curated_policy or {}),
                json.dumps(body.metadata or {}),
                now,
            ))
            conn.commit()
            record = get_policy_review_audit(db, policy_review_id)
            if not record:
                raise RuntimeError("PolicyReview audit was inserted but could not be read back")
            return record
        except Exception:
            conn.rollback()
            raise
        finally:
            cursor.close()


def get_policy_review_audit(db, policy_review_id: str) -> Optional[PolicyReviewAuditRecord]:
    setup_policy_review_table(db)
    with db.connection_scope() as conn:
        cursor = db.get_cursor(conn)
        try:
            cursor.execute(f"SELECT * FROM policy_review_audits WHERE policy_review_id = {db.param_style}", (policy_review_id,))
            return _format_row(cursor.fetchone())
        finally:
            cursor.close()


def list_policy_review_audits(db, query: ListPolicyReviewAuditsQuery) -> List[PolicyReviewAuditRecord]:
    setup_policy_review_table(db)
    where_clauses: List[str] = []
    params: List[Any] = []

    if query.account_id is not None:
        where_clauses.append(f"account_id = {db.param_style}")
        params.append(str(query.account_id))
    if query.symbol:
        where_clauses.append(f"symbol = {db.param_style}")
        params.append(query.symbol.upper())
    if query.status:
        where_clauses.append(f"status = {db.param_style}")
        params.append(query.status)
    if query.source:
        where_clauses.append(f"source = {db.param_style}")
        params.append(query.source)
    if query.advisory_only is not None:
        where_clauses.append(f"advisory_only = {db.param_style}")
        params.append(_db_bool(query.advisory_only, db.db_type))
    if query.auto_apply is not None:
        where_clauses.append(f"auto_apply = {db.param_style}")
        params.append(_db_bool(query.auto_apply, db.db_type))

    where_sql = f" WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
    sort_column = "created_at" if query.sort == "created_at" else "updated_at"
    sort_order = "ASC" if query.order == "asc" else "DESC"
    sql = f"SELECT * FROM policy_review_audits{where_sql} ORDER BY {sort_column} {sort_order} LIMIT {db.param_style} OFFSET {db.param_style}"
    params.extend([query.limit, query.offset])

    with db.connection_scope() as conn:
        cursor = db.get_cursor(conn)
        try:
            cursor.execute(sql, tuple(params))
            return [record for record in (_format_row(row) for row in cursor.fetchall()) if record is not None]
        finally:
            cursor.close()
