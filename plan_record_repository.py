import json
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from fastapi import HTTPException

from trade_plan_models import CreateTradePlanBody, TradePlanRecord, UpdateTradePlanStatusBody


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


def _loads(raw: Any, default):
    if raw is None:
        return default
    if isinstance(raw, (dict, list)):
        return raw
    try:
        return json.loads(raw)
    except Exception:
        return default


def _status_value(value: Any) -> str:
    return value.value if hasattr(value, "value") else str(value)


def _event(status: Any, *, reason: str | None = None, metadata: Dict[str, Any] | None = None) -> Dict[str, Any]:
    event = {
        "status": _status_value(status),
        "timestamp": _now_iso(),
    }
    if reason:
        event["reason"] = reason
    if metadata:
        event["metadata"] = metadata
    return event


def _format_row(row: Any) -> Optional[TradePlanRecord]:
    if not row:
        return None
    return TradePlanRecord(
        trade_plan_id=_row_get(row, "trade_plan_id", 0),
        account_id=_row_get(row, "account_id", 1),
        symbol=_row_get(row, "symbol", 2),
        side=str(_row_get(row, "side", 3)).lower(),
        status=str(_row_get(row, "status", 4)).lower(),
        correlation_id=_row_get(row, "correlation_id", 5),
        source=_row_get(row, "source", 6) or "manager-agent",
        strategy=_row_get(row, "strategy", 7) or "unassigned",
        strategy_bucket=_row_get(row, "strategy_bucket", 8) or "unassigned",
        risk_approval_id=_row_get(row, "risk_approval_id", 9),
        order_id=_row_get(row, "order_id", 10),
        execution_job_id=_row_get(row, "execution_job_id", 11),
        broker_order_id=_row_get(row, "broker_order_id", 12),
        plan=_loads(_row_get(row, "plan", 13), {}),
        lifecycle=_loads(_row_get(row, "lifecycle", 14), []),
        metadata=_loads(_row_get(row, "metadata", 15), {}),
        created_at=_parse_dt(_row_get(row, "created_at", 16)),
        updated_at=_parse_dt(_row_get(row, "updated_at", 17)),
    )


def setup_plan_record_table(db) -> None:
    timestamp_type = "TEXT" if db.db_type == "sqlite" else "TIMESTAMPTZ"
    with db.connection_scope() as conn:
        cursor = db.get_cursor(conn)
        try:
            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS trade_plans (
                    trade_plan_id TEXT PRIMARY KEY,
                    account_id TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    side TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'created',
                    correlation_id TEXT,
                    source TEXT NOT NULL DEFAULT 'manager-agent',
                    strategy TEXT NOT NULL DEFAULT 'unassigned',
                    strategy_bucket TEXT NOT NULL DEFAULT 'unassigned',
                    risk_approval_id TEXT,
                    order_id INTEGER,
                    execution_job_id TEXT,
                    broker_order_id TEXT,
                    plan TEXT DEFAULT '{{}}',
                    lifecycle TEXT DEFAULT '[]',
                    metadata TEXT DEFAULT '{{}}',
                    created_at {timestamp_type} DEFAULT CURRENT_TIMESTAMP,
                    updated_at {timestamp_type} DEFAULT CURRENT_TIMESTAMP
                );
            """)
            if db.db_type == "postgres":
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_trade_plans_account_status ON trade_plans(account_id, status)")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_trade_plans_symbol ON trade_plans(symbol)")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_trade_plans_risk_approval ON trade_plans(risk_approval_id)")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_trade_plans_order_id ON trade_plans(order_id)")
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cursor.close()


def create_plan_record(db, body: CreateTradePlanBody) -> TradePlanRecord:
    setup_plan_record_table(db)
    status = _status_value(body.status)
    lifecycle = [_event(status, reason="trade_plan_created", metadata={"source": body.source})]
    with db.connection_scope() as conn:
        cursor = db.get_cursor(conn)
        try:
            cursor.execute(f"""
                INSERT INTO trade_plans
                    (trade_plan_id, account_id, symbol, side, status, correlation_id, source, strategy,
                     strategy_bucket, risk_approval_id, order_id, execution_job_id, broker_order_id,
                     plan, lifecycle, metadata, updated_at)
                VALUES ({db.param_style}, {db.param_style}, {db.param_style}, {db.param_style}, {db.param_style},
                        {db.param_style}, {db.param_style}, {db.param_style}, {db.param_style}, {db.param_style},
                        {db.param_style}, {db.param_style}, {db.param_style}, {db.param_style}, {db.param_style},
                        {db.param_style}, {db.param_style})
            """, (
                body.trade_plan_id,
                str(body.account_id),
                body.symbol.upper(),
                body.side.value.lower(),
                status,
                body.correlation_id,
                body.source,
                body.strategy,
                body.strategy_bucket,
                body.risk_approval_id,
                body.order_id,
                str(body.execution_job_id) if body.execution_job_id is not None else None,
                body.broker_order_id,
                json.dumps(body.plan or {}),
                json.dumps(lifecycle),
                json.dumps(body.metadata or {}),
                _now_iso(),
            ))
            conn.commit()
            record = get_plan_record(db, body.trade_plan_id)
            if not record:
                raise RuntimeError("TradePlan was inserted but could not be read back")
            return record
        except Exception:
            conn.rollback()
            raise
        finally:
            cursor.close()


def get_plan_record(db, trade_plan_id: str) -> Optional[TradePlanRecord]:
    setup_plan_record_table(db)
    with db.connection_scope() as conn:
        cursor = db.get_cursor(conn)
        try:
            cursor.execute(f"SELECT * FROM trade_plans WHERE trade_plan_id = {db.param_style}", (trade_plan_id,))
            return _format_row(cursor.fetchone())
        finally:
            cursor.close()


def update_plan_record_status(db, trade_plan_id: str, body: UpdateTradePlanStatusBody) -> TradePlanRecord:
    setup_plan_record_table(db)
    now = _now_iso()
    with db.connection_scope() as conn:
        cursor = db.get_cursor(conn)
        try:
            lock_clause = " FOR UPDATE" if db.db_type == "postgres" else ""
            cursor.execute(f"SELECT * FROM trade_plans WHERE trade_plan_id = {db.param_style}{lock_clause}", (trade_plan_id,))
            current = _format_row(cursor.fetchone())
            if not current:
                raise HTTPException(status_code=404, detail=f"TradePlan {trade_plan_id} not found")
            metadata = {**(current.metadata or {}), **(body.metadata or {})}
            lifecycle = list(current.lifecycle or [])
            lifecycle.append(_event(body.status, reason=body.reason, metadata=body.metadata))
            cursor.execute(f"""
                UPDATE trade_plans
                SET status = {db.param_style},
                    risk_approval_id = COALESCE({db.param_style}, risk_approval_id),
                    order_id = COALESCE({db.param_style}, order_id),
                    execution_job_id = COALESCE({db.param_style}, execution_job_id),
                    broker_order_id = COALESCE({db.param_style}, broker_order_id),
                    lifecycle = {db.param_style},
                    metadata = {db.param_style},
                    updated_at = {db.param_style}
                WHERE trade_plan_id = {db.param_style}
            """, (
                _status_value(body.status),
                body.risk_approval_id,
                body.order_id,
                str(body.execution_job_id) if body.execution_job_id is not None else None,
                body.broker_order_id,
                json.dumps(lifecycle),
                json.dumps(metadata),
                now,
                trade_plan_id,
            ))
            conn.commit()
            updated = get_plan_record(db, trade_plan_id)
            if not updated:
                raise RuntimeError("TradePlan was updated but could not be read back")
            return updated
        except HTTPException:
            conn.rollback()
            raise
        except Exception:
            conn.rollback()
            raise
        finally:
            cursor.close()
