from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict, List

from position_bucket_repository import setup_position_bucket_columns

try:
    from psycopg2.extras import Json as PgJson
except Exception:  # pragma: no cover
    PgJson = None

VALID_STRATEGY_BUCKETS = {"core_dividend", "quality_growth", "value_rebound", "news_momentum", "unassigned"}
UNASSIGNED = "unassigned"


def _param(db) -> str:
    return db.param_style


def _now(db):
    return datetime.now(timezone.utc).isoformat() if db.db_type == "sqlite" else datetime.now(timezone.utc)


def _decimal(value: Any, default: Decimal = Decimal("0")) -> Decimal:
    try:
        if value is None or value == "":
            return default
        return Decimal(str(value))
    except Exception:
        return default


def _qty(value: Any) -> int:
    try:
        return int(Decimal(str(value or 0)))
    except Exception:
        return 0


def _normalize_strategy_bucket(raw: Any) -> str:
    bucket = str(raw or UNASSIGNED).strip().lower()
    return bucket if bucket in VALID_STRATEGY_BUCKETS else UNASSIGNED


def _strategy_bucket(item: Dict[str, Any]) -> str:
    metadata = item.get("metadata") if isinstance(item.get("metadata"), dict) else {}
    raw = (
        item.get("strategy_bucket")
        or item.get("bucket")
        or item.get("allocation_bucket")
        or metadata.get("strategy_bucket")
        or metadata.get("bucket")
        or metadata.get("allocation_bucket")
        or UNASSIGNED
    )
    return _normalize_strategy_bucket(raw)


def _strategy_bucket_or_existing(item: Dict[str, Any], existing: Any) -> str:
    incoming_bucket = _strategy_bucket(item)
    if incoming_bucket != UNASSIGNED:
        return incoming_bucket
    if isinstance(existing, dict):
        existing = existing.get("strategy_bucket")
    existing_bucket = _normalize_strategy_bucket(existing)
    return existing_bucket if existing_bucket != UNASSIGNED else UNASSIGNED


def _strategy_bucket_source_or_existing(item: Dict[str, Any], existing: Any) -> str:
    if _strategy_bucket(item) != UNASSIGNED:
        return "broker_sync_payload"
    if isinstance(existing, dict):
        return str(existing.get("strategy_bucket_source") or "broker_sync_existing")
    return "broker_sync"


def _existing_position_buckets(cursor, db, account_id: int) -> Dict[str, Dict[str, Any]]:
    p = _param(db)
    try:
        cursor.execute(
            f"""
            SELECT symbol, strategy_bucket, strategy_bucket_source, strategy_bucket_reason, strategy_bucket_updated_at
            FROM positions WHERE account_id = {p}
            """,
            (account_id,),
        )
        output: Dict[str, Dict[str, Any]] = {}
        for row in cursor.fetchall():
            item = dict(row)
            symbol = str(item.get("symbol") or "").upper()
            if symbol:
                item["strategy_bucket"] = _normalize_strategy_bucket(item.get("strategy_bucket"))
                output[symbol] = item
        return output
    except Exception:
        return {}


def _existing_order_bucket(cursor, db, broker_id: str) -> str:
    p = _param(db)
    try:
        cursor.execute(f"SELECT strategy_bucket FROM orders WHERE broker_order_id = {p}", (str(broker_id),))
        row = cursor.fetchone()
        if not row:
            return UNASSIGNED
        if isinstance(row, dict):
            return _normalize_strategy_bucket(row.get("strategy_bucket"))
        return _normalize_strategy_bucket(row[0])
    except Exception:
        return UNASSIGNED


def _status(value: Any) -> str:
    raw = str(value or "").lower()
    return {
        "new": "placed",
        "accepted": "placed",
        "pending_new": "placed",
        "pending_cancel": "placed",
        "partially_filled": "partially_filled",
        "filled": "executed",
        "canceled": "cancelled",
        "cancelled": "cancelled",
        "expired": "failed",
        "rejected": "failed",
    }.get(raw, "placed" if raw else "pending")


def _main_module():
    return sys.modules.get("main") or sys.modules.get("__main__")


def _register_status_route(db) -> None:
    main_module = _main_module()
    app = getattr(main_module, "app", None)
    if app is None or getattr(app.state, "broker_sync_routes_registered", False):
        return
    from broker_sync_status_repository import broker_sync_status

    wrap_response = getattr(main_module, "wrap_response", None)
    if wrap_response is None:
        def wrap_response(data=None, status="success", error=None):
            return {"status": status, "agent_type": "database", "data": data, "error": error}

    dependencies = []
    get_api_key = getattr(main_module, "get_api_key", None)
    if get_api_key is not None:
        try:
            from fastapi import Depends
            dependencies = [Depends(get_api_key)]
        except Exception:
            dependencies = []

    async def broker_sync_status_endpoint(account_id: int = 1):
        return wrap_response(data=broker_sync_status(db, account_id=account_id))

    async def broker_sync_snapshot_endpoint(payload: Dict[str, Any]):
        return wrap_response(data=sync_broker_state(db, payload))

    app.add_api_route("/broker-sync/status", broker_sync_status_endpoint, methods=["GET"], dependencies=dependencies, name="broker_sync_status_endpoint")
    app.add_api_route("/broker-sync/snapshot", broker_sync_snapshot_endpoint, methods=["POST"], dependencies=dependencies, name="broker_sync_snapshot_endpoint")
    app.state.broker_sync_status_route_registered = True
    app.state.broker_sync_snapshot_route_registered = True
    app.state.broker_sync_routes_registered = True


def setup_broker_sync_tables(db) -> None:
    timestamp_type = "TEXT" if db.db_type == "sqlite" else "TIMESTAMPTZ"
    json_type = "TEXT" if db.db_type == "sqlite" else "JSONB"
    pk_type = "INTEGER PRIMARY KEY AUTOINCREMENT" if db.db_type == "sqlite" else "SERIAL PRIMARY KEY"
    with db.connection_scope() as conn:
        cursor = db.get_cursor(conn)
        try:
            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS broker_sync_snapshots (
                    snapshot_id {pk_type},
                    account_id INTEGER NOT NULL,
                    synced_at {timestamp_type} NOT NULL,
                    broker_name TEXT,
                    raw_snapshot {json_type}
                )
            """)
            db._add_column_if_not_exists(cursor, "positions", "strategy_bucket", "TEXT DEFAULT 'unassigned'")
            db._add_column_if_not_exists(cursor, "positions", "strategy_bucket_source", "TEXT DEFAULT 'unknown'")
            db._add_column_if_not_exists(cursor, "positions", "strategy_bucket_reason", "TEXT")
            db._add_column_if_not_exists(cursor, "positions", "strategy_bucket_updated_at", timestamp_type)
            db._add_column_if_not_exists(cursor, "orders", "strategy_bucket", "TEXT DEFAULT 'unassigned'")
            db._add_column_if_not_exists(cursor, "orders", "strategy_bucket_source", "TEXT DEFAULT 'unknown'")
            db._add_column_if_not_exists(cursor, "orders", "strategy_bucket_reason", "TEXT")
            db._add_column_if_not_exists(cursor, "orders", "strategy_bucket_updated_at", timestamp_type)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cursor.close()
    setup_position_bucket_columns(db)
    _register_status_route(db)


def sync_broker_state(db, payload: Dict[str, Any]) -> Dict[str, Any]:
    account_id = int(payload.get("account_id") or 1)
    broker_name = str(payload.get("broker_name") or payload.get("broker") or "alpaca")
    positions = payload.get("positions") or []
    orders = payload.get("orders") or []
    synced_at = _now(db)
    p = _param(db)
    with db.connection_scope() as conn:
        cursor = db.get_cursor(conn)
        try:
            existing_buckets = _existing_position_buckets(cursor, db, account_id)
            cursor.execute(
                f"INSERT INTO broker_sync_snapshots (account_id, synced_at, broker_name, raw_snapshot) VALUES ({p}, {p}, {p}, {p})",
                (account_id, synced_at, broker_name, json.dumps(payload) if db.db_type == "sqlite" or PgJson is None else PgJson(payload)),
            )
            for item in positions:
                if not isinstance(item, dict):
                    continue
                symbol = str(item.get("symbol") or "").upper()
                if not symbol:
                    continue
                existing = existing_buckets.get(symbol) or {}
                bucket = _strategy_bucket_or_existing(item, existing)
                bucket_source = _strategy_bucket_source_or_existing(item, existing)
                cursor.execute(
                    f"""
                    INSERT INTO positions (account_id, symbol, quantity, average_cost, current_market_price, market_value, updated_at, strategy_bucket, strategy_bucket_source, strategy_bucket_reason, strategy_bucket_updated_at)
                    VALUES ({p}, {p}, {p}, {p}, {p}, {p}, {p}, {p}, {p}, {p}, {p})
                    ON CONFLICT(account_id, symbol) DO UPDATE SET
                        quantity = excluded.quantity,
                        average_cost = excluded.average_cost,
                        current_market_price = excluded.current_market_price,
                        market_value = excluded.market_value,
                        updated_at = excluded.updated_at,
                        strategy_bucket = excluded.strategy_bucket,
                        strategy_bucket_source = excluded.strategy_bucket_source,
                        strategy_bucket_reason = excluded.strategy_bucket_reason,
                        strategy_bucket_updated_at = excluded.strategy_bucket_updated_at
                    """,
                    (
                        account_id,
                        symbol,
                        _qty(item.get("qty") or item.get("quantity")),
                        _decimal(item.get("avg_entry_price") or item.get("average_cost") or item.get("average_entry_price")),
                        _decimal(item.get("current_price") or item.get("current_market_price") or item.get("market_price")),
                        _decimal(item.get("market_value")),
                        synced_at,
                        bucket,
                        bucket_source,
                        item.get("strategy_bucket_reason") or item.get("bucket_reason"),
                        synced_at,
                    ),
                )
            for item in orders:
                if not isinstance(item, dict):
                    continue
                broker_id = str(item.get("id") or item.get("order_id") or item.get("broker_order_id") or "")
                if not broker_id:
                    continue
                bucket = _strategy_bucket(item)
                if bucket == UNASSIGNED:
                    bucket = _existing_order_bucket(cursor, db, broker_id)
                cursor.execute(
                    f"""
                    INSERT INTO orders (account_id, broker_order_id, symbol, side, order_type, quantity, price, status, created_at, updated_at, strategy_bucket, strategy_bucket_source, strategy_bucket_reason, strategy_bucket_updated_at)
                    VALUES ({p}, {p}, {p}, {p}, {p}, {p}, {p}, {p}, {p}, {p}, {p}, {p}, {p}, {p})
                    ON CONFLICT(broker_order_id) DO UPDATE SET
                        symbol = excluded.symbol,
                        side = excluded.side,
                        order_type = excluded.order_type,
                        quantity = excluded.quantity,
                        price = excluded.price,
                        status = excluded.status,
                        updated_at = excluded.updated_at,
                        strategy_bucket = excluded.strategy_bucket,
                        strategy_bucket_source = excluded.strategy_bucket_source,
                        strategy_bucket_reason = excluded.strategy_bucket_reason,
                        strategy_bucket_updated_at = excluded.strategy_bucket_updated_at
                    """,
                    (
                        account_id,
                        broker_id,
                        str(item.get("symbol") or "").upper(),
                        str(item.get("side") or ""),
                        str(item.get("type") or item.get("order_type") or ""),
                        _qty(item.get("qty") or item.get("quantity")),
                        _decimal(item.get("limit_price") or item.get("price") or item.get("stop_price")),
                        _status(item.get("status")),
                        item.get("created_at") or synced_at,
                        synced_at,
                        bucket,
                        "broker_sync_payload" if bucket != UNASSIGNED else "broker_sync",
                        item.get("strategy_bucket_reason") or item.get("bucket_reason"),
                        synced_at,
                    ),
                )
            conn.commit()
            return {"status": "success", "account_id": account_id, "positions_synced": len(positions), "orders_synced": len(orders)}
        except Exception as exc:
            conn.rollback()
            return {"status": "error", "error": str(exc)}
        finally:
            cursor.close()
