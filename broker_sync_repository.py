from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict, List

try:
    from psycopg2.extras import Json as PgJson
except Exception:  # pragma: no cover
    PgJson = None


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
    if app is None:
        return
    if getattr(app.state, "broker_sync_status_route_registered", False):
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

    app.add_api_route(
        "/broker-sync/status",
        broker_sync_status_endpoint,
        methods=["GET"],
        dependencies=dependencies,
        name="broker_sync_status_endpoint",
    )
    app.state.broker_sync_status_route_registered = True


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
                    broker TEXT,
                    paper BOOLEAN,
                    captured_at {timestamp_type},
                    account_payload {json_type} NOT NULL,
                    positions_payload {json_type} NOT NULL,
                    open_orders_payload {json_type} NOT NULL,
                    summary_payload {json_type} NOT NULL,
                    created_at {timestamp_type} DEFAULT CURRENT_TIMESTAMP
                )
            """)
            db._add_column_if_not_exists(cursor, "orders", "broker_synced_at", timestamp_type)
            db._add_column_if_not_exists(cursor, "orders", "broker_status", "TEXT")
            db._add_column_if_not_exists(cursor, "positions", "current_market_price", "TEXT" if db.db_type == "sqlite" else "NUMERIC(18, 5)")
            db._add_column_if_not_exists(cursor, "positions", "market_value", "TEXT" if db.db_type == "sqlite" else "NUMERIC(18, 5)")
            db._add_column_if_not_exists(cursor, "positions", "broker_synced_at", timestamp_type)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cursor.close()
    _register_status_route(db)


def _payload(value: Any, db) -> Any:
    data = value or {}
    if db.db_type == "sqlite":
        return json.dumps(data, ensure_ascii=False, default=str)
    if PgJson is not None:
        return PgJson(data, dumps=lambda obj: json.dumps(obj, ensure_ascii=False, default=str))
    return json.dumps(data, ensure_ascii=False, default=str)


def _ensure_account(cursor, db, account_id: int, state: Dict[str, Any]) -> Decimal:
    p = _param(db)
    account = state.get("account") or {}
    cash = _decimal(account.get("cash"))
    cursor.execute(f"SELECT account_id FROM accounts WHERE account_id = {p}", (account_id,))
    if cursor.fetchone():
        cursor.execute(f"UPDATE accounts SET cash_balance = {p} WHERE account_id = {p}", (str(cash), account_id))
    else:
        cursor.execute(
            f"INSERT INTO accounts (account_id, account_name, cash_balance) VALUES ({p}, {p}, {p})",
            (account_id, f"broker_account_{account_id}", str(cash)),
        )
    return cash


def _replace_positions(cursor, db, account_id: int, positions: List[Dict[str, Any]]) -> int:
    p = _param(db)
    synced_at = _now(db)
    cursor.execute(f"DELETE FROM positions WHERE account_id = {p}", (account_id,))
    count = 0
    for item in positions or []:
        symbol = str(item.get("symbol") or "").upper()
        if not symbol:
            continue
        quantity = _qty(item.get("qty") or item.get("quantity"))
        average_cost = _decimal(item.get("avg_entry_price") or item.get("average_cost"))
        current_price = _decimal(item.get("current_price"), average_cost)
        market_value = _decimal(item.get("market_value"), Decimal(quantity) * current_price)
        cursor.execute(
            f"""
            INSERT INTO positions (account_id, symbol, quantity, average_cost, current_market_price, market_value, broker_synced_at)
            VALUES ({p}, {p}, {p}, {p}, {p}, {p}, {p})
            """,
            (account_id, symbol, quantity, str(average_cost), str(current_price), str(market_value), synced_at),
        )
        count += 1
    return count


def _sync_open_orders(cursor, db, account_id: int, rows: List[Dict[str, Any]]) -> int:
    p = _param(db)
    synced_at = _now(db)
    count = 0
    for item in rows or []:
        broker_id = item.get("id") or item.get("broker_order_id")
        symbol = str(item.get("symbol") or "").upper()
        if not broker_id or not symbol:
            continue
        side = str(item.get("side") or "buy").lower()
        quantity = _qty(item.get("qty") or item.get("quantity"))
        kind = str(item.get("type") or item.get("order_type") or "market").lower()
        tif = str(item.get("time_in_force") or "day")
        price = item.get("limit_price") or item.get("stop_price") or item.get("price")
        state = _status(item.get("status"))
        raw_state = str(item.get("status") or "")
        filled = _qty(item.get("filled_qty") or item.get("executed_quantity"))
        submitted_at = item.get("submitted_at") or synced_at
        cursor.execute(f"SELECT order_id FROM orders WHERE broker_order_id = {p}", (str(broker_id),))
        if cursor.fetchone():
            cursor.execute(
                f"""
                UPDATE orders
                SET symbol = {p}, side = {p}, order_type = {p}, quantity = {p}, price = {p}, time_in_force = {p},
                    status = {p}, broker_status = {p}, executed_quantity = {p}, broker_synced_at = {p}
                WHERE broker_order_id = {p}
                """,
                (symbol, side, kind, quantity, str(price) if price is not None else None, tif, state, raw_state, filled, synced_at, str(broker_id)),
            )
        else:
            trade_id = f"broker:{broker_id}"
            cursor.execute(
                f"""
                INSERT INTO orders (account_id, trade_id, symbol, side, order_type, quantity, price, time_in_force, status, broker_order_id, broker_status, executed_quantity, timestamp, broker_synced_at)
                VALUES ({p}, {p}, {p}, {p}, {p}, {p}, {p}, {p}, {p}, {p}, {p}, {p}, {p}, {p})
                """,
                (account_id, trade_id, symbol, side, kind, quantity, str(price) if price is not None else None, tif, state, str(broker_id), raw_state, filled, submitted_at, synced_at),
            )
        count += 1
    return count


def _mark_missing_orders(cursor, db, account_id: int, rows: List[Dict[str, Any]]) -> int:
    p = _param(db)
    ids = [str(item.get("id") or item.get("broker_order_id")) for item in (rows or []) if item.get("id") or item.get("broker_order_id")]
    if ids:
        placeholders = ",".join([p] * len(ids))
        params = ["cancelled", "missing_from_broker_sync", account_id, *ids]
        cursor.execute(
            f"""
            UPDATE orders SET status = {p}, reason = {p}
            WHERE account_id = {p} AND broker_order_id IS NOT NULL
              AND status IN ('pending', 'placed', 'partially_filled')
              AND broker_order_id NOT IN ({placeholders})
            """,
            tuple(params),
        )
    else:
        cursor.execute(
            f"""
            UPDATE orders SET status = {p}, reason = {p}
            WHERE account_id = {p} AND broker_order_id IS NOT NULL
              AND status IN ('pending', 'placed', 'partially_filled')
            """,
            ("cancelled", "missing_from_broker_sync", account_id),
        )
    return cursor.rowcount if cursor.rowcount is not None else 0


def _insert_snapshot(cursor, db, account_id: int, state: Dict[str, Any]) -> None:
    p = _param(db)
    cursor.execute(
        f"""
        INSERT INTO broker_sync_snapshots (account_id, broker, paper, captured_at, account_payload, positions_payload, open_orders_payload, summary_payload)
        VALUES ({p}, {p}, {p}, {p}, {p}, {p}, {p}, {p})
        """,
        (
            account_id,
            state.get("broker"),
            bool(state.get("paper")),
            state.get("captured_at") or _now(db),
            _payload(state.get("account") or {}, db),
            _payload(state.get("positions") or [], db),
            _payload(state.get("open_orders") or [], db),
            _payload(state.get("summary") or {}, db),
        ),
    )


def sync_broker_state(db, broker_state: Dict[str, Any]) -> Dict[str, Any]:
    setup_broker_sync_tables(db)
    account_id = int(broker_state.get("account_id") or 1)
    positions = broker_state.get("positions") or []
    open_orders = broker_state.get("open_orders") or []
    with db.connection_scope() as conn:
        cursor = db.get_cursor(conn)
        try:
            cash = _ensure_account(cursor, db, account_id, broker_state)
            positions_synced = _replace_positions(cursor, db, account_id, positions)
            open_orders_synced = _sync_open_orders(cursor, db, account_id, open_orders)
            missing_marked = _mark_missing_orders(cursor, db, account_id, open_orders)
            _insert_snapshot(cursor, db, account_id, broker_state)
            conn.commit()
            return {
                "account_id": account_id,
                "cash_balance": cash,
                "positions_synced": positions_synced,
                "open_orders_synced": open_orders_synced,
                "missing_open_orders_marked_cancelled": missing_marked,
                "synced_at": datetime.now(timezone.utc).isoformat(),
            }
        except Exception:
            conn.rollback()
            raise
        finally:
            cursor.close()
