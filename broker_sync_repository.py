from __future__ import annotations

import json
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict, List, Optional, Union


def _param(db) -> str:
    return db.param_style


def _now(db):
    return datetime.now(timezone.utc).isoformat() if db.db_type == "sqlite" else datetime.now(timezone.utc)


def _as_decimal(value: Any, default: Decimal = Decimal("0")) -> Decimal:
    try:
        if value is None or value == "":
            return default
        return Decimal(str(value))
    except Exception:
        return default


def _as_int_quantity(value: Any) -> int:
    try:
        return int(Decimal(str(value or 0)))
    except Exception:
        return 0


def _broker_order_status(status: Any) -> str:
    status_lower = str(status or "").lower()
    mapping = {
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
    }
    return mapping.get(status_lower, "placed" if status_lower else "pending")


def _broker_order_trade_id(broker_order: Dict[str, Any]) -> str:
    return f"broker:{broker_order.get('id') or broker_order.get('broker_order_id')}"


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
                );
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


def _json_payload(value: Any, db) -> Any:
    text = json.dumps(value or {}, ensure_ascii=False, default=str)
    if db.db_type == "sqlite":
        return text
    return text


def _ensure_account(cursor, db, account_id: int, broker_state: Dict[str, Any]) -> Decimal:
    p = _param(db)
    account = broker_state.get("account") or {}
    cash = _as_decimal(account.get("cash"))
    cursor.execute(f"SELECT account_id FROM accounts WHERE account_id = {p}", (account_id,))
    exists = cursor.fetchone()
    if exists:
        cursor.execute(f"UPDATE accounts SET cash_balance = {p} WHERE account_id = {p}", (str(cash), account_id))
    else:
        account_name = f"broker_account_{account_id}"
        cursor.execute(f"INSERT INTO accounts (account_id, account_name, cash_balance) VALUES ({p}, {p}, {p})", (account_id, account_name, str(cash)))
    return cash


def _replace_positions(cursor, db, account_id: int, positions: List[Dict[str, Any]]) -> int:
    p = _param(db)
    now = _now(db)
    cursor.execute(f"DELETE FROM positions WHERE account_id = {p}", (account_id,))
    count = 0
    for item in positions or []:
        symbol = str(item.get("symbol") or "").upper()
        if not symbol:
            continue
        quantity = _as_int_quantity(item.get("qty") or item.get("quantity"))
        average_cost = _as_decimal(item.get("avg_entry_price") or item.get("average_cost"))
        current_market_price = _as_decimal(item.get("current_price"), average_cost)
        market_value = _as_decimal(item.get("market_value"), Decimal(quantity) * current_market_price)
        cursor.execute(f"""
            INSERT INTO positions (account_id, symbol, quantity, average_cost, current_market_price, market_value, broker_synced_at)
            VALUES ({p}, {p}, {p}, {p}, {p}, {p}, {p})
        """, (account_id, symbol, quantity, str(average_cost), str(current_market_price), str(market_value), now))
        count += 1
    return count


def _upsert_open_orders(cursor, db, account_id: int, open_orders: List[Dict[str, Any]]) -> int:
    p = _param(db)
    now = _now(db)
    count = 0
    for item in open_orders or []:
        broker_order_id = item.get("id") or item.get("broker_order_id")
        symbol = str(item.get("symbol") or "").upper()
        if not broker_order_id or not symbol:
            continue
        side = str(item.get("side") or "buy").lower()
        quantity = _as_int_quantity(item.get("qty") or item.get("quantity"))
        order_type = str(item.get("type") or item.get("order_type") or "market").lower()
        time_in_force = str(item.get("time_in_force") or "day")
        price = item.get("limit_price") or item.get("stop_price") or item.get("price")
        status = _broker_order_status(item.get("status"))
        broker_status = str(item.get("status") or "")
        executed_quantity = _as_int_quantity(item.get("filled_qty") or item.get("executed_quantity"))
        submitted_at = item.get("submitted_at") or now
        trade_id = _broker_order_trade_id(item)

        cursor.execute(f"SELECT order_id FROM orders WHERE broker_order_id = {p}", (str(broker_order_id),))
        existing = cursor.fetchone()
        if existing:
            cursor.execute(f"""
                UPDATE orders
                SET symbol = {p}, side = {p}, order_type = {p}, quantity = {p}, price = {p}, time_in_force = {p},
                    status = {p}, broker_status = {p}, executed_quantity = {p}, broker_synced_at = {p}
                WHERE broker_order_id = {p}
            """, (symbol, side, order_type, quantity, str(price) if price is not None else None, time_in_force, status, broker_status, executed_quantity, now, str(broker_order_id)))
        else:
            cursor.execute(f"""
                INSERT INTO orders (account_id, trade_id, symbol, side, order_type, quantity, price, time_in_force, status, broker_order_id, broker_status, executed_quantity, timestamp, broker_synced_at)
                VALUES ({p}, {p}, {p}, {p}, {p}, {p}, {p}, {p}, {p}, {p}, {p}, {p}, {p}, {p})
            """, (account_id, trade_id, symbol, side, order_type, quantity, str(price) if price is not None else None, time_in_force, status, str(broker_order_id), broker_status, executed_quantity, submitted_at, now))
        count += 1
    return count


def _mark_missing_open_orders(cursor, db, account_id: int, open_orders: List[Dict[str, Any]]) -> int:
    p = _param(db)
    broker_ids = [str(item.get("id") or item.get("broker_order_id")) for item in (open_orders or []) if item.get("id") or item.get("broker_order_id")]
    if broker_ids:
        placeholders = ",".join([p] * len(broker_ids))
        params = ["cancelled", "missing_from_broker_sync", account_id, *broker_ids]
        cursor.execute(f"""
            UPDATE orders
            SET status = {p}, reason = {p}
            WHERE account_id = {p}
              AND broker_order_id IS NOT NULL
              AND status IN ('pending', 'placed', 'partially_filled')
              AND broker_order_id NOT IN ({placeholders})
        """, tuple(params))
    else:
        cursor.execute(f"""
            UPDATE orders
            SET status = {p}, reason = {p}
            WHERE account_id = {p}
              AND broker_order_id IS NOT NULL
              AND status IN ('pending', 'placed', 'partially_filled')
        """, ("cancelled", "missing_from_broker_sync", account_id))
    return cursor.rowcount if cursor.rowcount is not None else 0


def _insert_snapshot(cursor, db, account_id: int, broker_state: Dict[str, Any]) -> None:
    p = _param(db)
    cursor.execute(f"""
        INSERT INTO broker_sync_snapshots (account_id, broker, paper, captured_at, account_payload, positions_payload, open_orders_payload, summary_payload)
        VALUES ({p}, {p}, {p}, {p}, {p}, {p}, {p}, {p})
    """, (
        account_id,
        broker_state.get("broker"),
        bool(broker_state.get("paper")),
        broker_state.get("captured_at") or _now(db),
        _json_payload(broker_state.get("account") or {}, db),
        _json_payload(broker_state.get("positions") or [], db),
        _json_payload(broker_state.get("open_orders") or [], db),
        _json_payload(broker_state.get("summary") or {}, db),
    ))


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
            open_orders_synced = _upsert_open_orders(cursor, db, account_id, open_orders)
            stale_orders_marked = _mark_missing_open_orders(cursor, db, account_id, open_orders)
            _insert_snapshot(cursor, db, account_id, broker_state)
            conn.commit()
            return {
                "account_id": account_id,
                "cash_balance": cash,
                "positions_synced": positions_synced,
                "open_orders_synced": open_orders_synced,
                "missing_open_orders_marked_cancelled": stale_orders_marked,
                "synced_at": datetime.now(timezone.utc).isoformat(),
            }
        except Exception:
            conn.rollback()
            raise
        finally:
            cursor.close()
