from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict, List, Optional, Union


def _numeric_type(db) -> str:
    return "TEXT" if getattr(db, "db_type", "postgres") == "sqlite" else "NUMERIC(18, 5)"


def _pk_type(db) -> str:
    return "INTEGER PRIMARY KEY AUTOINCREMENT" if getattr(db, "db_type", "postgres") == "sqlite" else "SERIAL PRIMARY KEY"


def _timestamp_type(db) -> str:
    return "TEXT" if getattr(db, "db_type", "postgres") == "sqlite" else "TIMESTAMPTZ"


def _now_for_db(db):
    now = datetime.now(timezone.utc)
    return now.isoformat() if getattr(db, "db_type", "postgres") == "sqlite" else now


def _as_decimal(value: Any, default: Decimal = Decimal("0")) -> Decimal:
    try:
        if value is None:
            return default
        return Decimal(str(value))
    except Exception:
        return default


def _as_datetime(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        dt = value
    else:
        try:
            dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except Exception:
            return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _format_decimal(db, value: Any) -> Optional[Decimal]:
    if value is None:
        return None
    try:
        return db._to_decimal(value)
    except Exception:
        return _as_decimal(value)


def setup_stock_accounting_tables(db) -> None:
    numeric_type = _numeric_type(db)
    pk_type = _pk_type(db)
    timestamp_type = _timestamp_type(db)
    with db.connection_scope() as conn:
        cursor = db.get_cursor(conn)
        try:
            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS position_lots (
                    lot_id {pk_type},
                    account_id INTEGER NOT NULL,
                    symbol TEXT NOT NULL,
                    opening_fill_id INTEGER,
                    opening_order_id INTEGER,
                    opening_trade_id TEXT,
                    quantity_open BIGINT NOT NULL,
                    quantity_initial BIGINT NOT NULL,
                    cost_basis {numeric_type} NOT NULL,
                    fees_allocated {numeric_type} DEFAULT 0,
                    opened_at {timestamp_type} NOT NULL,
                    closed_at {timestamp_type},
                    status TEXT DEFAULT 'open',
                    metadata TEXT,
                    created_at {timestamp_type} DEFAULT CURRENT_TIMESTAMP,
                    updated_at {timestamp_type} DEFAULT CURRENT_TIMESTAMP
                );
            """)
            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS equity_snapshots (
                    snapshot_id {pk_type},
                    account_id INTEGER NOT NULL,
                    cash_balance {numeric_type} NOT NULL,
                    positions_value {numeric_type} DEFAULT 0,
                    realized_pnl {numeric_type} DEFAULT 0,
                    unrealized_pnl {numeric_type} DEFAULT 0,
                    equity {numeric_type} NOT NULL,
                    source TEXT DEFAULT 'database_agent',
                    captured_at {timestamp_type} NOT NULL,
                    metadata TEXT,
                    created_at {timestamp_type} DEFAULT CURRENT_TIMESTAMP
                );
            """)
            if getattr(db, "db_type", "postgres") == "postgres":
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_position_lots_account_symbol_status ON position_lots (account_id, symbol, status, opened_at)")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_equity_snapshots_account_time ON equity_snapshots (account_id, captured_at DESC)")
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cursor.close()


def _format_lot_row(db, row) -> Dict[str, Any]:
    import json
    data = dict(row)
    for field in ["cost_basis", "fees_allocated"]:
        data[field] = _format_decimal(db, data.get(field))
    if isinstance(data.get("metadata"), str):
        try:
            data["metadata"] = json.loads(data["metadata"])
        except Exception:
            data["metadata"] = {}
    elif data.get("metadata") is None:
        data["metadata"] = {}
    return data


def get_open_lots(db, account_id: Union[int, str], *, symbol: Optional[str] = None) -> List[Dict[str, Any]]:
    setup_stock_accounting_tables(db)
    with db.connection_scope() as conn:
        cursor = db.get_cursor(conn)
        try:
            params: List[Any] = [int(account_id), "open"]
            query = f"SELECT * FROM position_lots WHERE account_id = {db.param_style} AND status = {db.param_style}"
            if symbol:
                query += f" AND symbol = {db.param_style}"
                params.append(str(symbol).upper())
            query += " ORDER BY opened_at ASC, lot_id ASC"
            cursor.execute(query, tuple(params))
            return [_format_lot_row(db, row) for row in cursor.fetchall()]
        finally:
            cursor.close()


def _get_fill_by_id(db, cursor, fill_id: Union[int, str]) -> Optional[Dict[str, Any]]:
    cursor.execute(f"SELECT * FROM fills WHERE fill_id = {db.param_style}", (int(fill_id),))
    row = cursor.fetchone()
    return dict(row) if row else None


def _insert_lot(db, cursor, fill: Dict[str, Any]) -> None:
    import json
    qty = int(fill.get("quantity") or 0)
    price = _as_decimal(fill.get("fill_price"))
    fees = _as_decimal(fill.get("fees"))
    cost_basis = (price * Decimal(qty)) + fees
    opened_at = fill.get("filled_at") or _now_for_db(db)
    cursor.execute(f"""
        INSERT INTO position_lots (
            account_id, symbol, opening_fill_id, opening_order_id, opening_trade_id,
            quantity_open, quantity_initial, cost_basis, fees_allocated, opened_at,
            status, metadata
        ) VALUES (
            {db.param_style}, {db.param_style}, {db.param_style}, {db.param_style}, {db.param_style},
            {db.param_style}, {db.param_style}, {db.param_style}, {db.param_style}, {db.param_style},
            {db.param_style}, {db.param_style}
        )
    """, (
        int(fill["account_id"]), str(fill["symbol"]).upper(), fill.get("fill_id"), fill.get("order_id"), str(fill.get("trade_id")) if fill.get("trade_id") is not None else None,
        qty, qty, str(cost_basis), str(fees), opened_at, "open", json.dumps({"source": "fill_accounting"}),
    ))


def _select_open_lots_for_update(db, cursor, account_id: Union[int, str], symbol: str) -> List[Dict[str, Any]]:
    query = f"SELECT * FROM position_lots WHERE account_id = {db.param_style} AND symbol = {db.param_style} AND status = {db.param_style} ORDER BY opened_at ASC, lot_id ASC"
    cursor.execute(query, (int(account_id), symbol.upper(), "open"))
    return [dict(row) for row in cursor.fetchall()]


def _consume_lots_fifo(db, cursor, fill: Dict[str, Any]) -> Dict[str, Any]:
    qty_to_close = int(fill.get("quantity") or 0)
    if qty_to_close <= 0:
        return {"closed_quantity": 0, "realized_pnl": Decimal("0"), "remaining_unmatched_quantity": 0}

    sale_price = _as_decimal(fill.get("fill_price"))
    sale_fees = _as_decimal(fill.get("fees"))
    lots = _select_open_lots_for_update(db, cursor, fill["account_id"], fill["symbol"])
    realized = Decimal("0")
    closed_quantity = 0
    remaining = qty_to_close

    for lot in lots:
        if remaining <= 0:
            break
        lot_qty = int(lot.get("quantity_open") or 0)
        if lot_qty <= 0:
            continue
        closing_qty = min(remaining, lot_qty)
        total_lot_cost = _as_decimal(lot.get("cost_basis"))
        unit_cost = total_lot_cost / Decimal(lot_qty)
        allocated_sale_fees = sale_fees * (Decimal(closing_qty) / Decimal(qty_to_close))
        lot_realized = ((sale_price * Decimal(closing_qty)) - allocated_sale_fees) - (unit_cost * Decimal(closing_qty))
        realized += lot_realized
        closed_quantity += closing_qty
        remaining -= closing_qty
        new_qty = lot_qty - closing_qty
        new_cost = unit_cost * Decimal(new_qty)
        status = "closed" if new_qty == 0 else "open"
        closed_at = fill.get("filled_at") if new_qty == 0 else None
        cursor.execute(f"""
            UPDATE position_lots
            SET quantity_open = {db.param_style}, cost_basis = {db.param_style}, status = {db.param_style}, closed_at = {db.param_style}, updated_at = {db.param_style}
            WHERE lot_id = {db.param_style}
        """, (new_qty, str(new_cost), status, closed_at, _now_for_db(db), lot["lot_id"]))

    return {"closed_quantity": closed_quantity, "realized_pnl": realized, "remaining_unmatched_quantity": remaining}


def apply_stock_fill_to_lots(db, fill_id: Union[int, str]) -> Dict[str, Any]:
    setup_stock_accounting_tables(db)
    with db.connection_scope() as conn:
        cursor = db.get_cursor(conn)
        try:
            fill = _get_fill_by_id(db, cursor, fill_id)
            if not fill:
                raise ValueError(f"Fill {fill_id} not found")
            side = str(fill.get("side") or "").lower()
            if side == "buy":
                _insert_lot(db, cursor, fill)
                result = {"accounting_action": "opened_lot", "fill_id": int(fill_id), "realized_pnl": Decimal("0"), "closed_quantity": 0, "remaining_unmatched_quantity": 0}
            elif side == "sell":
                result = _consume_lots_fifo(db, cursor, fill)
                result.update({"accounting_action": "closed_lots_fifo", "fill_id": int(fill_id)})
            else:
                result = {"accounting_action": "ignored", "fill_id": int(fill_id), "realized_pnl": Decimal("0"), "closed_quantity": 0, "remaining_unmatched_quantity": 0}
            conn.commit()
            return result
        except Exception:
            conn.rollback()
            raise
        finally:
            cursor.close()


def build_stock_portfolio_summary(db, account_id: Union[int, str]) -> Dict[str, Any]:
    setup_stock_accounting_tables(db)
    lots = get_open_lots(db, account_id)
    positions: Dict[str, Dict[str, Any]] = {}
    for lot in lots:
        symbol = lot["symbol"]
        item = positions.setdefault(symbol, {"symbol": symbol, "quantity": 0, "cost_basis": Decimal("0")})
        item["quantity"] += int(lot.get("quantity_open") or 0)
        item["cost_basis"] += _as_decimal(lot.get("cost_basis"))
    for item in positions.values():
        qty = Decimal(item["quantity"] or 0)
        item["average_cost"] = item["cost_basis"] / qty if qty > 0 else Decimal("0")
    try:
        cash_balance = _as_decimal(db.get_account_balance(account_id))
    except Exception:
        cash_balance = Decimal("0")
    total_cost_basis = sum((_as_decimal(item["cost_basis"]) for item in positions.values()), Decimal("0"))
    return {
        "account_id": account_id,
        "cash_balance": cash_balance,
        "positions_value_cost_basis": total_cost_basis,
        "equity_cost_basis": cash_balance + total_cost_basis,
        "open_positions": list(positions.values()),
        "open_lot_count": len(lots),
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }


def create_equity_snapshot(db, account_id: Union[int, str], *, positions_value: Any = None, unrealized_pnl: Any = Decimal("0"), source: str = "database_agent", metadata: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    import json
    setup_stock_accounting_tables(db)
    summary = build_stock_portfolio_summary(db, account_id)
    cash = _as_decimal(summary.get("cash_balance"))
    position_value = _as_decimal(positions_value, _as_decimal(summary.get("positions_value_cost_basis")))
    unrealized = _as_decimal(unrealized_pnl)
    realized = Decimal("0")
    equity = cash + position_value + unrealized
    captured_at = _now_for_db(db)
    with db.connection_scope() as conn:
        cursor = db.get_cursor(conn)
        try:
            cursor.execute(f"""
                INSERT INTO equity_snapshots (account_id, cash_balance, positions_value, realized_pnl, unrealized_pnl, equity, source, captured_at, metadata)
                VALUES ({db.param_style}, {db.param_style}, {db.param_style}, {db.param_style}, {db.param_style}, {db.param_style}, {db.param_style}, {db.param_style}, {db.param_style})
            """, (int(account_id), str(cash), str(position_value), str(realized), str(unrealized), str(equity), source, captured_at, json.dumps(metadata or {}, default=str)))
            if getattr(db, "db_type", "postgres") == "sqlite":
                snapshot_id = cursor.lastrowid
            else:
                cursor.execute("SELECT LASTVAL() AS snapshot_id")
                snapshot_id = cursor.fetchone()["snapshot_id"]
            conn.commit()
            return {"snapshot_id": snapshot_id, "account_id": account_id, "cash_balance": cash, "positions_value": position_value, "realized_pnl": realized, "unrealized_pnl": unrealized, "equity": equity, "source": source, "captured_at": captured_at, "metadata": metadata or {}}
        except Exception:
            conn.rollback()
            raise
        finally:
            cursor.close()
