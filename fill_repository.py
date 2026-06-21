from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict, Optional, Union


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


def _to_decimal_or_none(value: Any) -> Optional[Decimal]:
    if value is None:
        return None
    return _as_decimal(value)


def setup_fill_table(db) -> None:
    numeric_type = _numeric_type(db)
    pk_type = _pk_type(db)
    timestamp_type = _timestamp_type(db)
    with db.connection_scope() as conn:
        cursor = db.get_cursor(conn)
        try:
            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS fills (
                    fill_id {pk_type},
                    account_id INTEGER NOT NULL,
                    order_id INTEGER,
                    trade_id TEXT,
                    symbol TEXT NOT NULL,
                    side TEXT NOT NULL,
                    quantity BIGINT NOT NULL,
                    fill_price {numeric_type} NOT NULL,
                    average_entry_price {numeric_type},
                    gross_pnl {numeric_type} DEFAULT 0,
                    fees {numeric_type} DEFAULT 0,
                    realized_pnl {numeric_type} DEFAULT 0,
                    broker_fill_id TEXT,
                    broker_order_id TEXT,
                    liquidity TEXT,
                    filled_at {timestamp_type} NOT NULL,
                    correlation_id TEXT,
                    metadata TEXT,
                    created_at {timestamp_type} DEFAULT CURRENT_TIMESTAMP
                );
            """)
            if getattr(db, "db_type", "postgres") == "postgres":
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_fills_account_time ON fills (account_id, filled_at DESC)")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_fills_account_symbol_time ON fills (account_id, symbol, filled_at DESC)")
                cursor.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_fills_broker_fill_id ON fills (broker_fill_id) WHERE broker_fill_id IS NOT NULL")
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cursor.close()


def _position_average_cost(db, cursor, account_id: Union[int, str], symbol: str) -> Optional[Decimal]:
    try:
        cursor.execute(
            f"SELECT average_cost FROM positions WHERE account_id = {db.param_style} AND symbol = {db.param_style}",
            (int(account_id), symbol.upper()),
        )
        row = cursor.fetchone()
        if not row:
            return None
        return _to_decimal_or_none(row["average_cost"])
    except Exception:
        return None


def calculate_fill_pnl(
    *,
    side: str,
    quantity: Union[int, Decimal],
    fill_price: Union[str, float, Decimal],
    average_entry_price: Optional[Union[str, float, Decimal]] = None,
    fees: Union[str, float, Decimal] = Decimal("0"),
) -> Dict[str, Decimal]:
    qty = _as_decimal(quantity)
    price = _as_decimal(fill_price)
    entry = _to_decimal_or_none(average_entry_price)
    fee_value = _as_decimal(fees)
    side_norm = str(side or "").lower()

    gross_pnl = Decimal("0")
    if side_norm == "sell" and entry is not None:
        gross_pnl = (price - entry) * qty
    elif side_norm == "buy" and entry is not None:
        # Covering short positions can be represented with side=buy and entry price.
        gross_pnl = (entry - price) * qty

    realized_pnl = gross_pnl - fee_value
    return {
        "gross_pnl": gross_pnl,
        "fees": fee_value,
        "realized_pnl": realized_pnl,
    }


def _format_fill_row(db, row) -> Dict[str, Any]:
    data = dict(row)
    for field in ["fill_price", "average_entry_price", "gross_pnl", "fees", "realized_pnl"]:
        if field in data:
            data[field] = db._to_decimal(data[field])
    return data


def create_fill_record(
    db,
    *,
    account_id: Union[int, str],
    order_id: Optional[int],
    trade_id: Optional[Union[int, str]],
    symbol: str,
    side: str,
    quantity: int,
    fill_price: Union[str, float, Decimal],
    average_entry_price: Optional[Union[str, float, Decimal]] = None,
    fees: Union[str, float, Decimal] = Decimal("0"),
    realized_pnl: Optional[Union[str, float, Decimal]] = None,
    broker_fill_id: Optional[str] = None,
    broker_order_id: Optional[str] = None,
    liquidity: Optional[str] = None,
    filled_at: Optional[Any] = None,
    correlation_id: Optional[str] = None,
    metadata: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    setup_fill_table(db)
    with db.connection_scope() as conn:
        cursor = db.get_cursor(conn)
        try:
            account_id_int = int(account_id)
            symbol_upper = symbol.upper()
            side_norm = side.lower()
            entry = _to_decimal_or_none(average_entry_price)
            if entry is None:
                entry = _position_average_cost(db, cursor, account_id_int, symbol_upper)

            pnl = calculate_fill_pnl(
                side=side_norm,
                quantity=quantity,
                fill_price=fill_price,
                average_entry_price=entry,
                fees=fees,
            )
            if realized_pnl is not None:
                pnl["realized_pnl"] = _as_decimal(realized_pnl)

            filled_at_value = filled_at or _now_for_db(db)
            if isinstance(filled_at_value, datetime) and getattr(db, "db_type", "postgres") == "sqlite":
                filled_at_value = filled_at_value.isoformat()

            import json
            metadata_text = json.dumps(metadata or {}, default=str)
            cursor.execute(f"""
                INSERT INTO fills (
                    account_id, order_id, trade_id, symbol, side, quantity,
                    fill_price, average_entry_price, gross_pnl, fees, realized_pnl,
                    broker_fill_id, broker_order_id, liquidity, filled_at,
                    correlation_id, metadata
                ) VALUES (
                    {db.param_style}, {db.param_style}, {db.param_style}, {db.param_style}, {db.param_style}, {db.param_style},
                    {db.param_style}, {db.param_style}, {db.param_style}, {db.param_style}, {db.param_style},
                    {db.param_style}, {db.param_style}, {db.param_style}, {db.param_style},
                    {db.param_style}, {db.param_style}
                )
            """, (
                account_id_int,
                order_id,
                str(trade_id) if trade_id is not None else None,
                symbol_upper,
                side_norm,
                int(quantity),
                str(_as_decimal(fill_price)),
                str(entry) if entry is not None else None,
                str(pnl["gross_pnl"]),
                str(pnl["fees"]),
                str(pnl["realized_pnl"]),
                broker_fill_id,
                broker_order_id,
                liquidity,
                filled_at_value,
                correlation_id,
                metadata_text,
            ))
            if getattr(db, "db_type", "postgres") == "sqlite":
                fill_id = cursor.lastrowid
            else:
                cursor.execute("SELECT LASTVAL() AS fill_id")
                fill_id = cursor.fetchone()["fill_id"]
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cursor.close()
    return get_fill_record(db, fill_id)


def get_fill_record(db, fill_id: Union[int, str]) -> Optional[Dict[str, Any]]:
    with db.connection_scope() as conn:
        cursor = db.get_cursor(conn)
        try:
            cursor.execute(f"SELECT * FROM fills WHERE fill_id = {db.param_style}", (int(fill_id),))
            row = cursor.fetchone()
            return _format_fill_row(db, row) if row else None
        finally:
            cursor.close()


def get_fill_records(db, account_id: Union[int, str], *, symbol: Optional[str] = None, limit: int = 100) -> list[Dict[str, Any]]:
    setup_fill_table(db)
    with db.connection_scope() as conn:
        cursor = db.get_cursor(conn)
        try:
            params: list[Any] = [int(account_id)]
            query = f"SELECT * FROM fills WHERE account_id = {db.param_style}"
            if symbol:
                query += f" AND symbol = {db.param_style}"
                params.append(symbol.upper())
            query += f" ORDER BY filled_at DESC, fill_id DESC LIMIT {db.param_style}"
            params.append(int(limit))
            cursor.execute(query, tuple(params))
            return [_format_fill_row(db, row) for row in cursor.fetchall()]
        finally:
            cursor.close()
