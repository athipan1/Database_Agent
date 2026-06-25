from __future__ import annotations

from decimal import Decimal
from typing import Any, Dict, List


def _decimal(value: Any, default: Decimal = Decimal("0")) -> Decimal:
    try:
        if value is None or value == "":
            return default
        return Decimal(str(value))
    except Exception:
        return default


def _money(value: Any) -> Decimal:
    return _decimal(value).quantize(Decimal("0.01"))


def _qty(value: Any) -> Decimal:
    return _decimal(value).quantize(Decimal("0.000001"))


def _position_qty_map(rows: List[Dict[str, Any]], qty_keys: tuple[str, ...]) -> Dict[str, str]:
    result: Dict[str, str] = {}
    for row in rows or []:
        symbol = str(row.get("symbol") or "").upper()
        if not symbol:
            continue
        qty = None
        for key in qty_keys:
            if row.get(key) is not None:
                qty = row.get(key)
                break
        result[symbol] = str(_qty(qty))
    return result


def _broker_order_id(row: Dict[str, Any]) -> str:
    return str(row.get("id") or row.get("broker_order_id") or "").strip()


def _database_order_id(row: Dict[str, Any]) -> str:
    return str(row.get("broker_order_id") or "").strip()


def broker_sync_diagnostics(
    db_account: Dict[str, Any],
    db_positions: List[Dict[str, Any]],
    db_orders: List[Dict[str, Any]],
    broker_snapshot: Dict[str, Any],
) -> Dict[str, Any]:
    broker_account = broker_snapshot.get("account") or {}
    broker_positions = broker_snapshot.get("positions") or []
    broker_orders = broker_snapshot.get("open_orders") or []

    db_cash = _money(db_account.get("cash_balance"))
    broker_cash = _money(broker_account.get("cash")) if broker_account else None
    cash_delta = (db_cash - broker_cash).quantize(Decimal("0.01")) if broker_cash is not None else None

    db_position_qty = _position_qty_map(db_positions, ("quantity", "qty"))
    broker_position_qty = _position_qty_map(broker_positions, ("qty", "quantity"))
    db_symbols = set(db_position_qty)
    broker_symbols = set(broker_position_qty)
    common_symbols = sorted(db_symbols & broker_symbols)

    db_order_ids = {_database_order_id(row) for row in db_orders or [] if _database_order_id(row)}
    broker_order_ids = {_broker_order_id(row) for row in broker_orders or [] if _broker_order_id(row)}

    return {
        "account": {
            "database_cash": str(db_cash),
            "broker_cash": str(broker_cash) if broker_cash is not None else None,
            "cash_delta": str(cash_delta) if cash_delta is not None else None,
            "cash_matches": broker_cash is not None and cash_delta == Decimal("0.00"),
        },
        "positions": {
            "database_count": len(db_position_qty),
            "broker_count": len(broker_position_qty),
            "missing_in_database": sorted(broker_symbols - db_symbols),
            "missing_in_broker": sorted(db_symbols - broker_symbols),
            "quantity_mismatches": [
                {"symbol": symbol, "database_qty": db_position_qty[symbol], "broker_qty": broker_position_qty[symbol]}
                for symbol in common_symbols
                if db_position_qty[symbol] != broker_position_qty[symbol]
            ],
            "matches": db_position_qty == broker_position_qty,
        },
        "open_orders": {
            "database_count": len(db_order_ids),
            "broker_count": len(broker_order_ids),
            "missing_in_database": sorted(broker_order_ids - db_order_ids),
            "missing_in_broker": sorted(db_order_ids - broker_order_ids),
            "matches": db_order_ids == broker_order_ids,
        },
    }


def broker_sync_summary(*, has_snapshot: bool, mismatch_count: int) -> Dict[str, str]:
    if not has_snapshot:
        return {
            "status": "no_snapshot",
            "severity": "warning",
            "recommended_action": "capture_broker_snapshot",
        }
    if mismatch_count:
        return {
            "status": "mismatch",
            "severity": "warning",
            "recommended_action": "refresh_broker_sync",
        }
    return {
        "status": "synced",
        "severity": "ok",
        "recommended_action": "none",
    }
