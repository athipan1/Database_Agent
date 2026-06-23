from __future__ import annotations

import json
from decimal import Decimal
from typing import Any, Dict, List, Optional


BUCKET_TARGETS = {
    "core_dividend": Decimal("0.50"),
    "value_rebound": Decimal("0.30"),
    "news_momentum": Decimal("0.20"),
    "unassigned": Decimal("0.00"),
}


def _param(db) -> str:
    return db.param_style


def _row_to_dict(row: Any) -> Dict[str, Any]:
    if row is None:
        return {}
    return dict(row)


def _json_load(value: Any, default: Any) -> Any:
    if value is None:
        return default
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(value)
    except Exception:
        return default


def _decimal(value: Any, default: Decimal = Decimal("0")) -> Decimal:
    try:
        if value is None or value == "":
            return default
        return Decimal(str(value))
    except Exception:
        return default


def _jsonable(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, dict):
        return {key: _jsonable(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_jsonable(item) for item in value]
    return value


def _table_exists(cursor, db, table: str) -> bool:
    if db.db_type == "sqlite":
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,))
    else:
        cursor.execute("SELECT to_regclass(%s)", (table,))
    return cursor.fetchone() is not None


def _latest_snapshot(cursor, db, account_id: int) -> Dict[str, Any]:
    if not _table_exists(cursor, db, "broker_sync_snapshots"):
        return {}
    p = _param(db)
    cursor.execute(f"""
        SELECT *
        FROM broker_sync_snapshots
        WHERE account_id = {p}
        ORDER BY created_at DESC, snapshot_id DESC
        LIMIT 1
    """, (account_id,))
    row = _row_to_dict(cursor.fetchone())
    if not row:
        return {}
    return {
        "snapshot_id": row.get("snapshot_id"),
        "account_id": row.get("account_id"),
        "broker": row.get("broker"),
        "paper": row.get("paper"),
        "captured_at": row.get("captured_at"),
        "created_at": row.get("created_at"),
        "account": _json_load(row.get("account_payload"), {}),
        "positions": _json_load(row.get("positions_payload"), []),
        "open_orders": _json_load(row.get("open_orders_payload"), []),
        "summary": _json_load(row.get("summary_payload"), {}),
    }


def _account(cursor, db, account_id: int) -> Dict[str, Any]:
    p = _param(db)
    cursor.execute(f"SELECT account_id, account_name, cash_balance FROM accounts WHERE account_id = {p}", (account_id,))
    return _row_to_dict(cursor.fetchone())


def _positions(cursor, db, account_id: int) -> List[Dict[str, Any]]:
    p = _param(db)
    cursor.execute(f"SELECT * FROM positions WHERE account_id = {p} ORDER BY symbol", (account_id,))
    return [_row_to_dict(row) for row in cursor.fetchall()]


def _orders(cursor, db, account_id: int) -> List[Dict[str, Any]]:
    p = _param(db)
    cursor.execute(f"""
        SELECT *
        FROM orders
        WHERE account_id = {p}
          AND status IN ('pending', 'placed', 'partially_filled')
        ORDER BY timestamp DESC, order_id DESC
    """, (account_id,))
    return [_row_to_dict(row) for row in cursor.fetchall()]


def _symbol_qty_map(positions: List[Dict[str, Any]], *, symbol_key: str = "symbol", qty_keys: tuple[str, ...] = ("quantity", "qty")) -> Dict[str, str]:
    result: Dict[str, str] = {}
    for item in positions or []:
        symbol = str(item.get(symbol_key) or "").upper()
        if not symbol:
            continue
        qty = None
        for key in qty_keys:
            if item.get(key) is not None:
                qty = item.get(key)
                break
        result[symbol] = str(qty if qty is not None else "0")
    return result


def _strategy_bucket(item: Dict[str, Any]) -> str:
    metadata = item.get("metadata") if isinstance(item.get("metadata"), dict) else {}
    bucket = str(item.get("strategy_bucket") or item.get("bucket") or metadata.get("strategy_bucket") or "unassigned").strip().lower()
    return bucket if bucket in BUCKET_TARGETS else "unassigned"


def _position_value(item: Dict[str, Any]) -> Decimal:
    market_value = _decimal(item.get("market_value") or item.get("value"))
    if market_value:
        return abs(market_value)
    qty = _decimal(item.get("quantity") or item.get("qty"))
    price = _decimal(item.get("current_market_price") or item.get("current_price") or item.get("average_cost") or item.get("avg_entry_price"))
    return abs(qty * price)


def _bucket_exposure(positions: List[Dict[str, Any]], account: Dict[str, Any]) -> Dict[str, Any]:
    equity = _decimal(account.get("equity") or account.get("portfolio_value") or account.get("cash_balance") or account.get("cash"))
    buckets: Dict[str, Dict[str, Any]] = {
        name: {
            "target_weight": str(target),
            "target_value": str((equity * target).quantize(Decimal("0.01"))) if equity else "0.00",
            "exposure": Decimal("0"),
            "symbols": [],
        }
        for name, target in BUCKET_TARGETS.items()
    }
    for item in positions or []:
        bucket = _strategy_bucket(item)
        exposure = _position_value(item)
        symbol = str(item.get("symbol") or "").upper()
        buckets[bucket]["exposure"] += exposure
        if symbol:
            buckets[bucket]["symbols"].append({"symbol": symbol, "exposure": exposure})
    for bucket, data in buckets.items():
        exposure = data["exposure"]
        data["exposure"] = str(exposure.quantize(Decimal("0.01")))
        data["current_weight"] = str((exposure / equity).quantize(Decimal("0.0001"))) if equity else "0"
        target = BUCKET_TARGETS[bucket]
        data["remaining_to_target"] = str(((equity * target) - exposure).quantize(Decimal("0.01"))) if equity else "0.00"
        data["symbols"] = [{"symbol": row["symbol"], "exposure": str(row["exposure"].quantize(Decimal("0.01")))} for row in data["symbols"]]
    return {"equity_basis": str(equity), "buckets": buckets}


def _mismatch_report(db_account: Dict[str, Any], db_positions: List[Dict[str, Any]], db_orders: List[Dict[str, Any]], snapshot: Dict[str, Any]) -> Dict[str, Any]:
    broker_account = snapshot.get("account") or {}
    broker_positions = snapshot.get("positions") or []
    broker_orders = snapshot.get("open_orders") or []
    db_position_map = _symbol_qty_map(db_positions, qty_keys=("quantity", "qty"))
    broker_position_map = _symbol_qty_map(broker_positions, qty_keys=("qty", "quantity"))
    mismatches: List[Dict[str, Any]] = []
    if broker_account:
        db_cash = str(db_account.get("cash_balance") or "")
        broker_cash = str(broker_account.get("cash") or "")
        if db_cash and broker_cash and db_cash != broker_cash:
            mismatches.append({"field": "cash_balance", "database": db_cash, "broker": broker_cash})
    if db_position_map != broker_position_map:
        mismatches.append({"field": "positions", "database": db_position_map, "broker": broker_position_map})
    if len(db_orders) != len(broker_orders):
        mismatches.append({"field": "open_orders_count", "database": len(db_orders), "broker": len(broker_orders)})
    return {
        "is_synced": len(mismatches) == 0 and bool(snapshot),
        "mismatch_count": len(mismatches),
        "mismatches": mismatches,
    }


def broker_sync_status(db, account_id: int = 1) -> Dict[str, Any]:
    with db.connection_scope() as conn:
        cursor = db.get_cursor(conn)
        try:
            snapshot = _latest_snapshot(cursor, db, account_id)
            account = _account(cursor, db, account_id)
            positions = _positions(cursor, db, account_id)
            open_orders = _orders(cursor, db, account_id)
            mismatch = _mismatch_report(account, positions, open_orders, snapshot)
            account_for_exposure = dict(account or {})
            if snapshot.get("account"):
                account_for_exposure.update(snapshot.get("account") or {})
            return _jsonable({
                "account_id": account_id,
                "has_snapshot": bool(snapshot),
                "latest_snapshot": snapshot,
                "database": {
                    "account": account,
                    "positions": positions,
                    "open_orders": open_orders,
                    "position_count": len(positions),
                    "open_order_count": len(open_orders),
                },
                "bucket_exposure": _bucket_exposure(positions, account_for_exposure),
                "mismatch": mismatch,
            })
        finally:
            cursor.close()
