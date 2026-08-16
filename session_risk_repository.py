from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Dict, Iterable, Optional, Union


TERMINAL_STATUSES = {"executed", "failed", "cancelled", "canceled"}
SYSTEM_PROVENANCE_SOURCE = "database_fill_order_execution_risk_chain.v1"


def _as_dict(value: Any) -> Dict[str, Any]:
    if isinstance(value, dict):
        return value
    if hasattr(value, "model_dump"):
        return value.model_dump(mode="json")
    if hasattr(value, "dict"):
        return value.dict()
    return {}


def _as_decimal(value: Any, default: Decimal = Decimal("0")) -> Decimal:
    try:
        if value is None:
            return default
        return Decimal(str(value))
    except Exception:
        return default


def _as_datetime(value: Any) -> Optional[datetime]:
    if not value:
        return None
    if isinstance(value, datetime):
        dt = value
    else:
        raw = str(value).replace("Z", "+00:00")
        try:
            dt = datetime.fromisoformat(raw)
        except ValueError:
            return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _status_of(row: Dict[str, Any]) -> str:
    return str(row.get("status") or row.get("outcome") or "").lower()


def _symbol_of(row: Dict[str, Any]) -> str:
    return str(row.get("symbol") or row.get("asset_id") or "").upper()


def _timestamp_of(row: Dict[str, Any]) -> Optional[datetime]:
    return _as_datetime(
        row.get("filled_at")
        or row.get("executed_at")
        or row.get("closed_at")
        or row.get("timestamp")
        or row.get("updated_at")
        or row.get("created_at")
    )


def _realized_pnl(row: Dict[str, Any]) -> Decimal:
    for key in ("realized_pnl", "net_pnl", "profit_loss", "pnl"):
        if row.get(key) is not None:
            return _as_decimal(row.get(key))

    metadata = row.get("metadata") or {}
    if isinstance(metadata, dict):
        for key in ("realized_pnl", "net_pnl", "profit_loss", "pnl"):
            if metadata.get(key) is not None:
                return _as_decimal(metadata.get(key))

    return_pct = row.get("return_pct")
    if return_pct is not None:
        entry_price = _as_decimal(row.get("entry_price"))
        quantity = _as_decimal(
            row.get("quantity") or row.get("executed_quantity"), Decimal("1")
        )
        notional = _as_decimal(row.get("notional"), entry_price * quantity)
        return notional * _as_decimal(return_pct)

    entry_price = row.get("entry_price") or row.get("average_entry_price")
    exit_price = (
        row.get("exit_price")
        or row.get("avg_execution_price")
        or row.get("fill_price")
    )
    quantity = row.get("quantity") or row.get("executed_quantity")
    side = str(row.get("side") or "buy").lower()
    if entry_price is not None and exit_price is not None and quantity is not None:
        entry = _as_decimal(entry_price)
        exit_ = _as_decimal(exit_price)
        qty = _as_decimal(quantity)
        raw = (exit_ - entry) * qty
        fees = _as_decimal(row.get("fees"))
        return (
            (-raw if side == "buy" else raw) - fees
            if side in {"buy", "sell"}
            else raw - fees
        )

    return Decimal("0")


def _terminal_rows(rows: Iterable[Any]) -> list[Dict[str, Any]]:
    result: list[Dict[str, Any]] = []
    for value in rows or []:
        row = _as_dict(value)
        if not row:
            continue
        status = _status_of(row)
        if (
            status in TERMINAL_STATUSES
            or row.get("realized_pnl") is not None
            or row.get("return_pct") is not None
            or row.get("filled_at") is not None
        ):
            result.append(row)
    return result


def _rows_since(
    rows: list[Dict[str, Any]],
    start: datetime,
    *,
    symbol: Optional[str] = None,
) -> list[Dict[str, Any]]:
    symbol_upper = symbol.upper() if symbol else None
    selected: list[Dict[str, Any]] = []
    for row in rows:
        ts = _timestamp_of(row)
        if not ts or ts < start:
            continue
        if symbol_upper and _symbol_of(row) != symbol_upper:
            continue
        selected.append(row)
    return selected


def _consecutive_losses(rows: list[Dict[str, Any]]) -> int:
    ordered = sorted(
        rows,
        key=lambda row: _timestamp_of(row)
        or datetime.min.replace(tzinfo=timezone.utc),
        reverse=True,
    )
    losses = 0
    for row in ordered:
        pnl = _realized_pnl(row)
        if pnl < 0:
            losses += 1
            continue
        if pnl > 0:
            break
    return losses


def _minutes_since(timestamp: Optional[datetime], now: datetime) -> Optional[float]:
    if timestamp is None:
        return None
    return max(0.0, (now - timestamp).total_seconds() / 60.0)


def _safe_rows_from(db, method_name: str, *args, **kwargs) -> list[Any]:
    if method_name == "get_fills":
        direct = getattr(db, "get_fills", None)
        if callable(direct):
            try:
                return direct(*args, **kwargs) or []
            except Exception:
                return []
        try:
            from fill_repository import get_fill_records

            return get_fill_records(db, args[0], limit=kwargs.get("limit", 10000)) or []
        except Exception:
            return []
    try:
        method = getattr(db, method_name)
    except AttributeError:
        return []
    try:
        return method(*args, **kwargs) or []
    except Exception:
        return []


def _row_value(row: Any, key: str, index: int) -> Any:
    try:
        return row[key]
    except Exception:
        try:
            return row[index]
        except Exception:
            return None


def _verify_system_managed_fill(
    db,
    account_id: Union[int, str],
    fill: Dict[str, Any],
) -> bool:
    """Verify fill provenance through the persisted execution safety chain.

    A fill is considered system-managed only when it can be linked to a normal
    (non broker-imported) order, a succeeded execution job, and a used Risk
    approval for that exact order/trade. The fill and order must both carry the
    same non-empty correlation ID. Any missing table/field/query failure returns
    False so profitability attribution fails closed.
    """

    order_id = fill.get("order_id")
    trade_id = str(fill.get("trade_id") or "").strip()
    fill_correlation_id = str(fill.get("correlation_id") or "").strip()
    if (
        order_id is None
        or not trade_id
        or trade_id.startswith("broker:")
        or not fill_correlation_id
    ):
        return False

    verifier = getattr(db, "verify_system_managed_fill", None)
    if callable(verifier):
        try:
            return bool(verifier(account_id, fill))
        except Exception:
            return False

    try:
        with db.connection_scope() as conn:
            cursor = db.get_cursor(conn)
            try:
                p = db.param_style
                cursor.execute(
                    f"""
                    SELECT o.trade_id, o.correlation_id, ej.status, ej.trade_id,
                           ra.status, ra.order_id
                    FROM orders o
                    JOIN execution_jobs ej ON ej.order_id = o.order_id
                    JOIN risk_approvals ra ON ra.order_id = o.order_id
                    WHERE o.order_id = {p}
                      AND o.account_id = {p}
                      AND o.trade_id = {p}
                      AND ej.trade_id = {p}
                      AND ej.status = 'succeeded'
                      AND ra.status = 'used'
                    LIMIT 1
                    """,
                    (int(order_id), str(account_id), trade_id, trade_id),
                )
                row = cursor.fetchone()
            finally:
                cursor.close()
    except Exception:
        return False

    if not row:
        return False
    order_trade_id = str(_row_value(row, "trade_id", 0) or "").strip()
    order_correlation_id = str(_row_value(row, "correlation_id", 1) or "").strip()
    execution_status = str(_row_value(row, "status", 2) or "").lower()
    execution_trade_id = str(_row_value(row, "trade_id", 3) or "").strip()
    approval_status = str(_row_value(row, "status", 4) or "").lower()
    approval_order_id = _row_value(row, "order_id", 5)

    return bool(
        order_trade_id == trade_id
        and execution_trade_id == trade_id
        and execution_status == "succeeded"
        and approval_status == "used"
        and int(approval_order_id) == int(order_id)
        and order_correlation_id
        and order_correlation_id == fill_correlation_id
        and not order_trade_id.startswith("broker:")
    )


def _system_provenance_summary(
    db,
    account_id: Union[int, str],
    *,
    daily_rows: list[Dict[str, Any]],
    fills_are_source: bool,
) -> Dict[str, Any]:
    if not fills_are_source:
        return {
            "system_provenance_verified": False,
            "system_managed_trades_today": 0,
            "system_unverified_trades_today": len(daily_rows),
            "system_managed_realized_pnl": 0.0,
            "system_provenance_source": "unavailable_without_fill_records",
        }

    managed_rows = [
        row
        for row in daily_rows
        if _verify_system_managed_fill(db, account_id, row)
    ]
    unverified_count = max(0, len(daily_rows) - len(managed_rows))
    complete = bool(daily_rows) and unverified_count == 0
    return {
        "system_provenance_verified": complete,
        "system_managed_trades_today": len(managed_rows),
        "system_unverified_trades_today": unverified_count,
        "system_managed_realized_pnl": float(
            sum((_realized_pnl(row) for row in managed_rows), Decimal("0"))
        ),
        "system_provenance_source": SYSTEM_PROVENANCE_SOURCE,
    }


def build_session_risk_snapshot(
    db,
    account_id: Union[int, str],
    *,
    symbol: Optional[str] = None,
    emergency_halt: bool = False,
    now: Optional[datetime] = None,
) -> Dict[str, Any]:
    now = now or datetime.now(timezone.utc)
    day_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    week_start = day_start - timedelta(days=day_start.weekday())

    fills = _safe_rows_from(db, "get_fills", account_id, limit=10000)
    trades = _safe_rows_from(db, "get_trade_history", account_id)
    orders = _safe_rows_from(db, "get_orders", account_id)

    fill_rows = _terminal_rows(fills)
    history_rows = fill_rows or _terminal_rows(trades) or _terminal_rows(orders)
    daily_rows = _rows_since(history_rows, day_start)
    weekly_rows = _rows_since(history_rows, week_start)
    symbol_rows = (
        _rows_since(history_rows, day_start, symbol=symbol) if symbol else []
    )

    provenance = _system_provenance_summary(
        db,
        account_id,
        daily_rows=daily_rows,
        fills_are_source=bool(fill_rows),
    )

    latest_loss_time = None
    for row in sorted(
        history_rows,
        key=lambda item: _timestamp_of(item)
        or datetime.min.replace(tzinfo=timezone.utc),
        reverse=True,
    ):
        if _realized_pnl(row) < 0:
            latest_loss_time = _timestamp_of(row)
            break

    latest_symbol_trade_time = None
    if symbol:
        for row in sorted(
            symbol_rows,
            key=lambda item: _timestamp_of(item)
            or datetime.min.replace(tzinfo=timezone.utc),
            reverse=True,
        ):
            latest_symbol_trade_time = _timestamp_of(row)
            break

    return {
        "account_id": account_id,
        "symbol": symbol.upper() if symbol else None,
        "daily_realized_pnl": float(
            sum((_realized_pnl(row) for row in daily_rows), Decimal("0"))
        ),
        "weekly_realized_pnl": float(
            sum((_realized_pnl(row) for row in weekly_rows), Decimal("0"))
        ),
        "consecutive_losses": _consecutive_losses(history_rows),
        "trades_today": len(daily_rows),
        "symbol_trades_today": len(symbol_rows),
        "minutes_since_last_loss": _minutes_since(latest_loss_time, now),
        "minutes_since_last_symbol_trade": _minutes_since(
            latest_symbol_trade_time, now
        ),
        "emergency_halt": bool(emergency_halt),
        "source": "database_agent_fills" if fill_rows else "database_agent",
        **provenance,
        "generated_at": now.isoformat(),
    }