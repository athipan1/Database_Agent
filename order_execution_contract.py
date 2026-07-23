"""Install the account-aware order execution contract used by the API runtime.

The legacy ``TradingDB.execute_order`` method accepts only ``order_id`` and
returns a tuple ``(status, reason, account_id)``.  The FastAPI endpoint passes
``account_id`` and ``order_id`` and declares an ``OrderExecutionResponse`` data
object.  This adapter preserves the legacy one-argument repository contract
while providing the account-aware response required by the HTTP endpoint.
"""

from __future__ import annotations

from types import MethodType
from typing import Any, Dict, Optional


_INSTALL_FLAG = "_order_execution_contract_installed"
_ORIGINAL_EXECUTE_ORDER = "_runtime_original_execute_order"


def _failed_response(
    *,
    account_id: int,
    order_id: int,
    reason: str,
    trade_id: Optional[Any] = None,
) -> Dict[str, Any]:
    return {
        "order_id": order_id,
        "trade_id": trade_id,
        "account_id": account_id,
        "status": "failed",
        "reason": reason,
    }


def _execute_order_contract(self, *args):
    """Support both repository and account-aware API invocation styles."""
    original_execute_order = getattr(self, _ORIGINAL_EXECUTE_ORDER)

    if len(args) == 1:
        return original_execute_order(args[0])

    if len(args) != 2:
        raise TypeError(
            "execute_order expects order_id or account_id, order_id"
        )

    requested_account_id, requested_order_id = args
    account_id = int(requested_account_id)
    order_id = int(requested_order_id)

    order_before = self.get_order_by_id(order_id)
    if not order_before:
        return _failed_response(
            account_id=account_id,
            order_id=order_id,
            reason="order_not_found",
        )

    persisted_account_id = int(order_before["account_id"])
    trade_id = order_before.get("trade_id")
    if persisted_account_id != account_id:
        return _failed_response(
            account_id=account_id,
            order_id=order_id,
            trade_id=trade_id,
            reason="account_mismatch",
        )

    result = original_execute_order(order_id)
    if isinstance(result, dict):
        response = dict(result)
        response.setdefault("order_id", order_id)
        response.setdefault("trade_id", trade_id)
        response.setdefault("account_id", account_id)
        return response

    try:
        status, reason, executed_account_id = result
    except (TypeError, ValueError) as exc:
        raise TypeError(
            "legacy execute_order must return (status, reason, account_id)"
        ) from exc

    order_after = self.get_order_by_id(order_id) or order_before
    return {
        "order_id": order_id,
        "trade_id": order_after.get("trade_id", trade_id),
        "account_id": (
            int(executed_account_id)
            if executed_account_id is not None
            else account_id
        ),
        "status": status,
        "reason": reason,
    }


def install_order_execution_contract(main_module) -> None:
    """Install the runtime adapter once without changing legacy callers."""
    if getattr(main_module, _INSTALL_FLAG, False):
        return

    db = main_module.db
    setattr(db, _ORIGINAL_EXECUTE_ORDER, db.execute_order)
    db.execute_order = MethodType(_execute_order_contract, db)
    setattr(main_module, _INSTALL_FLAG, True)
