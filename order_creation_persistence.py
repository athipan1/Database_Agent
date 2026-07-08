"""Install the bucket-aware order creation contract used by the runtime entrypoint.

The legacy ``TradingDB.create_order`` method inserts an order before protective
metadata is persisted.  That left a short-lived (and failure-prone) state where
``orders.strategy_bucket`` was still the database default ``unassigned``.

This module upgrades the runtime contract without changing unrelated database
operations:

* ``_order_body_to_create_args`` forwards ``CreateOrderBody.strategy_bucket``.
* ``db.create_order`` writes the bucket in the initial INSERT transaction.
* duplicate trade IDs preserve idempotency and reject conflicting buckets.
"""

from __future__ import annotations

import sqlite3
from types import MethodType
from typing import Any, Optional

from protective_order_repository import ALLOWED_STRATEGY_BUCKETS


_INSTALL_FLAG = "_strategy_bucket_order_creation_installed"
_ORIGINAL_HELPER = "_strategy_bucket_original_order_body_to_create_args"
_ORIGINAL_CREATE_ORDER = "_strategy_bucket_original_create_order"


def _normalize_strategy_bucket(value: Any) -> str:
    bucket = str(value or "unassigned").strip().lower() or "unassigned"
    if bucket not in ALLOWED_STRATEGY_BUCKETS:
        raise ValueError(f"unsupported strategy_bucket: {value!r}")
    return bucket


def _is_unique_violation(exc: Exception) -> bool:
    return isinstance(exc, sqlite3.IntegrityError) or exc.__class__.__name__ == "UniqueViolation"


def _existing_order_after_duplicate(db, trade_id: str, requested_bucket: str) -> Optional[int]:
    """Return an idempotent existing order and fail closed on bucket conflicts."""
    with db.connection_scope() as conn:
        cursor = db.get_cursor(conn)
        try:
            cursor.execute(
                f"SELECT order_id, strategy_bucket FROM orders WHERE trade_id = {db.param_style}",
                (trade_id,),
            )
            existing = cursor.fetchone()
            if not existing:
                return None

            order_id = existing["order_id"]
            persisted_bucket = _normalize_strategy_bucket(existing["strategy_bucket"])

            if persisted_bucket == "unassigned" and requested_bucket != "unassigned":
                cursor.execute(
                    f"UPDATE orders SET strategy_bucket = {db.param_style} WHERE order_id = {db.param_style}",
                    (requested_bucket, order_id),
                )
                conn.commit()
                return order_id

            if (
                persisted_bucket != "unassigned"
                and requested_bucket != "unassigned"
                and persisted_bucket != requested_bucket
            ):
                raise ValueError(
                    "strategy_bucket_conflict: "
                    f"trade_id={trade_id!r}, persisted={persisted_bucket!r}, "
                    f"requested={requested_bucket!r}"
                )

            return order_id
        except Exception:
            conn.rollback()
            raise
        finally:
            cursor.close()


def _create_order_with_strategy_bucket(
    self,
    account_id,
    trade_id: str,
    symbol: str,
    side: str,
    order_type: str = "market",
    quantity: int = 0,
    price=None,
    time_in_force: str = "GTC",
    correlation_id: str = "",
    strategy_bucket: str = "unassigned",
) -> Optional[int]:
    """Insert an order and its strategy bucket atomically."""
    normalized_bucket = _normalize_strategy_bucket(strategy_bucket)
    normalized_trade_id = str(trade_id)

    with self.connection_scope() as conn:
        cursor = self.get_cursor(conn)
        try:
            normalized_account_id = int(account_id)
            query = f"""
                INSERT INTO orders (
                    account_id, trade_id, symbol, side, order_type, quantity,
                    price, time_in_force, strategy_bucket, status,
                    correlation_id, client_order_id
                )
                VALUES (
                    {self.param_style}, {self.param_style}, {self.param_style},
                    {self.param_style}, {self.param_style}, {self.param_style},
                    {self.param_style}, {self.param_style}, {self.param_style},
                    'pending', {self.param_style}, {self.param_style}
                )
            """
            params = (
                normalized_account_id,
                normalized_trade_id,
                symbol.upper(),
                side.lower(),
                order_type.lower(),
                quantity,
                str(price) if price is not None else None,
                time_in_force,
                normalized_bucket,
                correlation_id,
                normalized_trade_id,
            )
            cursor.execute(query, params)
            cursor.execute(
                f"SELECT order_id FROM orders WHERE trade_id = {self.param_style}",
                (normalized_trade_id,),
            )
            order_id = cursor.fetchone()["order_id"]
            conn.commit()
            return order_id
        except Exception as exc:
            conn.rollback()
            if _is_unique_violation(exc):
                return _existing_order_after_duplicate(
                    self,
                    normalized_trade_id,
                    normalized_bucket,
                )
            raise
        finally:
            cursor.close()


def install_strategy_bucket_order_creation(main_module) -> None:
    """Install the direct-insert contract once for the active Database runtime."""
    if getattr(main_module, _INSTALL_FLAG, False):
        return

    original_helper = main_module._order_body_to_create_args
    setattr(main_module, _ORIGINAL_HELPER, original_helper)

    def bucket_aware_order_body_to_create_args(account_id, body, correlation_id):
        args = original_helper(account_id, body, correlation_id)
        args["strategy_bucket"] = _normalize_strategy_bucket(
            getattr(body, "strategy_bucket", "unassigned")
        )
        return args

    main_module._order_body_to_create_args = bucket_aware_order_body_to_create_args

    db = main_module.db
    setattr(db, _ORIGINAL_CREATE_ORDER, db.create_order)
    db.create_order = MethodType(_create_order_with_strategy_bucket, db)
    setattr(main_module, _INSTALL_FLAG, True)
