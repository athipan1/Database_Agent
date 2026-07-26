"""Account, order, trade, portfolio, and price-history routes."""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Union

from fastapi import APIRouter, Depends, HTTPException, Query

from models import (
    AccountBalance,
    CreateOrderBody,
    ExecutionTrade,
    Order,
    OrderExecutionResponse,
    PortfolioMetrics,
    Position,
    Price,
    StandardAgentResponse,
)


IN_FLIGHT_ORDER_STATUSES = {"pending", "placed", "partially_filled"}


ROUTE_SIGNATURES = frozenset(
    {
        ("/orders", "GET"),
        ("/orders/trade/{trade_id}", "GET"),
        ("/orders/{order_id}", "GET"),
        ("/orders/{order_id}", "PATCH"),
        ("/accounts/{account_id}/balance", "GET"),
        ("/accounts/{account_id}/positions", "GET"),
        ("/accounts/{account_id}/orders", "GET"),
        ("/accounts/{account_id}/orders", "POST"),
        ("/accounts/{account_id}/orders/{order_id}/execute", "POST"),
        ("/accounts/{account_id}/trades", "GET"),
        ("/accounts/{account_id}/portfolio/metrics", "GET"),
        ("/prices/{symbol}/history", "GET"),
    }
)


def create_accounts_orders_router(runtime: Any) -> APIRouter:
    router = APIRouter(tags=["accounts-orders"])

    @router.get(
        "/orders",
        response_model=StandardAgentResponse[List[Order]],
    )
    async def list_orders_compatibility_endpoint(
        account_id: Union[int, str] = 1,
        status: Optional[str] = None,
        limit: int = Query(100, ge=1, le=1000),
        api_key: str = Depends(runtime.get_api_key),
        correlation_id: str = Depends(runtime.get_correlation_id),
    ):
        """Return account-scoped orders for Execution_Agent reconciliation.

        Execution_Agent historically queried ``GET /orders?status=in_flight``.
        Database_Agent's modular API moved order history under the account route,
        which left that runtime contract unresolved. This compatibility endpoint
        keeps the request account-scoped, defaults to the configured account 1
        used by the hourly Paper workflow, and filters deterministically.
        """

        logging.info(
            "Compatibility order lookup for account %s, status=%s, limit=%s.",
            account_id,
            status,
            limit,
        )
        try:
            orders = runtime.db.get_orders(account_id)
        except Exception as exc:
            logging.error(
                "Compatibility order lookup failed for account %s: %s",
                account_id,
                exc,
                exc_info=True,
            )
            raise HTTPException(status_code=500, detail=str(exc)) from exc

        normalized = [
            runtime.normalize_order_protective_metadata(order)
            for order in (orders or [])
        ]
        requested_status = str(status or "").strip().lower()
        if requested_status == "in_flight":
            normalized = [
                order
                for order in normalized
                if str(order.get("status") or "").strip().lower()
                in IN_FLIGHT_ORDER_STATUSES
            ]
        elif requested_status:
            normalized = [
                order
                for order in normalized
                if str(order.get("status") or "").strip().lower()
                == requested_status
            ]

        return runtime.wrap_response(data=normalized[:limit])

    @router.get(
        "/orders/trade/{trade_id}",
        response_model=StandardAgentResponse[Order],
    )
    async def get_order_by_trade_id_endpoint(
        trade_id: Union[int, str],
        api_key: str = Depends(runtime.get_api_key),
        correlation_id: str = Depends(runtime.get_correlation_id),
    ):
        order = runtime._normalize_order_or_404(
            runtime.db.get_order_by_trade_id(trade_id),
            f"Order trade_id {trade_id} not found",
        )
        return runtime.wrap_response(data=Order(**order))

    @router.get(
        "/orders/{order_id}",
        response_model=StandardAgentResponse[Order],
    )
    async def get_order_by_id_endpoint(
        order_id: int,
        api_key: str = Depends(runtime.get_api_key),
        correlation_id: str = Depends(runtime.get_correlation_id),
    ):
        order = runtime._normalize_order_or_404(
            runtime.db.get_order_by_id(order_id),
            f"Order {order_id} not found",
        )
        return runtime.wrap_response(data=Order(**order))

    @router.patch(
        "/orders/{order_id}",
        response_model=StandardAgentResponse[Order],
    )
    async def update_order_endpoint(
        order_id: int,
        updates: Dict[str, Any],
        api_key: str = Depends(runtime.get_api_key),
        correlation_id: str = Depends(runtime.get_correlation_id),
    ):
        order = runtime.db.update_order(order_id, updates)
        order = runtime._normalize_order_or_404(
            order,
            f"Order {order_id} not found",
        )
        return runtime.wrap_response(data=Order(**order))

    @router.get(
        "/accounts/{account_id}/balance",
        response_model=StandardAgentResponse[AccountBalance],
    )
    async def get_balance(
        account_id: Union[int, str],
        api_key: str = Depends(runtime.get_api_key),
        correlation_id: str = Depends(runtime.get_correlation_id),
    ):
        logging.info("Request to get balance for account %s.", account_id)
        try:
            balance = runtime.db.get_account_balance(account_id)
        except Exception as exc:
            logging.warning(
                "Balance lookup failed for account %s: %s",
                account_id,
                exc,
            )
            balance = None
        if balance is None:
            if (
                runtime.DATABASE_DEV_MODE
                and str(account_id) == str(runtime.DEFAULT_DEV_ACCOUNT_ID)
            ):
                return runtime.wrap_response(
                    data=AccountBalance(
                        account_id=account_id,
                        cash_balance=runtime.DEFAULT_DEV_CASH_BALANCE,
                    )
                )
            raise HTTPException(
                status_code=404,
                detail=f"Account {account_id} not found",
            )
        return runtime.wrap_response(
            data=AccountBalance(
                account_id=account_id,
                cash_balance=balance,
            )
        )

    @router.get(
        "/accounts/{account_id}/positions",
        response_model=StandardAgentResponse[List[Position]],
    )
    async def get_positions_for_account(
        account_id: Union[int, str],
        api_key: str = Depends(runtime.get_api_key),
        correlation_id: str = Depends(runtime.get_correlation_id),
    ):
        logging.info("Request to get positions for account %s.", account_id)
        try:
            positions = runtime.db.get_positions(account_id)
        except Exception as exc:
            logging.warning(
                "Positions lookup failed for account %s: %s",
                account_id,
                exc,
            )
            positions = []
        return runtime.wrap_response(data=positions or [])

    @router.get(
        "/accounts/{account_id}/orders",
        response_model=StandardAgentResponse[List[Order]],
    )
    async def get_orders_for_account(
        account_id: Union[int, str],
        api_key: str = Depends(runtime.get_api_key),
        correlation_id: str = Depends(runtime.get_correlation_id),
    ):
        logging.info("Request to get orders for account %s.", account_id)
        try:
            orders = runtime.db.get_orders(account_id)
        except Exception as exc:
            logging.warning(
                "Orders lookup failed for account %s: %s",
                account_id,
                exc,
            )
            orders = []
        normalized = [
            runtime.normalize_order_protective_metadata(order)
            for order in (orders or [])
        ]
        return runtime.wrap_response(data=normalized)

    @router.post(
        "/accounts/{account_id}/orders",
        response_model=StandardAgentResponse[Order],
    )
    async def create_order_for_account(
        account_id: Union[int, str],
        body: CreateOrderBody,
        api_key: str = Depends(runtime.get_api_key),
        correlation_id: str = Depends(runtime.get_correlation_id),
    ):
        logging.info(
            "Request to create order for account %s, symbol %s.",
            account_id,
            body.symbol,
        )
        try:
            runtime.setup_protective_order_columns(runtime.db)
            order_id = runtime.db.create_order(
                **runtime._order_body_to_create_args(
                    account_id,
                    body,
                    correlation_id,
                )
            )
            if order_id is not None:
                runtime.persist_protective_order_metadata(
                    runtime.db,
                    order_id,
                    risk_approval_id=body.risk_approval_id,
                    final_quantity=body.final_quantity,
                    guard_plan=body.guard_plan,
                    protective_exit=body.protective_exit,
                )
            order = (
                runtime.db.get_order_by_id(order_id)
                if order_id is not None
                else None
            )
            order = runtime.normalize_order_protective_metadata(order)
        except Exception as exc:
            logging.error("Order creation failed: %s", exc, exc_info=True)
            raise HTTPException(status_code=500, detail=str(exc)) from exc
        if not order:
            raise HTTPException(
                status_code=500,
                detail="Order creation did not return a persisted order",
            )
        return runtime.wrap_response(data=Order(**order))

    @router.post(
        "/accounts/{account_id}/orders/{order_id}/execute",
        response_model=StandardAgentResponse[OrderExecutionResponse],
    )
    async def execute_order_for_account(
        account_id: Union[int, str],
        order_id: Union[int, str],
        api_key: str = Depends(runtime.get_api_key),
        correlation_id: str = Depends(runtime.get_correlation_id),
    ):
        logging.info(
            "Request to execute order %s for account %s.",
            order_id,
            account_id,
        )
        try:
            result = runtime.db.execute_order(account_id, order_id)
        except Exception as exc:
            logging.error("Order execution failed: %s", exc, exc_info=True)
            raise HTTPException(status_code=500, detail=str(exc)) from exc
        return runtime.wrap_response(data=result)

    @router.get(
        "/accounts/{account_id}/trades",
        response_model=StandardAgentResponse[List[ExecutionTrade]],
    )
    async def get_trades_for_account(
        account_id: Union[int, str],
        api_key: str = Depends(runtime.get_api_key),
        correlation_id: str = Depends(runtime.get_correlation_id),
    ):
        logging.info("Request to get trades for account %s, symbol=%s.", account_id, None)
        try:
            trades = runtime.db.get_trade_history(account_id)
        except Exception as exc:
            logging.warning(
                "Trade history lookup failed for account %s: %s",
                account_id,
                exc,
            )
            trades = []
        return runtime.wrap_response(data=trades or [])

    @router.get(
        "/accounts/{account_id}/portfolio/metrics",
        response_model=StandardAgentResponse[PortfolioMetrics],
    )
    async def get_portfolio_metrics(
        account_id: Union[int, str],
        api_key: str = Depends(runtime.get_api_key),
        correlation_id: str = Depends(runtime.get_correlation_id),
    ):
        logging.info(
            "Request to get portfolio metrics for account %s.",
            account_id,
        )
        try:
            metrics = runtime.db.get_portfolio_metrics(account_id)
        except Exception as exc:
            logging.warning(
                "Portfolio metrics lookup failed for account %s: %s",
                account_id,
                exc,
            )
            metrics = None
        if metrics is None:
            if runtime.DATABASE_DEV_MODE:
                metrics = runtime._default_portfolio_metrics()
            else:
                raise HTTPException(
                    status_code=404,
                    detail=(
                        "Portfolio metrics not found for account "
                        f"{account_id}"
                    ),
                )
        return runtime.wrap_response(data=metrics)

    @router.get(
        "/prices/{symbol}/history",
        response_model=StandardAgentResponse[List[Price]],
    )
    async def get_price_history(
        symbol: str,
        limit: int = 100,
        api_key: str = Depends(runtime.get_api_key),
        correlation_id: str = Depends(runtime.get_correlation_id),
    ):
        logging.info(
            "Request to get price history for symbol %s with limit %s.",
            symbol,
            limit,
        )
        try:
            prices = runtime.db.get_price_history(symbol, limit=limit)
        except Exception as exc:
            logging.warning("Price history lookup failed for %s: %s", symbol, exc)
            prices = []
        if not prices:
            if runtime.DATABASE_DEV_MODE:
                return runtime.wrap_response(
                    data=runtime._mock_price_history(symbol, limit=limit)
                )
            raise HTTPException(
                status_code=404,
                detail=f"No price history found for {symbol}",
            )
        return runtime.wrap_response(data=prices)

    return router
