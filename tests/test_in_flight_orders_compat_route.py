from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import Mock

from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.routers.accounts_orders import create_accounts_orders_router


def _order(order_id: int, status: str, *, account_id: int = 1):
    return {
        "order_id": order_id,
        "trade_id": f"trade-{order_id}",
        "account_id": account_id,
        "symbol": "AAPL",
        "side": "buy",
        "order_type": "market",
        "price": "100.00",
        "quantity": 1,
        "time_in_force": "GTC",
        "strategy_bucket": "quality_growth",
        "status": status,
        "executed_quantity": 0,
        "metadata": {},
    }


def _runtime(rows):
    async def get_correlation_id():
        return "corr-in-flight-orders"

    def get_api_key():
        return "test-key"

    def wrap_response(data=None, status="success", error=None):
        return {
            "status": status,
            "agent_type": "database",
            "version": "1.1.0",
            "schema_version": "1.0",
            "timestamp": datetime.now(timezone.utc),
            "correlation_id": "corr-in-flight-orders",
            "data": data,
            "metadata": {},
            "error": error,
            "confidence_score": None,
        }

    return SimpleNamespace(
        db=SimpleNamespace(get_orders=Mock(return_value=rows)),
        get_api_key=get_api_key,
        get_correlation_id=get_correlation_id,
        wrap_response=wrap_response,
        normalize_order_protective_metadata=lambda order: dict(order),
    )


def test_global_orders_compatibility_route_filters_in_flight_for_account_one():
    runtime = _runtime(
        [
            _order(1, "pending"),
            _order(2, "placed"),
            _order(3, "partially_filled"),
            _order(4, "executed"),
            _order(5, "cancelled"),
        ]
    )
    app = FastAPI()
    app.include_router(create_accounts_orders_router(runtime))

    response = TestClient(app).get(
        "/orders",
        params={"status": "in_flight", "limit": 2},
    )

    assert response.status_code == 200
    assert [row["order_id"] for row in response.json()["data"]] == [1, 2]
    runtime.db.get_orders.assert_called_once_with(1)


def test_global_orders_compatibility_route_supports_explicit_account_and_status():
    runtime = _runtime(
        [
            _order(10, "failed", account_id=7),
            _order(11, "placed", account_id=7),
        ]
    )
    app = FastAPI()
    app.include_router(create_accounts_orders_router(runtime))

    response = TestClient(app).get(
        "/orders",
        params={"account_id": 7, "status": "failed", "limit": 100},
    )

    assert response.status_code == 200
    assert [row["order_id"] for row in response.json()["data"]] == [10]
    runtime.db.get_orders.assert_called_once_with("7")


def test_global_orders_compatibility_route_rejects_unbounded_limit():
    runtime = _runtime([])
    app = FastAPI()
    app.include_router(create_accounts_orders_router(runtime))

    response = TestClient(app).get(
        "/orders",
        params={"status": "in_flight", "limit": 1001},
    )

    assert response.status_code == 422
    runtime.db.get_orders.assert_not_called()
