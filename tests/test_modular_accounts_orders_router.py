from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import Mock

from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.routers.accounts_orders import (
    ROUTE_SIGNATURES,
    create_accounts_orders_router,
)


def _runtime():
    async def get_correlation_id():
        return "corr-account-order-router"

    def get_api_key():
        return "test-key"

    def wrap_response(data=None, status="success", error=None):
        return {
            "status": status,
            "agent_type": "database",
            "version": "1.1.0",
            "timestamp": datetime.now(timezone.utc),
            "data": data,
            "error": error,
            "confidence_score": None,
        }

    db = SimpleNamespace(
        execute_order=Mock(
            return_value={
                "order_id": 42,
                "trade_id": "trade-modular-42",
                "account_id": 1,
                "status": "executed",
                "reason": None,
            }
        )
    )
    return SimpleNamespace(
        db=db,
        get_api_key=get_api_key,
        get_correlation_id=get_correlation_id,
        wrap_response=wrap_response,
    )


def test_accounts_orders_router_declares_expected_route_signatures():
    router = create_accounts_orders_router(_runtime())
    signatures = {
        (route.path, method)
        for route in router.routes
        for method in (route.methods or set())
    }

    assert signatures == ROUTE_SIGNATURES


def test_execute_route_uses_account_aware_database_contract():
    runtime = _runtime()
    app = FastAPI()
    app.include_router(create_accounts_orders_router(runtime))

    response = TestClient(app).post(
        "/accounts/1/orders/42/execute",
    )

    assert response.status_code == 200
    assert response.json()["data"] == {
        "order_id": 42,
        "trade_id": "trade-modular-42",
        "account_id": 1,
        "status": "executed",
        "reason": None,
    }
    runtime.db.execute_order.assert_called_once_with("1", "42")
