from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import Mock

from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.routers.accounts_orders import create_accounts_orders_router
from models import Order, TimeInForce


def production_acgl_order(time_in_force: str = "day") -> dict:
    return {
        "order_id": 1,
        "trade_id": "broker:11892a93-bac7-4c22-b11a-043818288cf8",
        "account_id": 1,
        "symbol": "ACGL",
        "side": "sell",
        "order_type": "limit",
        "price": "109.14000",
        "quantity": 151,
        "time_in_force": time_in_force,
        "strategy_bucket": "value_rebound",
        "status": "placed",
        "broker_order_id": "11892a93-bac7-4c22-b11a-043818288cf8",
        "broker_status": "new",
        "executed_quantity": 0,
        "metadata": {},
    }


def runtime_for_update(row: dict):
    async def get_correlation_id():
        return "corr-day-tif"

    def get_api_key():
        return "test-key"

    def wrap_response(data=None, status="success", error=None):
        return {
            "status": status,
            "agent_type": "database",
            "version": "1.1.0",
            "schema_version": "1.0",
            "timestamp": datetime.now(timezone.utc),
            "correlation_id": "corr-day-tif",
            "data": data,
            "metadata": {},
            "error": error,
            "confidence_score": None,
        }

    db = SimpleNamespace(update_order=Mock(return_value=row))
    return SimpleNamespace(
        db=db,
        get_api_key=get_api_key,
        get_correlation_id=get_correlation_id,
        wrap_response=wrap_response,
        _normalize_order_or_404=lambda value, message: dict(value),
    )


def test_order_model_accepts_case_insensitive_broker_time_in_force():
    assert Order.model_validate(production_acgl_order("day")).time_in_force is TimeInForce.DAY
    assert Order.model_validate(production_acgl_order("DAY")).time_in_force is TimeInForce.DAY
    assert Order.model_validate(production_acgl_order("gtc")).time_in_force is TimeInForce.GTC


def test_patch_order_response_accepts_existing_broker_day_value():
    row = production_acgl_order("day")
    runtime = runtime_for_update(row)
    app = FastAPI()
    app.include_router(create_accounts_orders_router(runtime))

    response = TestClient(app).patch(
        "/orders/1",
        json={"status": "placed", "broker_status": "new"},
    )

    assert response.status_code == 200
    body = response.json()
    assert body["data"]["symbol"] == "ACGL"
    assert body["data"]["time_in_force"] == "DAY"
    assert body["data"]["broker_status"] == "new"
    runtime.db.update_order.assert_called_once_with(
        1,
        {"status": "placed", "broker_status": "new"},
    )
