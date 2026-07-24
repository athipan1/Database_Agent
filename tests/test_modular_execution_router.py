from datetime import datetime, timezone
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import Mock

from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.routers.execution import ROUTE_SIGNATURES, create_execution_router


def _runtime():
    async def get_correlation_id():
        return "corr-execution-router"

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

    return SimpleNamespace(
        db=object(),
        get_api_key=get_api_key,
        get_correlation_id=get_correlation_id,
        wrap_response=wrap_response,
        TRADING_MODE="PAPER",
        DATABASE_EMERGENCY_HALT=False,
        sync_broker_state=Mock(),
        create_risk_approval=Mock(),
        get_risk_approval=Mock(),
        mark_risk_approval_used=Mock(),
        create_execution_job=Mock(),
        get_execution_job=Mock(),
        get_execution_job_by_order_id=Mock(),
        claim_next_execution_job=Mock(),
        update_execution_job=Mock(),
        create_fill_record=Mock(),
        get_fill_records=Mock(return_value=[]),
        build_session_risk_snapshot=Mock(),
    )


def test_execution_router_declares_expected_route_signatures():
    router = create_execution_router(_runtime())
    signatures = {
        (route.path, method)
        for route in router.routes
        for method in (route.methods or set())
    }

    assert signatures == ROUTE_SIGNATURES


def test_fill_route_resolves_repository_function_at_request_time():
    runtime = _runtime()
    app = FastAPI()
    app.include_router(create_execution_router(runtime))
    client = TestClient(app)

    fill = {
        "fill_id": 1,
        "account_id": 1,
        "order_id": 10,
        "trade_id": "trade-modular-1",
        "symbol": "AAPL",
        "side": "sell",
        "quantity": 2,
        "fill_price": Decimal("110"),
        "average_entry_price": Decimal("100"),
        "gross_pnl": Decimal("20"),
        "fees": Decimal("1"),
        "realized_pnl": Decimal("19"),
        "broker_fill_id": "broker-fill-1",
        "broker_order_id": "broker-order-1",
        "liquidity": "taker",
        "filled_at": datetime.now(timezone.utc),
        "correlation_id": "corr-execution-router",
        "metadata": {"modular": True},
        "created_at": datetime.now(timezone.utc),
    }
    runtime.create_fill_record = Mock(return_value=fill)

    response = client.post(
        "/accounts/1/fills",
        json={
            "order_id": 10,
            "trade_id": "trade-modular-1",
            "symbol": "AAPL",
            "side": "sell",
            "quantity": 2,
            "fill_price": 110,
            "average_entry_price": 100,
            "fees": 1,
            "metadata": {"modular": True},
        },
    )

    assert response.status_code == 200
    assert response.json()["data"]["realized_pnl"] == 19.0
    runtime.create_fill_record.assert_called_once()
    assert runtime.create_fill_record.call_args.args[0] is runtime.db
