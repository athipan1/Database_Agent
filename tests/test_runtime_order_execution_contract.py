import os
from unittest.mock import patch

from fastapi.testclient import TestClient

os.environ.setdefault("DATABASE_AGENT_API_KEY", "test-key")
os.environ.setdefault("DATABASE_DEV_MODE", "true")
os.environ.setdefault("TRADING_MODE", "PAPER")
os.environ.setdefault("ALPACA_API_KEY", "test-alpaca-key")
os.environ.setdefault("ALPACA_SECRET_KEY", "test-alpaca-secret")

import main_with_skill_routes as runtime


client = TestClient(runtime.app)


def _route_count(path: str, method: str) -> int:
    return sum(
        1
        for route in runtime.app.routes
        if getattr(route, "path", None) == path
        and method.upper() in (getattr(route, "methods", None) or set())
    )


def test_skill_performance_rank_route_is_registered_once():
    assert _route_count("/skills/performance/rank", "GET") == 1


def test_latest_backtest_route_is_registered_once():
    assert _route_count("/backtests/runs/latest", "GET") == 1


def test_skill_backtest_status_route_is_registered_once():
    assert _route_count("/skills/{skill_id}/backtest-status", "GET") == 1


def test_execute_order_endpoint_returns_declared_response_object():
    order_before = {
        "order_id": 42,
        "trade_id": "trade-runtime-42",
        "account_id": 1,
    }
    order_after = {
        **order_before,
        "status": "executed",
    }

    with patch.object(runtime.main_module, "DATABASE_AGENT_API_KEY", "test-key"), \
            patch.object(
                runtime.db,
                "get_order_by_id",
                side_effect=[order_before, order_after],
            ), \
            patch.object(
                runtime.db,
                "_runtime_original_execute_order",
                return_value=("executed", None, 1),
            ) as execute_order:
        response = client.post(
            "/accounts/1/orders/42/execute",
            headers={
                "X-API-KEY": "test-key",
                "X-Correlation-ID": "corr-runtime-execute",
            },
        )

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "success"
    assert body["data"] == {
        "order_id": 42,
        "trade_id": "trade-runtime-42",
        "account_id": 1,
        "status": "executed",
        "reason": None,
    }
    execute_order.assert_called_once_with(42)


def test_execute_order_endpoint_rejects_account_mismatch_without_execution():
    persisted_order = {
        "order_id": 42,
        "trade_id": "trade-runtime-42",
        "account_id": 2,
    }

    with patch.object(runtime.main_module, "DATABASE_AGENT_API_KEY", "test-key"), \
            patch.object(runtime.db, "get_order_by_id", return_value=persisted_order), \
            patch.object(
                runtime.db,
                "_runtime_original_execute_order",
                return_value=("executed", None, 2),
            ) as execute_order:
        response = client.post(
            "/accounts/1/orders/42/execute",
            headers={"X-API-KEY": "test-key"},
        )

    assert response.status_code == 200
    body = response.json()
    assert body["data"]["status"] == "failed"
    assert body["data"]["reason"] == "account_mismatch"
    assert body["data"]["account_id"] == 1
    execute_order.assert_not_called()


def test_legacy_single_argument_execute_order_contract_is_preserved():
    expected = ("failed", "invalid_state", 1)
    with patch.object(
        runtime.db,
        "_runtime_original_execute_order",
        return_value=expected,
    ) as execute_order:
        result = runtime.db.execute_order(42)

    assert result == expected
    execute_order.assert_called_once_with(42)
