import os
from decimal import Decimal
from unittest.mock import patch

os.environ.setdefault("ALPACA_API_KEY", "test-alpaca-key")
os.environ.setdefault("ALPACA_SECRET_KEY", "test-alpaca-secret")

from fastapi.testclient import TestClient

import main


client = TestClient(main.app)


def test_create_order_endpoint_maps_body_to_trading_db_contract():
    payload = {
        "trade_id": "trade-api-contract-1",
        "account_id": 1,
        "symbol": "AAPL",
        "side": "buy",
        "order_type": "market",
        "quantity": 3,
        "price": "150.25",
        "time_in_force": "GTC",
        "risk_approval_id": "risk-approval-1",
        "final_quantity": 3,
        "guard_plan": {
            "symbol": "AAPL",
            "side": "sell",
            "quantity": 3,
            "trigger_price": 140,
            "time_in_force": "GTC",
        },
    }
    expected_guard_plan = {
        **payload["guard_plan"],
        "strategy_bucket": "unassigned",
    }
    persisted_order = {
        "order_id": 42,
        "trade_id": payload["trade_id"],
        "account_id": 1,
        "symbol": "AAPL",
        "side": "buy",
        "order_type": "market",
        "quantity": 3,
        "price": Decimal("150.25"),
        "time_in_force": "GTC",
        "strategy_bucket": "unassigned",
        "status": "pending",
        "client_order_id": payload["trade_id"],
        "risk_approval_id": "risk-approval-1",
        "final_quantity": 3,
        "guard_plan": expected_guard_plan,
        "protective_exit": None,
    }

    with patch.object(main, "DATABASE_AGENT_API_KEY", "test-key"), \
            patch.object(main, "setup_protective_order_columns") as setup_columns, \
            patch.object(main, "persist_protective_order_metadata") as persist_metadata, \
            patch.object(main.db, "create_order", return_value=42) as create_order, \
            patch.object(main.db, "get_order_by_id", return_value=persisted_order):
        response = client.post(
            "/accounts/1/orders",
            json=payload,
            headers={"X-API-KEY": "test-key", "X-Correlation-ID": "corr-api-contract"},
        )

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "success"
    assert body["data"]["order_id"] == 42
    assert body["data"]["trade_id"] == payload["trade_id"]
    assert body["data"]["status"] == "pending"
    assert body["data"]["strategy_bucket"] == "unassigned"
    assert body["data"]["risk_approval_id"] == "risk-approval-1"
    assert body["data"]["final_quantity"] == 3
    assert body["data"]["guard_plan"] == expected_guard_plan

    setup_columns.assert_called_once_with(main.db)
    create_order.assert_called_once_with(
        account_id="1",
        trade_id="trade-api-contract-1",
        symbol="AAPL",
        side="buy",
        order_type="market",
        quantity=3,
        price=Decimal("150.25"),
        time_in_force="GTC",
        correlation_id="corr-api-contract",
    )
    persist_metadata.assert_called_once_with(
        main.db,
        42,
        risk_approval_id="risk-approval-1",
        final_quantity=3,
        guard_plan=expected_guard_plan,
        protective_exit=None,
    )


def test_create_order_endpoint_preserves_strategy_bucket_for_value_rebound_orders():
    payload = {
        "trade_id": "trade-cinf-value-rebound",
        "account_id": 1,
        "symbol": "CINF",
        "side": "buy",
        "order_type": "market",
        "quantity": 5,
        "price": "125.50",
        "time_in_force": "GTC",
        "strategy_bucket": "value_rebound",
        "risk_approval_id": "risk-cinf-value-rebound",
        "final_quantity": 5,
        "guard_plan": {
            "symbol": "CINF",
            "side": "sell",
            "quantity": 5,
            "trigger_price": 118,
            "time_in_force": "GTC",
        },
    }
    expected_guard_plan = {
        **payload["guard_plan"],
        "strategy_bucket": "value_rebound",
    }
    persisted_order = {
        "order_id": 43,
        "trade_id": payload["trade_id"],
        "account_id": 1,
        "symbol": "CINF",
        "side": "buy",
        "order_type": "market",
        "quantity": 5,
        "price": Decimal("125.50"),
        "time_in_force": "GTC",
        "strategy_bucket": "value_rebound",
        "status": "pending",
        "client_order_id": payload["trade_id"],
        "risk_approval_id": "risk-cinf-value-rebound",
        "final_quantity": 5,
        "guard_plan": expected_guard_plan,
        "protective_exit": None,
    }

    with patch.object(main, "DATABASE_AGENT_API_KEY", "test-key"), \
            patch.object(main, "setup_protective_order_columns"), \
            patch.object(main, "persist_protective_order_metadata") as persist_metadata, \
            patch.object(main.db, "create_order", return_value=43), \
            patch.object(main.db, "get_order_by_id", return_value=persisted_order):
        response = client.post(
            "/accounts/1/orders",
            json=payload,
            headers={"X-API-KEY": "test-key", "X-Correlation-ID": "corr-cinf-bucket"},
        )

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "success"
    assert body["data"]["symbol"] == "CINF"
    assert body["data"]["strategy_bucket"] == "value_rebound"
    assert body["data"]["guard_plan"]["strategy_bucket"] == "value_rebound"

    persist_metadata.assert_called_once_with(
        main.db,
        43,
        risk_approval_id="risk-cinf-value-rebound",
        final_quantity=5,
        guard_plan=expected_guard_plan,
        protective_exit=None,
    )
