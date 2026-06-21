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
        "status": "pending",
        "client_order_id": payload["trade_id"],
        "risk_approval_id": "risk-approval-1",
        "final_quantity": 3,
        "guard_plan": payload["guard_plan"],
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
    assert body["data"]["risk_approval_id"] == "risk-approval-1"
    assert body["data"]["final_quantity"] == 3
    assert body["data"]["guard_plan"] == payload["guard_plan"]

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
        guard_plan=payload["guard_plan"],
        protective_exit=None,
    )
