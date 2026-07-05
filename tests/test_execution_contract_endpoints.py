import os
from unittest.mock import patch

os.environ.setdefault("ALPACA_API_KEY", "test-alpaca-key")
os.environ.setdefault("ALPACA_SECRET_KEY", "test-alpaca-secret")

from fastapi.testclient import TestClient

import main


client = TestClient(main.app)
HEADERS = {"X-API-KEY": "test-key", "X-Correlation-ID": "corr-execution-contract"}


def order_row(**overrides):
    data = {
        "order_id": 42,
        "trade_id": "trade-42",
        "account_id": 1,
        "symbol": "AAPL",
        "side": "buy",
        "order_type": "market",
        "quantity": 10,
        "price": None,
        "time_in_force": "GTC",
        "status": "pending",
        "risk_approval_id": "risk-42",
        "final_quantity": 10,
        "guard_plan": '{"symbol":"AAPL","side":"sell","quantity":10,"trigger_price":90}',
        "protective_exit": None,
        "metadata": '{"curator_signal":{"skill_id":"skill-1"}}',
        "executed_quantity": 0,
    }
    data.update(overrides)
    return data


def job_row(**overrides):
    data = {
        "job_id": 7,
        "order_id": 42,
        "trade_id": "trade-42",
        "status": "queued",
        "attempts": 0,
        "max_attempts": 3,
        "last_error": None,
    }
    data.update(overrides)
    return data


def test_get_order_by_trade_id_contract_returns_protective_metadata():
    with patch.object(main, "DATABASE_AGENT_API_KEY", "test-key"), \
            patch.object(main.db, "get_order_by_trade_id", return_value=order_row()):
        response = client.get("/orders/trade/trade-42", headers=HEADERS)

    assert response.status_code == 200
    data = response.json()["data"]
    assert data["order_id"] == 42
    assert data["guard_plan"] == {"symbol": "AAPL", "side": "sell", "quantity": 10, "trigger_price": 90}
    assert data["metadata"] == {"curator_signal": {"skill_id": "skill-1"}}


def test_get_order_by_id_contract_returns_order():
    with patch.object(main, "DATABASE_AGENT_API_KEY", "test-key"), \
            patch.object(main.db, "get_order_by_id", return_value=order_row()):
        response = client.get("/orders/42", headers=HEADERS)

    assert response.status_code == 200
    assert response.json()["data"]["trade_id"] == "trade-42"
    assert response.json()["data"]["metadata"]["curator_signal"]["skill_id"] == "skill-1"


def test_patch_order_contract_updates_status_fields():
    updates = {"status": "placed", "broker_order_id": "broker-1", "executed_quantity": 0}
    with patch.object(main, "DATABASE_AGENT_API_KEY", "test-key"), \
            patch.object(main.db, "update_order", return_value=order_row(status="placed", broker_order_id="broker-1")) as update_order:
        response = client.patch("/orders/42", json=updates, headers=HEADERS)

    assert response.status_code == 200
    update_order.assert_called_once_with(42, updates)
    assert response.json()["data"]["status"] == "placed"
    assert response.json()["data"]["broker_order_id"] == "broker-1"


def test_patch_order_contract_updates_metadata():
    updates = {"metadata": {"curator_signal": {"skill_id": "skill-2", "execution_log_id": "log-2"}}}
    with patch.object(main, "DATABASE_AGENT_API_KEY", "test-key"), \
            patch.object(main.db, "update_order", return_value=order_row(metadata=updates["metadata"])) as update_order:
        response = client.patch("/orders/42", json=updates, headers=HEADERS)

    assert response.status_code == 200
    update_order.assert_called_once_with(42, updates)
    assert response.json()["data"]["metadata"] == updates["metadata"]


def test_create_execution_job_contract():
    with patch.object(main, "DATABASE_AGENT_API_KEY", "test-key"), \
            patch.object(main, "create_execution_job", return_value=job_row()) as create_job:
        response = client.post("/execution-jobs", json={"order_id": 42, "trade_id": "trade-42"}, headers=HEADERS)

    assert response.status_code == 200
    create_job.assert_called_once_with(main.db, 42, "trade-42", 3)
    assert response.json()["data"]["status"] == "queued"


def test_get_execution_job_by_order_contract():
    with patch.object(main, "DATABASE_AGENT_API_KEY", "test-key"), \
            patch.object(main, "get_execution_job_by_order_id", return_value=job_row()):
        response = client.get("/orders/42/execution-job", headers=HEADERS)

    assert response.status_code == 200
    assert response.json()["data"]["job_id"] == 7


def test_claim_next_execution_job_contract():
    with patch.object(main, "DATABASE_AGENT_API_KEY", "test-key"), \
            patch.object(main, "claim_next_execution_job", return_value=job_row(status="running", attempts=1)):
        response = client.post("/execution-jobs/claim-next", headers=HEADERS)

    assert response.status_code == 200
    data = response.json()["data"]
    assert data["status"] == "running"
    assert data["attempts"] == 1


def test_patch_execution_job_contract():
    updates = {"status": "succeeded", "last_error": None}
    with patch.object(main, "DATABASE_AGENT_API_KEY", "test-key"), \
            patch.object(main, "update_execution_job", return_value=job_row(status="succeeded")) as update_job:
        response = client.patch("/execution-jobs/7", json=updates, headers=HEADERS)

    assert response.status_code == 200
    update_job.assert_called_once_with(main.db, "7", updates)
    assert response.json()["data"]["status"] == "succeeded"
