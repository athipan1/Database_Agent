import os
from unittest.mock import patch

os.environ.setdefault("ALPACA_API_KEY", "test-alpaca-key")
os.environ.setdefault("ALPACA_SECRET_KEY", "test-alpaca-secret")
os.environ.setdefault("DATABASE_DEV_MODE", "true")
os.environ.setdefault("TRADING_MODE", "PAPER")

from fastapi.testclient import TestClient

import main
from models import OrderSide
from trade_plan_models import TradePlanLifecycleStatus, TradePlanRecord

client = TestClient(main.app)


def plan_record(status=TradePlanLifecycleStatus.CREATED):
    return TradePlanRecord(
        trade_plan_id="plan-api-1",
        account_id="1",
        symbol="AAPL",
        side=OrderSide.BUY,
        status=status,
        correlation_id="corr-api-1",
        source="manager-agent",
        strategy="trend_pullback",
        strategy_bucket="value_rebound",
        risk_approval_id="risk-api-1" if status == TradePlanLifecycleStatus.RISK_APPROVED else None,
        plan={"plan_id": "plan-api-1", "symbol": "AAPL"},
        lifecycle=[{"status": status.value, "timestamp": "2026-06-28T00:00:00+00:00"}],
        metadata={"test": "plan_record_endpoint"},
    )


def headers():
    return {"X-API-KEY": "test-key", "X-Correlation-ID": "corr-api-test"}


def test_create_trade_plan_endpoint():
    payload = {
        "trade_plan_id": "plan-api-1",
        "account_id": "1",
        "symbol": "aapl",
        "side": "buy",
        "status": "created",
        "correlation_id": "corr-api-1",
        "source": "manager-agent",
        "strategy": "trend_pullback",
        "strategy_bucket": "value_rebound",
        "plan": {"plan_id": "plan-api-1", "symbol": "AAPL"},
        "metadata": {"test": "plan_record_endpoint"},
    }

    with patch.object(main, "DATABASE_AGENT_API_KEY", "test-key"), \
            patch("plan_record_routes.create_plan_record", return_value=plan_record()) as create_record:
        response = client.post("/trade-plans", json=payload, headers=headers())

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "success"
    assert body["data"]["trade_plan_id"] == "plan-api-1"
    assert body["data"]["symbol"] == "AAPL"
    create_record.assert_called_once()


def test_get_trade_plan_endpoint():
    with patch.object(main, "DATABASE_AGENT_API_KEY", "test-key"), \
            patch("plan_record_routes.get_plan_record", return_value=plan_record()) as get_record:
        response = client.get("/trade-plans/plan-api-1", headers=headers())

    assert response.status_code == 200
    body = response.json()
    assert body["data"]["trade_plan_id"] == "plan-api-1"
    get_record.assert_called_once_with(main.db, "plan-api-1")


def test_get_trade_plan_endpoint_returns_404_when_missing():
    with patch.object(main, "DATABASE_AGENT_API_KEY", "test-key"), \
            patch("plan_record_routes.get_plan_record", return_value=None):
        response = client.get("/trade-plans/missing-plan", headers=headers())

    assert response.status_code == 404


def test_update_trade_plan_status_endpoint():
    payload = {
        "status": "risk_approved",
        "reason": "risk approved",
        "risk_approval_id": "risk-api-1",
        "metadata": {"risk_score": 0.25},
    }

    with patch.object(main, "DATABASE_AGENT_API_KEY", "test-key"), \
            patch("plan_record_routes.update_plan_record_status", return_value=plan_record(TradePlanLifecycleStatus.RISK_APPROVED)) as update_record:
        response = client.post("/trade-plans/plan-api-1/status", json=payload, headers=headers())

    assert response.status_code == 200
    body = response.json()
    assert body["data"]["status"] == "risk_approved"
    assert body["data"]["risk_approval_id"] == "risk-api-1"
    update_record.assert_called_once()


def collect_paths(routes):
    paths = set()
    for route in routes:
        path = getattr(route, "path", None)
        if path:
            paths.add(path)
        child_routes = getattr(route, "routes", None)
        if child_routes:
            paths.update(collect_paths(child_routes))
    return paths


def test_trade_plan_routes_are_registered():
    paths = collect_paths(main.app.routes)

    assert "/trade-plans" in paths
    assert "/trade-plans/{trade_plan_id}" in paths
    assert "/trade-plans/{trade_plan_id}/status" in paths
