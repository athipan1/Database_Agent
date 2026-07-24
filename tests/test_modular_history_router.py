from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import Mock

from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.routers.history import ROUTE_SIGNATURES, create_history_router


def _runtime():
    async def get_correlation_id():
        return "corr-history-router"

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
        create_signal_record=Mock(),
        get_signal_records=Mock(return_value=[]),
        create_performance_record=Mock(),
        get_performance_records=Mock(return_value=[]),
    )


def test_history_router_declares_expected_route_signatures():
    runtime = _runtime()
    router = create_history_router(runtime)
    signatures = {
        (route.path, method)
        for route in router.routes
        for method in (route.methods or set())
    }

    assert signatures == ROUTE_SIGNATURES


def test_history_router_resolves_repository_function_at_request_time():
    runtime = _runtime()
    app = FastAPI()
    app.include_router(create_history_router(runtime))
    client = TestClient(app)

    record = {
        "signal_id": "signal-modular-1",
        "account_id": "diagnostic",
        "symbol": "AAPL",
        "timestamp": datetime.now(timezone.utc),
        "source_agent": "manager-agent",
        "candidate_score": 0.7,
        "technical_score": 0.8,
        "fundamental_score": 0.6,
        "final_verdict": "BUY",
        "market_regime": "BULL",
        "metadata": {"modular": True},
    }
    runtime.create_signal_record = Mock(return_value=record)

    response = client.post(
        "/history/signals",
        json={
            "signal_id": "signal-modular-1",
            "account_id": "diagnostic",
            "symbol": "AAPL",
            "candidate_score": 0.7,
            "technical_score": 0.8,
            "fundamental_score": 0.6,
            "final_verdict": "BUY",
            "market_regime": "BULL",
            "metadata": {"modular": True},
        },
    )

    assert response.status_code == 200
    assert response.json()["data"]["signal_id"] == "signal-modular-1"
    runtime.create_signal_record.assert_called_once()
    assert runtime.create_signal_record.call_args.args[0] is runtime.db
