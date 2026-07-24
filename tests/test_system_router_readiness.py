from datetime import datetime, timezone
from types import SimpleNamespace

from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.routers.system import ROUTE_SIGNATURES, create_system_router


class DatabaseStub:
    def __init__(self, connected: bool):
        self.connected = connected

    def check_connection(self) -> bool:
        return self.connected


def _runtime(*, connected: bool, dev_mode: bool = False):
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
        db=DatabaseStub(connected),
        wrap_response=wrap_response,
        DATABASE_DEV_MODE=dev_mode,
        TRADING_MODE="PAPER",
        DATABASE_EMERGENCY_HALT=False,
    )


def _client(*, connected: bool, dev_mode: bool = False) -> TestClient:
    app = FastAPI()
    app.include_router(
        create_system_router(
            _runtime(connected=connected, dev_mode=dev_mode)
        )
    )
    return TestClient(app)


def test_system_router_declares_health_readiness_and_metrics():
    router = create_system_router(_runtime(connected=True))
    signatures = {
        (route.path, method)
        for route in router.routes
        for method in (route.methods or set())
    }

    assert signatures == ROUTE_SIGNATURES


def test_ready_returns_200_only_when_database_is_connected():
    response = _client(connected=True).get("/ready")

    assert response.status_code == 200
    assert response.json()["data"]["database_connection"] == "connected"


def test_ready_returns_503_when_database_is_disconnected_even_in_dev_mode():
    response = _client(connected=False, dev_mode=True).get("/ready")

    assert response.status_code == 503
    assert response.json()["data"]["database_connection"] == "dev_fallback"


def test_health_remains_200_and_reports_disconnected_state():
    response = _client(connected=False).get("/health")

    assert response.status_code == 200
    assert response.json()["data"]["status"] == "unhealthy"
    assert response.json()["data"]["database_connection"] == "disconnected"
