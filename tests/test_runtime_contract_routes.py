from fastapi import FastAPI
from fastapi.testclient import TestClient

from system_contract_routes import create_system_contract_routes


REQUIRED_STANDARD_RESPONSE_FIELDS = {
    "status",
    "agent_type",
    "version",
    "schema_version",
    "timestamp",
    "correlation_id",
    "data",
    "metadata",
    "error",
    "confidence_score",
}


async def fixed_correlation_id():
    return "test-correlation-id"


def make_app(**overrides):
    app = FastAPI()
    app.include_router(
        create_system_contract_routes(
            trading_mode=overrides.get("trading_mode", "PAPER"),
            database_dev_mode=overrides.get("database_dev_mode", False),
            database_emergency_halt=overrides.get("database_emergency_halt", False),
            database_agent_api_key_configured=overrides.get("database_agent_api_key_configured", True),
            get_correlation_id_dependency=fixed_correlation_id,
        )
    )
    return app


def assert_standard_response(payload):
    assert REQUIRED_STANDARD_RESPONSE_FIELDS.issubset(payload.keys())
    assert payload["agent_type"] == "database"
    assert payload["version"] == "1.1.0"
    assert payload["schema_version"] == "1.0"
    assert payload["correlation_id"] == "test-correlation-id"


def test_version_endpoint_uses_standard_contract():
    client = TestClient(make_app())
    response = client.get("/version")

    assert response.status_code == 200
    payload = response.json()
    assert_standard_response(payload)
    assert payload["data"]["api_contract"] == "multi-agent-trading-api-contract"
    assert payload["data"]["schema_version"] == "1.0"


def test_ready_endpoint_uses_standard_contract():
    client = TestClient(make_app())
    response = client.get("/ready")

    assert response.status_code == 200
    payload = response.json()
    assert_standard_response(payload)
    assert payload["status"] == "success"
    assert payload["data"]["ready"] is True
    assert payload["error"] is None


def test_ready_endpoint_reports_live_dev_mode_violation():
    client = TestClient(make_app(trading_mode="LIVE", database_dev_mode=True))
    response = client.get("/ready")

    assert response.status_code == 200
    payload = response.json()
    assert_standard_response(payload)
    assert payload["status"] == "error"
    assert payload["data"]["ready"] is False
    assert payload["data"]["live_dev_mode_violation"] is True
    assert payload["error"]["code"] == "DATABASE_AGENT_NOT_READY"
