import datetime

import pytest
from pydantic import ValidationError

from models import StandardAgentResponse


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


def test_standard_agent_response_has_contract_defaults():
    response = StandardAgentResponse(
        status="success",
        timestamp=datetime.datetime.now(datetime.UTC),
        data={"database_connection": "connected"},
    )

    payload = response.model_dump(mode="json")

    assert REQUIRED_STANDARD_RESPONSE_FIELDS.issubset(payload.keys())
    assert payload["agent_type"] == "database"
    assert payload["version"] == "1.1.0"
    assert payload["schema_version"] == "1.0"
    assert payload["correlation_id"] is None
    assert payload["metadata"] == {}
    assert payload["confidence_score"] is None


def test_standard_agent_response_accepts_correlation_id_and_metadata():
    response = StandardAgentResponse(
        status="success",
        timestamp=datetime.datetime.now(datetime.UTC),
        correlation_id="trace-123",
        metadata={"trading_mode": "PAPER"},
        data={"ok": True},
    )

    payload = response.model_dump(mode="json")

    assert payload["correlation_id"] == "trace-123"
    assert payload["metadata"] == {"trading_mode": "PAPER"}


def test_standard_agent_response_rejects_invalid_schema_version():
    with pytest.raises(ValidationError):
        StandardAgentResponse(
            status="success",
            timestamp=datetime.datetime.now(datetime.UTC),
            schema_version="v1",
            data={},
        )
