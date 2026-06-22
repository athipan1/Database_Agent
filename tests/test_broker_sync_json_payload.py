import json

from broker_sync_repository import _payload


class FakeSQLiteDB:
    db_type = "sqlite"


class FakePostgresDB:
    db_type = "postgres"


def test_payload_returns_json_string_for_sqlite():
    value = {"cash": "-100223.4", "positions": [{"symbol": "ACGL", "qty": "2190"}]}

    payload = _payload(value, FakeSQLiteDB())

    assert isinstance(payload, str)
    assert json.loads(payload)["positions"][0]["symbol"] == "ACGL"


def test_payload_uses_adapted_json_for_postgres_when_available():
    value = {"cash": "-100223.4", "positions": [{"symbol": "ACGL", "qty": "2190"}]}

    payload = _payload(value, FakePostgresDB())

    if isinstance(payload, str):
        # psycopg2 may be absent in minimal local test environments.
        assert json.loads(payload)["positions"][0]["qty"] == "2190"
    else:
        assert hasattr(payload, "adapted")
        assert payload.adapted["positions"][0]["symbol"] == "ACGL"
