from __future__ import annotations

from types import SimpleNamespace

import pytest

from app.core.config import Settings
from app.routers.system import ROUTE_SIGNATURES
from app.services import database_readiness
from schema_identity_repository import SCHEMA_SHA256, SCHEMA_VERSION


class FakeDB:
    db_type = "postgres"

    def __init__(self, connected: bool = True) -> None:
        self.connected = connected

    def check_connection(self) -> bool:
        return self.connected


def _settings(**overrides) -> Settings:
    values = {
        "trading_mode": "PAPER",
        "database_dev_mode": False,
        "database_agent_api_key": "test-key",
        "database_provider": "supabase",
        "database_url_configured": True,
        "database_ssl_mode": "require",
        "database_create_if_missing": False,
        "database_cutover_guard_enabled": True,
        "database_expected_provider": "supabase",
        "database_require_schema_identity": True,
    }
    values.update(overrides)
    return Settings(**values)


def test_cutover_guard_requires_expected_provider_and_no_dev_mode():
    with pytest.raises(ValueError, match="DATABASE_EXPECTED_PROVIDER"):
        _settings(database_expected_provider=None).validate()

    with pytest.raises(ValueError, match="must match"):
        _settings(database_expected_provider="postgres").validate()

    with pytest.raises(ValueError, match="DATABASE_DEV_MODE"):
        _settings(database_dev_mode=True).validate()


def test_supabase_readiness_passes_with_tls_and_schema_identity(monkeypatch):
    monkeypatch.setattr(
        database_readiness,
        "_postgres_transport_state",
        lambda db: {"server_version_num": 170006, "tls": True},
    )
    monkeypatch.setattr(
        database_readiness,
        "get_schema_identity",
        lambda db: {
            "schema_version": SCHEMA_VERSION,
            "schema_sha256": SCHEMA_SHA256,
        },
    )

    report = database_readiness.inspect_database_readiness(FakeDB(), _settings())

    assert report["ready"] is True
    assert report["provider"] == "supabase"
    assert report["tls"] is True
    assert report["schema_identity_match"] is True
    assert report["reasons"] == []


def test_supabase_readiness_fails_closed_on_tls_or_schema_drift(monkeypatch):
    monkeypatch.setattr(
        database_readiness,
        "_postgres_transport_state",
        lambda db: {"server_version_num": 170006, "tls": False},
    )
    monkeypatch.setattr(
        database_readiness,
        "get_schema_identity",
        lambda db: {
            "schema_version": SCHEMA_VERSION,
            "schema_sha256": "0" * 64,
        },
    )

    report = database_readiness.inspect_database_readiness(FakeDB(), _settings())

    assert report["ready"] is False
    assert "tls_required" in report["reasons"]
    assert "schema_identity_mismatch" in report["reasons"]


def test_readiness_route_is_part_of_system_contract():
    assert ("/ready", "GET") in ROUTE_SIGNATURES
