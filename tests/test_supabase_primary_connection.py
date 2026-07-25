from __future__ import annotations

from types import SimpleNamespace

import pytest

from app.core.config import Settings
from app.services import database_provider
from app.services.database_provider import ManagedPostgresTradingDB
from trading_db import TradingDB


class FakeCursor:
    def __init__(self) -> None:
        self.executed = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, statement):
        self.executed.append(statement)

    def fetchone(self):
        return (1,)


class FakeConnection:
    def __init__(self) -> None:
        self.cursor_instance = FakeCursor()
        self.rolled_back = False

    def cursor(self):
        return self.cursor_instance

    def rollback(self):
        self.rolled_back = True


class FakePool:
    instances = []

    def __init__(self, *, minconn, maxconn, **kwargs) -> None:
        self.minconn = minconn
        self.maxconn = maxconn
        self.kwargs = kwargs
        self.connection = FakeConnection()
        self.returned = None
        self.closed = False
        self.__class__.instances.append(self)

    def getconn(self):
        return self.connection

    def putconn(self, connection):
        self.returned = connection

    def closeall(self):
        self.closed = True


def _supabase_settings(**overrides):
    values = {
        "trading_mode": "PAPER",
        "database_dev_mode": True,
        "database_provider": "supabase",
        "database_url_configured": True,
        "database_ssl_mode": "require",
        "database_create_if_missing": False,
        "database_pool_min": 1,
        "database_pool_max": 8,
        "database_connect_timeout_seconds": 7,
    }
    values.update(overrides)
    return Settings(**values)


def test_supabase_environment_defaults_are_fail_closed():
    settings = Settings.from_environ(
        {
            "TRADING_MODE": "PAPER",
            "DATABASE_DEV_MODE": "true",
            "DATABASE_PROVIDER": "supabase",
            "DATABASE_URL": "postgresql://example.invalid/postgres",
        }
    )

    assert settings.database_provider == "supabase"
    assert settings.database_ssl_mode == "require"
    assert settings.database_create_if_missing is False
    settings.validate()


def test_supabase_requires_url_tls_and_no_database_creation():
    with pytest.raises(ValueError, match="DATABASE_URL"):
        _supabase_settings(database_url_configured=False).validate()

    with pytest.raises(ValueError, match="DATABASE_CREATE_IF_MISSING"):
        _supabase_settings(database_create_if_missing=True).validate()

    with pytest.raises(ValueError, match="require TLS"):
        _supabase_settings(database_ssl_mode="prefer").validate()


def test_managed_provider_skips_create_database_and_enforces_pool_settings(monkeypatch):
    FakePool.instances.clear()
    monkeypatch.setattr(database_provider.psycopg2.pool, "ThreadedConnectionPool", FakePool)
    monkeypatch.setattr(TradingDB, "_init_redis", lambda self: None)
    monkeypatch.setattr(
        ManagedPostgresTradingDB,
        "_ensure_database_exists",
        lambda *args, **kwargs: pytest.fail("managed provider must not create databases"),
    )

    client = ManagedPostgresTradingDB(
        _supabase_settings(),
        database_url=(
            "postgresql://postgres.project:secret@"
            "aws-0-ap-southeast-1.pooler.supabase.com:5432/postgres"
        ),
        max_retries=1,
    )

    pool = FakePool.instances[-1]
    assert client.pool is pool
    assert pool.minconn == 1
    assert pool.maxconn == 8
    assert pool.kwargs["sslmode"] == "require"
    assert pool.kwargs["connect_timeout"] == 7
    assert pool.kwargs["application_name"] == "database-agent"
    assert pool.connection.cursor_instance.executed == ["SELECT 1"]
    assert pool.connection.rolled_back is True
    assert pool.returned is pool.connection


def test_factory_preserves_local_postgres_behavior(monkeypatch):
    sentinel = SimpleNamespace(provider="local-postgres")
    monkeypatch.setattr(database_provider, "TradingDB", lambda: sentinel)

    result = database_provider.create_trading_db(
        Settings(
            trading_mode="PAPER",
            database_dev_mode=True,
            database_provider="postgres",
            database_create_if_missing=True,
        )
    )

    assert result is sentinel


def test_pool_bounds_are_validated():
    with pytest.raises(ValueError, match="DATABASE_POOL_MAX"):
        _supabase_settings(database_pool_min=10, database_pool_max=5).validate()

    with pytest.raises(ValueError, match="must not exceed 50"):
        _supabase_settings(database_pool_max=51).validate()
