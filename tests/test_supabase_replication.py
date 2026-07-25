from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.core.config import Settings
from app.core.supabase_events import (
    build_supabase_event,
    install_supabase_event_capture,
)
from app.services.supabase_replication import (
    SupabaseEventClient,
    SupabaseReplicationWorker,
)
from supabase_replication_repository import (
    claim_supabase_events,
    enqueue_supabase_event,
    get_supabase_outbox_stats,
    mark_supabase_event_failed,
    mark_supabase_event_sent,
    setup_supabase_replication_outbox,
)


class SQLiteDB:
    db_type = "sqlite"
    param_style = "?"

    def __init__(self) -> None:
        self.conn = sqlite3.connect(":memory:", check_same_thread=False)
        self.conn.row_factory = sqlite3.Row

    @contextmanager
    def connection_scope(self):
        yield self.conn

    def get_cursor(self, conn=None):
        return (conn or self.conn).cursor()


def _event(correlation_id: str = "corr-supabase-1"):
    return build_supabase_event(
        method="POST",
        route="/accounts/{account_id}/orders",
        correlation_id=correlation_id,
        payload={"order_id": 42, "account_id": 1, "symbol": "AAPL"},
    )


def test_supabase_settings_require_server_credentials_when_enabled():
    settings = Settings.from_environ(
        {
            "TRADING_MODE": "PAPER",
            "DATABASE_DEV_MODE": "true",
            "SUPABASE_REPLICATION_ENABLED": "true",
            "SUPABASE_URL": "https://project.supabase.co",
            "SUPABASE_SECRET_KEY": "sb_secret_test",
        }
    )
    settings.validate()
    assert settings.supabase_replication_enabled is True
    assert settings.supabase_table == "database_agent_events"

    missing_key = Settings.from_environ(
        {
            "TRADING_MODE": "PAPER",
            "DATABASE_DEV_MODE": "true",
            "SUPABASE_REPLICATION_ENABLED": "true",
            "SUPABASE_URL": "https://project.supabase.co",
        }
    )
    with pytest.raises(ValueError, match="SUPABASE_SECRET_KEY"):
        missing_key.validate()


def test_outbox_is_idempotent_and_tracks_delivery():
    db = SQLiteDB()
    setup_supabase_replication_outbox(db)
    event = _event()

    assert enqueue_supabase_event(db, event) is True
    assert enqueue_supabase_event(db, event) is False

    claimed = claim_supabase_events(db, limit=10)
    assert len(claimed) == 1
    assert claimed[0]["event_id"] == event["event_id"]
    assert claimed[0]["event"]["payload"]["order_id"] == 42

    mark_supabase_event_sent(db, event["event_id"])
    stats = get_supabase_outbox_stats(db)
    assert stats["sent"] == 1
    assert stats["pending"] == 0


def test_outbox_retries_without_losing_event():
    db = SQLiteDB()
    setup_supabase_replication_outbox(db)
    event = _event("corr-retry")
    enqueue_supabase_event(db, event)
    claimed = claim_supabase_events(db, limit=1)

    status = mark_supabase_event_failed(
        db,
        event["event_id"],
        attempts=claimed[0]["attempts"],
        error="network unavailable",
        max_attempts=3,
    )

    assert status == "retry"
    stats = get_supabase_outbox_stats(db)
    assert stats["retry"] == 1
    assert stats["dead"] == 0


def test_write_capture_is_fail_open_and_idempotent():
    captured = []
    runtime = SimpleNamespace(
        SUPABASE_REPLICATION_ENABLED=True,
        enqueue_supabase_event=lambda event: captured.append(event) or True,
    )
    app = FastAPI()
    install_supabase_event_capture(app, runtime)

    @app.post("/accounts/{account_id}/orders")
    async def create_order(account_id: int):
        return {"status": "success", "data": {"order_id": 7, "account_id": account_id}}

    @app.get("/accounts/{account_id}/orders")
    async def list_orders(account_id: int):
        return {"status": "success", "data": []}

    client = TestClient(app)
    response = client.post(
        "/accounts/1/orders",
        headers={"X-Correlation-ID": "corr-capture"},
    )
    assert response.status_code == 200
    assert len(captured) == 1
    assert captured[0]["event_type"] == "order.create"
    assert captured[0]["correlation_id"] == "corr-capture"
    assert captured[0]["payload"]["order_id"] == 7

    second = client.get("/accounts/1/orders")
    assert second.status_code == 200
    assert len(captured) == 1


def test_rest_client_uses_secret_key_without_bearer_for_modern_keys(monkeypatch):
    seen = {}

    class Response:
        status = 201

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    def fake_urlopen(request, timeout):
        seen["headers"] = dict(request.header_items())
        seen["url"] = request.full_url
        seen["timeout"] = timeout
        return Response()

    monkeypatch.setattr(
        "app.services.supabase_replication.urlopen",
        fake_urlopen,
    )
    client = SupabaseEventClient(
        url="https://project.supabase.co",
        secret_key="sb_secret_example",
    )
    client.upsert(_event())

    normalized = {key.lower(): value for key, value in seen["headers"].items()}
    assert normalized["apikey"] == "sb_secret_example"
    assert "authorization" not in normalized
    assert "on_conflict=event_id" in seen["url"]


def test_worker_delivers_from_outbox_without_blocking_request_path(monkeypatch):
    db = SQLiteDB()
    setup_supabase_replication_outbox(db)
    event = _event("corr-worker")
    enqueue_supabase_event(db, event)

    worker = SupabaseReplicationWorker(
        db=db,
        enabled=True,
        url="https://project.supabase.co",
        secret_key="sb_secret_example",
        interval_seconds=1,
    )
    delivered = []
    monkeypatch.setattr(worker._client, "upsert", delivered.append)

    result = worker.flush_once()

    assert result == {"claimed": 1, "delivered": 1, "failed": 0}
    assert delivered[0]["event_id"] == event["event_id"]
    assert get_supabase_outbox_stats(db)["sent"] == 1
