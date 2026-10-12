from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

import app.startup as startup
import scripts.apply_runtime_migrations as migrations
from schema_identity_repository import SCHEMA_NAME, SCHEMA_SHA256, SCHEMA_VERSION


class _Scheduler:
    def __init__(self):
        self.configured = None
        self.started = False
        self.stopped = False

    def configure(self, **kwargs):
        self.configured = kwargs

    def start(self):
        self.started = True

    def stop(self):
        self.stopped = True


class _MigrationDB:
    def __init__(self):
        self.setup_database_calls = 0
        self.partition_calls = 0

    def setup_database(self):
        self.setup_database_calls += 1

    def ensure_price_partitions(self):
        self.partition_calls += 1


def _identity(**overrides):
    value = {
        "schema_name": SCHEMA_NAME,
        "schema_version": SCHEMA_VERSION,
        "schema_sha256": SCHEMA_SHA256,
    }
    value.update(overrides)
    return value


def test_verify_runtime_schema_is_read_only_and_accepts_current_identity(monkeypatch):
    calls = []

    def fake_get_schema_identity(db):
        calls.append(db)
        return _identity()

    monkeypatch.setattr(startup, "get_schema_identity", fake_get_schema_identity)
    db = object()

    assert startup.verify_runtime_schema(db) == _identity()
    assert calls == [db]


def test_verify_runtime_schema_fails_closed_when_migration_is_missing(monkeypatch):
    monkeypatch.setattr(startup, "get_schema_identity", lambda db: {})

    with pytest.raises(RuntimeError, match="Run deployment migrations"):
        startup.verify_runtime_schema(object())


def test_verify_runtime_schema_fails_closed_on_version_drift(monkeypatch):
    monkeypatch.setattr(
        startup,
        "get_schema_identity",
        lambda db: _identity(schema_version="outdated"),
    )

    with pytest.raises(RuntimeError, match="schema_version"):
        startup.verify_runtime_schema(object())


def test_startup_only_verifies_schema_and_configures_non_ddl_jobs(monkeypatch):
    monkeypatch.setattr(startup, "verify_runtime_schema", lambda db: _identity())
    scheduler = _Scheduler()
    runtime = SimpleNamespace(
        db=object(),
        runtime_scheduler=scheduler,
        run_ingestion_job=lambda: None,
        log_database_stats=lambda: None,
        DATABASE_DEV_MODE=False,
    )

    asyncio.run(startup.startup_runtime(runtime))

    assert scheduler.started is True
    assert set(scheduler.configured) == {"ingestion_job", "stats_job"}
    assert "partition_job" not in scheduler.configured


def test_current_schema_skips_all_deployment_ddl(monkeypatch):
    db = _MigrationDB()
    monkeypatch.setattr(migrations, "schema_identity_matches", lambda candidate: True)

    def must_not_run(*args, **kwargs):
        raise AssertionError("DDL setup must not run for an already-current schema")

    for name in (
        "setup_history_tables",
        "setup_risk_approval_table",
        "setup_protective_order_columns",
        "setup_execution_job_table",
        "setup_fill_table",
        "setup_broker_sync_tables",
        "setup_plan_record_table",
        "setup_policy_review_table",
        "setup_profit_lifecycle_tables",
        "setup_backtest_tables",
        "setup_backtest_promotion_tables",
        "setup_research_evidence_table",
        "setup_schema_identity_table",
    ):
        monkeypatch.setattr(migrations, name, must_not_run)

    assert migrations.apply_runtime_migrations(db) is False
    assert db.setup_database_calls == 0
    assert db.partition_calls == 0


def test_deployment_migration_marks_identity_last(monkeypatch):
    db = _MigrationDB()
    events = []
    identity_checks = iter([False, True])
    monkeypatch.setattr(
        migrations,
        "schema_identity_matches",
        lambda candidate: next(identity_checks),
    )

    for name in (
        "setup_history_tables",
        "setup_risk_approval_table",
        "setup_protective_order_columns",
        "setup_execution_job_table",
        "setup_fill_table",
        "setup_broker_sync_tables",
        "setup_plan_record_table",
        "setup_policy_review_table",
        "setup_profit_lifecycle_tables",
        "setup_backtest_tables",
        "setup_backtest_promotion_tables",
        "setup_research_evidence_table",
    ):
        monkeypatch.setattr(
            migrations,
            name,
            lambda candidate, step=name: events.append(step),
        )
    monkeypatch.setattr(
        migrations,
        "setup_schema_identity_table",
        lambda candidate: events.append("schema_identity"),
    )

    assert migrations.apply_runtime_migrations(db) is True
    assert db.setup_database_calls == 1
    assert db.partition_calls == 1
    assert events[-1] == "schema_identity"


def test_live_postgres_broker_sync_never_runs_schema_ddl(monkeypatch):
    """Pre-deploy migrations own Postgres DDL; the Paper snapshot cannot rerun it."""
    from contextlib import contextmanager
    from decimal import Decimal

    import broker_sync_repository as repository

    class Cursor:
        closed = False

        def close(self):
            self.closed = True

    class Connection:
        def __init__(self):
            self.committed = False
            self.rolled_back = False

        def commit(self):
            self.committed = True

        def rollback(self):
            self.rolled_back = True

    class MigratedPostgres:
        db_type = "postgres"
        param_style = "%s"

        def __init__(self):
            self.conn = Connection()
            self.cursor = Cursor()

        @contextmanager
        def connection_scope(self):
            yield self.conn

        def get_cursor(self, conn):
            assert conn is self.conn
            return self.cursor

    db = MigratedPostgres()
    calls = []

    def reject_runtime_ddl(_db):
        raise AssertionError("Runtime snapshot must never execute migrations")

    monkeypatch.setattr(repository, "setup_broker_sync_tables", reject_runtime_ddl)
    monkeypatch.setattr(
        repository, "_ensure_account",
        lambda cursor, database, account_id, state: Decimal("90000.00"),
    )
    monkeypatch.setattr(
        repository, "_replace_positions",
        lambda cursor, database, account_id, positions: len(positions),
    )
    monkeypatch.setattr(
        repository, "_sync_open_orders",
        lambda cursor, database, account_id, orders: len(orders),
    )
    monkeypatch.setattr(
        repository, "_mark_missing_orders",
        lambda cursor, database, account_id, orders: 0,
    )
    monkeypatch.setattr(
        repository, "_insert_snapshot",
        lambda cursor, database, account_id, state: calls.append(account_id),
    )

    result = repository.sync_broker_state(
        db,
        {
            "account_id": 1,
            "broker": "ALPACA",
            "paper": True,
            "positions": [{"symbol": "AGCO", "qty": "87.716996"}],
            "open_orders": [],
        },
    )

    assert result["positions_synced"] == 1
    assert result["open_orders_synced"] == 0
    assert db.conn.committed is True
    assert db.conn.rolled_back is False
    assert db.cursor.closed is True
    assert calls == [1]
