import sqlite3
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from decimal import Decimal

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from profit_lifecycle_models import (
    ReserveProfitDecisionBody,
    TransitionProfitDecisionBody,
)
from profit_lifecycle_repository import (
    InvalidProfitDecisionTransition,
    StalePositionVersion,
    get_profit_lifecycle,
    reserve_profit_decision,
    setup_profit_lifecycle_tables,
    transition_profit_decision,
)
from profit_lifecycle_routes import create_profit_lifecycle_routes


class LifecycleDB:
    db_type = "sqlite"
    param_style = "?"

    def __init__(self):
        self.conn = sqlite3.connect(":memory:", check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.executescript(
            """
            CREATE TABLE accounts (
                account_id INTEGER PRIMARY KEY,
                account_name TEXT NOT NULL UNIQUE,
                cash_balance TEXT NOT NULL
            );
            CREATE TABLE positions (
                position_id INTEGER PRIMARY KEY AUTOINCREMENT,
                account_id INTEGER NOT NULL,
                symbol TEXT NOT NULL,
                quantity BIGINT NOT NULL,
                average_cost TEXT NOT NULL,
                highest_price_since_entry TEXT,
                UNIQUE (account_id, symbol)
            );
            INSERT INTO accounts VALUES (1, 'paper', '100000');
            INSERT INTO positions (
                account_id, symbol, quantity, average_cost,
                highest_price_since_entry
            ) VALUES (1, 'ACGL', 10, '100', '120');
            """
        )
        self.conn.commit()

    @contextmanager
    def connection_scope(self):
        yield self.conn

    def get_cursor(self, conn):
        return conn.cursor()

    def _add_column_if_not_exists(self, cursor, table, column, definition):
        try:
            cursor.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")
        except sqlite3.OperationalError as exc:
            if "duplicate column name" not in str(exc).lower():
                raise


@pytest.fixture()
def db():
    database = LifecycleDB()
    setup_profit_lifecycle_tables(database)
    return database


def _reserve_body(**overrides):
    data = {
        "position_id": "account-1:position-1",
        "position_version": 1,
        "decision_id": "profit:account-1:position-1:ACGL:v1:tp1",
        "decision_type": "first_take_profit",
        "proposed_quantity": "3",
        "next_lifecycle_state": {"first_target_executed": True},
        "metadata": {"advisory_only": True},
    }
    data.update(overrides)
    return ReserveProfitDecisionBody.model_validate(data)


def _transition(expected_status, status, **overrides):
    return TransitionProfitDecisionBody.model_validate(
        {
            "expected_status": expected_status,
            "status": status,
            **overrides,
        }
    )


def test_migration_backfills_position_lifecycle(db):
    lifecycle = get_profit_lifecycle(db, 1, "account-1:position-1")

    assert lifecycle["position_version"] == 1
    assert lifecycle["remaining_quantity"] == Decimal("10")
    assert lifecycle["first_target_executed"] is False
    assert lifecycle["second_target_executed"] is False
    assert lifecycle["total_exited_quantity"] == Decimal("0")


def test_duplicate_reservation_returns_existing_decision(db):
    body = _reserve_body()

    first = reserve_profit_decision(db, 1, body, "corr-1")
    duplicate = reserve_profit_decision(db, 1, body, "corr-2")

    assert first["status"] == "PROPOSED"
    assert first["duplicate"] is False
    assert duplicate["decision_id"] == first["decision_id"]
    assert duplicate["duplicate"] is True
    count = db.conn.execute("SELECT COUNT(*) FROM profit_decisions").fetchone()[0]
    assert count == 1


def test_concurrent_duplicate_requests_create_one_record(db):
    body = _reserve_body()

    with ThreadPoolExecutor(max_workers=6) as executor:
        results = list(
            executor.map(
                lambda index: reserve_profit_decision(
                    db, 1, body, f"corr-{index}"
                ),
                range(12),
            )
        )

    assert sum(result["duplicate"] is False for result in results) == 1
    assert len({result["decision_id"] for result in results}) == 1
    count = db.conn.execute("SELECT COUNT(*) FROM profit_decisions").fetchone()[0]
    assert count == 1


def test_stale_version_is_rejected_before_reservation(db):
    with pytest.raises(StalePositionVersion, match="stale position version"):
        reserve_profit_decision(
            db,
            1,
            _reserve_body(position_version=2),
            "corr-stale",
        )

    assert db.conn.execute("SELECT COUNT(*) FROM profit_decisions").fetchone()[0] == 0


def test_target_is_not_marked_before_confirmed_execution(db):
    body = _reserve_body()
    reserve_profit_decision(db, 1, body, "corr-1")

    transition_profit_decision(
        db,
        1,
        body.decision_id,
        _transition("PROPOSED", "RISK_APPROVED"),
        "corr-1",
    )
    transition_profit_decision(
        db,
        1,
        body.decision_id,
        _transition("RISK_APPROVED", "EXECUTION_PENDING"),
        "corr-1",
    )

    lifecycle = get_profit_lifecycle(db, 1, body.position_id)
    assert lifecycle["first_target_executed"] is False
    assert lifecycle["position_version"] == 1


def test_confirmed_fill_updates_lifecycle_once(db):
    body = _reserve_body()
    reserve_profit_decision(db, 1, body, "corr-1")
    transition_profit_decision(
        db,
        1,
        body.decision_id,
        _transition("PROPOSED", "RISK_APPROVED"),
        "corr-1",
    )
    transition_profit_decision(
        db,
        1,
        body.decision_id,
        _transition("RISK_APPROVED", "EXECUTION_PENDING"),
        "corr-1",
    )
    executed = transition_profit_decision(
        db,
        1,
        body.decision_id,
        _transition(
            "EXECUTION_PENDING", "EXECUTED", executed_quantity="3"
        ),
        "corr-1",
    )
    duplicate = transition_profit_decision(
        db,
        1,
        body.decision_id,
        _transition(
            "EXECUTION_PENDING", "EXECUTED", executed_quantity="3"
        ),
        "corr-retry",
    )

    lifecycle = get_profit_lifecycle(db, 1, body.position_id)
    assert executed["status"] == "EXECUTED"
    assert duplicate["duplicate"] is True
    assert lifecycle["first_target_executed"] is True
    assert lifecycle["position_version"] == 2
    assert lifecycle["total_exited_quantity"] == Decimal("3")


def test_partial_fill_stays_pending_and_does_not_advance_target(db):
    body = _reserve_body()
    reserve_profit_decision(db, 1, body, "corr-1")
    transition_profit_decision(
        db, 1, body.decision_id, _transition("PROPOSED", "RISK_APPROVED"), "corr-1"
    )
    transition_profit_decision(
        db,
        1,
        body.decision_id,
        _transition("RISK_APPROVED", "EXECUTION_PENDING"),
        "corr-1",
    )

    partial = transition_profit_decision(
        db,
        1,
        body.decision_id,
        _transition(
            "EXECUTION_PENDING", "EXECUTION_PENDING", executed_quantity="1"
        ),
        "corr-partial",
    )

    lifecycle = get_profit_lifecycle(db, 1, body.position_id)
    assert partial["status"] == "EXECUTION_PENDING"
    assert partial["executed_quantity"] == Decimal("1")
    assert partial["metadata"]["partial_fill_recorded"] is True
    assert lifecycle["first_target_executed"] is False
    assert lifecycle["position_version"] == 1


def test_full_exit_can_finalize_after_broker_removed_position(db):
    body = _reserve_body(
        decision_id="profit:account-1:position-1:ACGL:v1:hard-stop",
        decision_type="hard_stop_exit",
        proposed_quantity="10",
        next_lifecycle_state={},
    )
    reserve_profit_decision(db, 1, body, "corr-exit")
    transition_profit_decision(
        db, 1, body.decision_id, _transition("PROPOSED", "RISK_APPROVED"), "corr-exit"
    )
    transition_profit_decision(
        db,
        1,
        body.decision_id,
        _transition("RISK_APPROVED", "EXECUTION_PENDING"),
        "corr-exit",
    )
    db.conn.execute("DELETE FROM positions WHERE position_id = 1")
    db.conn.commit()

    executed = transition_profit_decision(
        db,
        1,
        body.decision_id,
        _transition(
            "EXECUTION_PENDING", "EXECUTED", executed_quantity="10"
        ),
        "corr-exit",
    )

    assert executed["status"] == "EXECUTED"
    assert executed["metadata"]["position_closed_before_executed_transition"] is True


def test_invalid_state_transition_is_rejected(db):
    body = _reserve_body()
    reserve_profit_decision(db, 1, body, "corr-1")

    with pytest.raises(InvalidProfitDecisionTransition, match="invalid transition"):
        transition_profit_decision(
            db,
            1,
            body.decision_id,
            _transition("PROPOSED", "EXECUTED", executed_quantity="3"),
            "corr-1",
        )


def test_migration_files_define_upgrade_and_downgrade():
    from pathlib import Path

    root = Path(__file__).resolve().parents[1]
    upgrade = (root / "migrations/002_profit_lifecycle.up.sql").read_text()
    downgrade = (root / "migrations/002_profit_lifecycle.down.sql").read_text()

    assert "UNIQUE (account_id, position_id, decision_id)" in upgrade
    assert "position_version" in upgrade
    assert "DROP TABLE IF EXISTS profit_decisions" in downgrade
    assert "DROP COLUMN IF EXISTS position_version" in downgrade


def test_authenticated_lifecycle_api_reserves_and_reads_decision(db):
    app = FastAPI()

    def api_key(value):
        if value != "database-test-key":
            raise AssertionError("unexpected API key")
        return value

    async def correlation_id():
        return "corr-api"

    app.include_router(
        create_profit_lifecycle_routes(db, api_key, correlation_id)
    )
    client = TestClient(app)
    headers = {"X-API-KEY": "database-test-key"}

    lifecycle = client.get(
        "/accounts/1/profit-lifecycles/account-1:position-1",
        headers=headers,
    )
    reserved = client.post(
        "/accounts/1/profit-decisions/reserve",
        headers=headers,
        json=_reserve_body().model_dump(mode="json"),
    )
    decision_id = reserved.json()["data"]["decision_id"]
    fetched = client.get(
        f"/accounts/1/profit-decisions/{decision_id}",
        headers=headers,
    )

    assert lifecycle.status_code == 200
    assert lifecycle.json()["data"]["position_version"] == 1
    assert reserved.status_code == 200
    assert reserved.json()["schema_version"] == "profit-lifecycle.v1"
    assert reserved.json()["correlation_id"] == "corr-api"
    assert fetched.status_code == 200
    assert fetched.json()["data"]["status"] == "PROPOSED"


def test_lifecycle_api_returns_conflict_for_stale_version(db):
    app = FastAPI()

    def api_key(value):
        return value

    async def correlation_id():
        return "corr-stale"

    app.include_router(
        create_profit_lifecycle_routes(db, api_key, correlation_id)
    )
    response = TestClient(app).post(
        "/accounts/1/profit-decisions/reserve",
        headers={"X-API-KEY": "key"},
        json=_reserve_body(position_version=9).model_dump(mode="json"),
    )

    assert response.status_code == 409
    assert "stale position version" in response.json()["detail"]
