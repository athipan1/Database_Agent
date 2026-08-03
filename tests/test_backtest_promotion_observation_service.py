from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone

import pytest
from pydantic import ValidationError

import backtest_promotion_observation_service as service
from backtest_promotion_base import (
    PromotionDatabaseConflict,
    PromotionTerminalState,
    PromotionValidationFailed,
    StalePromotionVersion,
    setup_backtest_promotion_tables,
)
from backtest_promotion_observation_models import ObserveBacktestPromotionBody
from backtest_promotion_repository import get_backtest_promotion


NOW = datetime(2026, 8, 3, 5, 0, tzinfo=timezone.utc)


class SQLiteDB:
    db_type = "sqlite"
    param_style = "?"

    def __init__(self):
        self.conn = sqlite3.connect(":memory:")
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA foreign_keys = ON")

    @contextmanager
    def connection_scope(self):
        yield self.conn

    def get_cursor(self, conn):
        return conn.cursor()


def insert_promotion(
    db,
    *,
    promotion_id="promotion-1",
    run_id="run-1",
    state="APPROVED_FOR_PAPER",
    version=5,
    expires_at=None,
):
    setup_backtest_promotion_tables(db)
    created_at = NOW - timedelta(hours=2)
    with db.connection_scope() as conn:
        conn.execute(
            """
            INSERT INTO backtest_promotions (
                promotion_id, account_id, run_id, skill_id, strategy_id,
                symbol, timeframe, dataset_fingerprint, engine_version,
                validation_profile, state, version, evidence_version,
                created_at, updated_at, approved_for_paper_at,
                paper_observing_at, expires_at, last_observed_at,
                reason_code, reason, correlation_id, metadata
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                promotion_id,
                "1",
                run_id,
                "skill-1",
                "strategy-1",
                "AAPL",
                "1d",
                "a" * 64,
                "engine-1",
                "nested_walk_forward_v2",
                state,
                version,
                1,
                created_at.isoformat(),
                created_at.isoformat(),
                created_at.isoformat(),
                created_at.isoformat() if state == "PAPER_OBSERVING" else None,
                (expires_at or NOW + timedelta(days=1)).isoformat(),
                created_at.isoformat() if state == "PAPER_OBSERVING" else None,
                "seed",
                "seed promotion",
                "seed-correlation",
                "{}",
            ),
        )
        conn.commit()


def body(**updates):
    value = {
        "expected_state": "APPROVED_FOR_PAPER",
        "expected_version": 5,
        "observation_key": "observation-001",
        "observed_at": NOW,
        "paper_drawdown_pct": 0.02,
        "reconciliation_ok": True,
        "duplicate_order_count": 0,
        "broker_order_count": 1,
        "database_order_count": 1,
        "filled_order_count": 1,
        "strategy_drift": False,
        "emergency_halt": False,
        "notes": ["paper reconciliation healthy"],
        "correlation_id": "corr-1",
        "metadata": {"source": "test"},
    }
    value.update(updates)
    return ObserveBacktestPromotionBody.model_validate(value)


def update_promotion(
    db,
    promotion_id,
    *,
    state,
    version,
    correlation_id,
    metadata=None,
):
    timestamp = NOW + timedelta(seconds=1)
    with db.connection_scope() as conn:
        conn.execute(
            """
            UPDATE backtest_promotions
            SET state = ?, version = ?, updated_at = ?,
                paper_observing_at = CASE
                    WHEN ? = 'PAPER_OBSERVING' THEN ?
                    ELSE paper_observing_at
                END,
                revoked_at = CASE
                    WHEN ? = 'REVOKED' THEN ?
                    ELSE revoked_at
                END,
                reason_code = ?, reason = ?, correlation_id = ?, metadata = ?
            WHERE promotion_id = ?
            """,
            (
                state,
                version,
                timestamp.isoformat(),
                state,
                timestamp.isoformat(),
                state,
                timestamp.isoformat(),
                f"to_{state.lower()}",
                f"transitioned to {state}",
                correlation_id,
                json.dumps(metadata or {}, sort_keys=True),
                promotion_id,
            ),
        )
        conn.commit()
    return get_backtest_promotion(db, promotion_id)


@pytest.fixture
def db():
    database = SQLiteDB()
    yield database
    database.conn.close()


@pytest.fixture
def fake_transitions(monkeypatch):
    calls = {"transition": []}

    def transition(db, promotion_id, transition_body, correlation_id):
        calls["transition"].append(transition_body)
        return update_promotion(
            db,
            promotion_id,
            state=transition_body.next_state,
            version=transition_body.expected_version + 1,
            correlation_id=correlation_id,
            metadata=transition_body.metadata,
        )

    monkeypatch.setattr(service, "transition_backtest_promotion", transition)
    return calls


def test_first_healthy_observation_starts_observing_and_replays(
    db,
    fake_transitions,
):
    insert_promotion(db)
    request = body()

    first = service.observe_backtest_promotion(
        db,
        "promotion-1",
        request,
        "corr-1",
    )
    replay = service.observe_backtest_promotion(
        db,
        "promotion-1",
        request,
        "corr-1",
    )

    assert first.action == "START_OBSERVING"
    assert first.from_state == "APPROVED_FOR_PAPER"
    assert first.to_state == "PAPER_OBSERVING"
    assert first.from_version == 5
    assert first.to_version == 6
    assert first.idempotent_replay is False
    assert replay.observation_id == first.observation_id
    assert replay.idempotent_replay is True
    assert len(fake_transitions["transition"]) == 1
    assert len(
        service.list_backtest_promotion_observations(db, "promotion-1")
    ) == 1


def test_transition_replay_rejects_different_observation_identity(
    db,
    fake_transitions,
):
    insert_promotion(db)
    service.observe_backtest_promotion(
        db,
        "promotion-1",
        body(observation_key="first-observation"),
        "corr-1",
    )

    with pytest.raises(PromotionDatabaseConflict, match="different observation"):
        service.observe_backtest_promotion(
            db,
            "promotion-1",
            body(observation_key="second-observation"),
            "corr-2",
        )

    assert len(fake_transitions["transition"]) == 1
    assert len(
        service.list_backtest_promotion_observations(db, "promotion-1")
    ) == 1


def test_healthy_observing_heartbeat_updates_version_and_last_observed(db):
    insert_promotion(db, state="PAPER_OBSERVING", version=6)
    request = body(
        expected_state="PAPER_OBSERVING",
        expected_version=6,
        observation_key="heartbeat-001",
    )

    result = service.observe_backtest_promotion(
        db,
        "promotion-1",
        request,
        "corr-1",
    )
    current = get_backtest_promotion(db, "promotion-1")

    assert result.action == "HEARTBEAT"
    assert result.to_state == "PAPER_OBSERVING"
    assert result.to_version == 7
    assert current.version == 7
    assert current.last_observed_at == NOW
    assert current.metadata["last_observation_key"] == "heartbeat-001"


@pytest.mark.parametrize(
    ("updates", "reason_code"),
    [
        ({"emergency_halt": True}, "emergency_halt"),
        ({"duplicate_order_count": 1}, "duplicate_order_detected"),
        ({"reconciliation_ok": False}, "broker_reconciliation_failed"),
        (
            {"broker_order_count": 2, "database_order_count": 1},
            "broker_reconciliation_failed",
        ),
        (
            {"filled_order_count": 2, "broker_order_count": 1},
            "broker_reconciliation_failed",
        ),
        ({"strategy_drift": True}, "strategy_drift"),
        ({"paper_drawdown_pct": 0.11}, "paper_drawdown_exceeded"),
    ],
)
def test_reconciliation_failures_revoke_immediately(
    db,
    fake_transitions,
    updates,
    reason_code,
):
    insert_promotion(db)
    result = service.observe_backtest_promotion(
        db,
        "promotion-1",
        body(**updates),
        "corr-1",
    )

    transition = fake_transitions["transition"][0]
    assert result.action == "REVOKE"
    assert result.to_state == "REVOKED"
    assert result.reason_code == reason_code
    assert transition.next_state == "REVOKED"
    assert transition.reason_code == reason_code
    assert transition.metadata["revocation"] is True


def test_expired_observation_transitions_to_expired(db, fake_transitions):
    insert_promotion(db, expires_at=NOW - timedelta(seconds=1))
    result = service.observe_backtest_promotion(
        db,
        "promotion-1",
        body(),
        "corr-1",
    )

    assert result.action == "EXPIRE"
    assert result.to_state == "EXPIRED"
    assert fake_transitions["transition"][0].next_state == "EXPIRED"


def test_stale_terminal_and_future_observations_fail_closed(
    db,
    fake_transitions,
):
    insert_promotion(db)
    with pytest.raises(StalePromotionVersion):
        service.observe_backtest_promotion(
            db,
            "promotion-1",
            body(expected_version=4),
            "corr-1",
        )

    with pytest.raises(PromotionValidationFailed, match="future"):
        service.observe_backtest_promotion(
            db,
            "promotion-1",
            body(
                observation_key="future-001",
                observed_at=datetime.now(timezone.utc) + timedelta(minutes=10),
            ),
            "corr-1",
        )

    update_promotion(
        db,
        "promotion-1",
        state="REVOKED",
        version=6,
        correlation_id="corr-2",
    )
    with pytest.raises(PromotionTerminalState):
        service.observe_backtest_promotion(
            db,
            "promotion-1",
            body(observation_key="terminal-001", expected_version=6),
            "corr-2",
        )


def test_observation_key_and_timestamp_contracts_are_strict():
    with pytest.raises(ValidationError, match="unsupported characters"):
        body(observation_key="bad key")
    with pytest.raises(ValidationError, match="timezone"):
        body(observed_at=datetime(2026, 8, 3, 5, 0))
