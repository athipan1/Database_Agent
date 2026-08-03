from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone

import pytest
from pydantic import ValidationError

import backtest_promotion_observation_service as service
from backtest_promotion_base import setup_backtest_promotion_tables
from backtest_promotion_observation_models import ObserveBacktestPromotionBody
from backtest_promotion_repository import get_backtest_promotion


NOW = datetime.now(timezone.utc).replace(microsecond=0)


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


def _insert_promotion(db: SQLiteDB) -> None:
    setup_backtest_promotion_tables(db)
    created_at = NOW - timedelta(hours=1)
    with db.connection_scope() as conn:
        conn.execute(
            """
            INSERT INTO backtest_promotions (
                promotion_id, account_id, run_id, skill_id, strategy_id,
                symbol, timeframe, dataset_fingerprint, engine_version,
                validation_profile, state, version, evidence_version,
                created_at, updated_at, approved_for_paper_at, expires_at,
                reason_code, reason, correlation_id, metadata
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "promotion-recovery",
                "account-1",
                "run-recovery",
                "skill-1",
                "strategy-1",
                "AAPL",
                "1d",
                "a" * 64,
                "engine-1",
                "nested_walk_forward_v2",
                "APPROVED_FOR_PAPER",
                5,
                1,
                created_at.isoformat(),
                created_at.isoformat(),
                created_at.isoformat(),
                (NOW + timedelta(days=1)).isoformat(),
                "seed",
                "seed promotion",
                "seed-correlation",
                "{}",
            ),
        )
        conn.commit()


def _body(**updates) -> ObserveBacktestPromotionBody:
    payload = {
        "expected_state": "APPROVED_FOR_PAPER",
        "expected_version": 5,
        "observation_key": "observation-recovery-1",
        "observed_at": NOW,
        "paper_drawdown_pct": 0.01,
        "reconciliation_ok": True,
        "duplicate_order_count": 0,
        "broker_order_count": 1,
        "database_order_count": 1,
        "filled_order_count": 1,
        "strategy_drift": False,
        "emergency_halt": False,
        "notes": ["healthy"],
        "correlation_id": "corr-recovery",
        "metadata": {"source": "recovery-test"},
    }
    payload.update(updates)
    return ObserveBacktestPromotionBody.model_validate(payload)


def test_transition_then_ledger_failure_is_recovered_on_retry(monkeypatch):
    db = SQLiteDB()
    _insert_promotion(db)
    transition_calls = 0
    persisted_transition = None

    def transition(database, promotion_id, transition_body, correlation_id):
        nonlocal transition_calls, persisted_transition
        transition_calls += 1
        if persisted_transition is not None:
            return persisted_transition.model_copy(update={"idempotent_replay": True})
        current = get_backtest_promotion(database, promotion_id)
        updated = current.model_copy(
            update={
                "state": transition_body.next_state,
                "version": transition_body.expected_version + 1,
                "updated_at": NOW,
                "paper_observing_at": NOW,
                "last_observed_at": NOW,
                "correlation_id": correlation_id,
                "idempotent_replay": False,
            }
        )
        with database.connection_scope() as conn:
            conn.execute(
                """
                UPDATE backtest_promotions
                SET state = ?, version = ?, updated_at = ?,
                    paper_observing_at = ?, last_observed_at = ?,
                    correlation_id = ?
                WHERE promotion_id = ? AND version = ?
                """,
                (
                    updated.state,
                    updated.version,
                    NOW.isoformat(),
                    NOW.isoformat(),
                    NOW.isoformat(),
                    correlation_id,
                    promotion_id,
                    transition_body.expected_version,
                ),
            )
            conn.commit()
        persisted_transition = updated
        return updated

    original_persist = service._persist_observation
    persist_calls = 0

    def fail_once(database, record):
        nonlocal persist_calls
        persist_calls += 1
        if persist_calls == 1:
            raise RuntimeError("simulated crash after transition commit")
        return original_persist(database, record)

    monkeypatch.setattr(service, "transition_backtest_promotion", transition)
    monkeypatch.setattr(service, "_persist_observation", fail_once)

    request = _body()
    with pytest.raises(RuntimeError, match="simulated crash"):
        service.observe_backtest_promotion(
            db,
            "promotion-recovery",
            request,
            "corr-recovery",
        )

    recovered = service.observe_backtest_promotion(
        db,
        "promotion-recovery",
        request,
        "corr-recovery",
    )
    replay = service.observe_backtest_promotion(
        db,
        "promotion-recovery",
        request,
        "corr-recovery",
    )

    assert transition_calls == 2
    assert recovered.to_state == "PAPER_OBSERVING"
    assert recovered.to_version == 6
    assert recovered.idempotent_replay is True
    assert replay.observation_id == recovered.observation_id
    assert replay.idempotent_replay is True
    assert len(
        service.list_backtest_promotion_observations(
            db,
            "promotion-recovery",
        )
    ) == 1
    db.conn.close()


def test_observation_contract_rejects_invalid_values():
    with pytest.raises(ValidationError, match="unsupported characters"):
        _body(observation_key="bad key")
    with pytest.raises(ValidationError, match="timezone"):
        _body(observed_at=datetime(2026, 8, 3, 5, 0))
    with pytest.raises(ValidationError):
        _body(paper_drawdown_pct=float("nan"))
    with pytest.raises(ValidationError):
        ObserveBacktestPromotionBody.model_validate(
            {
                **_body().model_dump(),
                "unknown_field": "blocked",
            }
        )
