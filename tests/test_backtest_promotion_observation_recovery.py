from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone

import pytest
from pydantic import ValidationError

import backtest_promotion_observation_service as service
from backtest_promotion_base import (
    StalePromotionVersion,
    setup_backtest_promotion_tables,
)
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


def _insert_promotion(
    db: SQLiteDB,
    *,
    state: str = "APPROVED_FOR_PAPER",
    version: int = 5,
) -> None:
    setup_backtest_promotion_tables(db)
    created_at = NOW - timedelta(hours=1)
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
                state,
                version,
                1,
                created_at.isoformat(),
                created_at.isoformat(),
                created_at.isoformat(),
                created_at.isoformat() if state == "PAPER_OBSERVING" else None,
                (NOW + timedelta(days=1)).isoformat(),
                created_at.isoformat() if state == "PAPER_OBSERVING" else None,
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


def _record(db: SQLiteDB):
    current = get_backtest_promotion(db, "promotion-recovery")
    promoted = current.model_copy(
        update={
            "state": "PAPER_OBSERVING",
            "version": current.version + 1,
            "paper_observing_at": NOW,
            "last_observed_at": NOW,
        }
    )
    return service._record(
        observation_id=service.deterministic_observation_id(
            current.promotion_id,
            "direct-persist",
        ),
        promotion_id=current.promotion_id,
        body=_body(observation_key="direct-persist"),
        action="START_OBSERVING",
        reason_code="paper_observation_started",
        from_state="APPROVED_FOR_PAPER",
        from_version=current.version,
        promotion=promoted.model_dump(mode="json"),
        correlation_id="corr-direct",
    )


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
        metadata = {**current.metadata, **transition_body.metadata}
        updated = current.model_copy(
            update={
                "state": transition_body.next_state,
                "version": transition_body.expected_version + 1,
                "updated_at": NOW,
                "paper_observing_at": NOW,
                "last_observed_at": NOW,
                "correlation_id": correlation_id,
                "metadata": metadata,
                "idempotent_replay": False,
            }
        )
        with database.connection_scope() as conn:
            conn.execute(
                """
                UPDATE backtest_promotions
                SET state = ?, version = ?, updated_at = ?,
                    paper_observing_at = ?, last_observed_at = ?,
                    correlation_id = ?, metadata = ?
                WHERE promotion_id = ? AND version = ?
                """,
                (
                    updated.state,
                    updated.version,
                    NOW.isoformat(),
                    NOW.isoformat(),
                    NOW.isoformat(),
                    correlation_id,
                    json.dumps(metadata, sort_keys=True),
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


def test_persisting_same_record_returns_single_replay():
    db = SQLiteDB()
    _insert_promotion(db)
    service.setup_backtest_promotion_observation_tables(db)
    record = _record(db)

    first = service._persist_observation(db, record)
    replay = service._persist_observation(db, record)

    assert first.idempotent_replay is False
    assert replay.observation_id == first.observation_id
    assert replay.idempotent_replay is True
    assert len(
        service.list_backtest_promotion_observations(
            db,
            "promotion-recovery",
        )
    ) == 1
    db.conn.close()


def test_empty_result_snapshot_is_ignored():
    db = SQLiteDB()
    _insert_promotion(db)
    service.setup_backtest_promotion_observation_tables(db)
    record = _record(db)
    service._persist_observation(db, record)
    with db.connection_scope() as conn:
        conn.execute(
            """
            UPDATE backtest_promotion_observations
            SET result_snapshot = '{}'
            WHERE observation_id = ?
            """,
            (record.observation_id,),
        )
        conn.commit()

    assert (
        service.get_backtest_promotion_observation(db, record.observation_id)
        is None
    )
    assert service.list_backtest_promotion_observations(
        db,
        "promotion-recovery",
    ) == []
    db.conn.close()


def test_heartbeat_replays_existing_ledger_without_version_increment():
    db = SQLiteDB()
    _insert_promotion(db, state="PAPER_OBSERVING", version=6)
    request = _body(
        expected_state="PAPER_OBSERVING",
        expected_version=6,
        observation_key="heartbeat-replay",
    )
    promotion = get_backtest_promotion(db, "promotion-recovery")
    first = service.observe_backtest_promotion(
        db,
        "promotion-recovery",
        request,
        "corr-heartbeat",
    )
    replay = service._heartbeat(
        db,
        promotion,
        request,
        first.observation_id,
        "corr-heartbeat-retry",
    )

    assert first.to_version == 7
    assert replay.idempotent_replay is True
    assert replay.to_version == 7
    assert get_backtest_promotion(db, "promotion-recovery").version == 7
    db.conn.close()


def test_heartbeat_lost_compare_and_swap_fails_stale():
    db = SQLiteDB()
    _insert_promotion(db, state="PAPER_OBSERVING", version=6)
    service.setup_backtest_promotion_observation_tables(db)
    stale = get_backtest_promotion(db, "promotion-recovery")
    with db.connection_scope() as conn:
        conn.execute(
            "UPDATE backtest_promotions SET version = 7 WHERE promotion_id = ?",
            ("promotion-recovery",),
        )
        conn.commit()
    request = _body(
        expected_state="PAPER_OBSERVING",
        expected_version=6,
        observation_key="heartbeat-lost-cas",
    )

    with pytest.raises(StalePromotionVersion, match="concurrency race"):
        service._heartbeat(
            db,
            stale,
            request,
            service.deterministic_observation_id(
                "promotion-recovery",
                request.observation_key,
            ),
            "corr-stale",
        )
    db.conn.close()


def test_drawdown_configuration_and_ids_fail_safely(monkeypatch):
    monkeypatch.setenv("BACKTEST_PROMOTION_MAX_PAPER_DRAWDOWN_PCT", "invalid")
    assert service._max_drawdown_pct() == 0.10
    monkeypatch.setenv("BACKTEST_PROMOTION_MAX_PAPER_DRAWDOWN_PCT", "0")
    assert service._max_drawdown_pct() == 0.10
    monkeypatch.setenv("BACKTEST_PROMOTION_MAX_PAPER_DRAWDOWN_PCT", "0.25")
    assert service._max_drawdown_pct() == 0.25

    first = service.deterministic_observation_id("promotion-1", "key-1")
    assert first == service.deterministic_observation_id("promotion-1", "key-1")
    assert first != service.deterministic_observation_id("promotion-1", "key-2")


def test_observation_contract_rejects_invalid_values():
    with pytest.raises(ValidationError, match="unsupported characters"):
        _body(observation_key="bad key")
    with pytest.raises(ValidationError, match="timezone"):
        _body(observed_at=datetime(2026, 8, 3, 5, 0))
    with pytest.raises(ValidationError, match="ISO-8601"):
        ObserveBacktestPromotionBody.model_validate(
            {
                **_body().model_dump(),
                "observed_at": "not-a-date",
            }
        )
    with pytest.raises(ValidationError, match="non-empty strings"):
        _body(notes=["   "])
    with pytest.raises(ValidationError):
        _body(paper_drawdown_pct=float("nan"))
    with pytest.raises(ValidationError):
        ObserveBacktestPromotionBody.model_validate(
            {
                **_body().model_dump(),
                "unknown_field": "blocked",
            }
        )
