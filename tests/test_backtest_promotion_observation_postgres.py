from __future__ import annotations

import os
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

import pytest

from backtest_promotion_base import StalePromotionVersion
from backtest_promotion_observation_models import ObserveBacktestPromotionBody
from backtest_promotion_observation_service import (
    list_backtest_promotion_observations,
    observe_backtest_promotion,
    setup_backtest_promotion_observation_tables,
)
from backtest_promotion_repository import (
    get_backtest_promotion,
    setup_backtest_promotion_tables,
)
from trading_db import TradingDB


pytestmark = pytest.mark.skipif(
    not os.getenv("POSTGRES_HOST"),
    reason="PostgreSQL service is not configured for this test run",
)


def _seed_observing_promotion(db: TradingDB) -> str:
    suffix = uuid.uuid4().hex
    promotion_id = f"promotion-observation-pg-{suffix}"
    now = datetime.now(timezone.utc)
    setup_backtest_promotion_tables(db)
    setup_backtest_promotion_observation_tables(db)
    with db.connection_scope() as conn:
        cursor = db.get_cursor(conn)
        try:
            cursor.execute(
                """
                INSERT INTO backtest_promotions (
                    promotion_id, account_id, run_id, skill_id, strategy_id,
                    symbol, timeframe, dataset_fingerprint, engine_version,
                    validation_profile, state, version, evidence_version,
                    created_at, updated_at, approved_for_paper_at,
                    paper_observing_at, expires_at, last_observed_at,
                    reason_code, reason, correlation_id, metadata
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                """,
                (
                    promotion_id,
                    f"account-{suffix}",
                    f"run-{suffix}",
                    "skill-observation-pg",
                    f"strategy-{suffix}",
                    "AAPL",
                    "1d",
                    uuid.uuid5(uuid.NAMESPACE_URL, suffix).hex * 2,
                    "backtest-agent-pg-test",
                    "nested_walk_forward_v2",
                    "PAPER_OBSERVING",
                    6,
                    1,
                    now - timedelta(hours=2),
                    now - timedelta(hours=1),
                    now - timedelta(hours=1),
                    now - timedelta(minutes=30),
                    now + timedelta(days=1),
                    now - timedelta(minutes=30),
                    "paper_observation_started",
                    "seed observing promotion",
                    f"corr-seed-{suffix}",
                    "{}",
                ),
            )
            conn.commit()
        finally:
            cursor.close()
    return promotion_id


def _request(key: str) -> ObserveBacktestPromotionBody:
    return ObserveBacktestPromotionBody(
        expected_state="PAPER_OBSERVING",
        expected_version=6,
        observation_key=key,
        observed_at=datetime.now(timezone.utc),
        paper_drawdown_pct=0.01,
        reconciliation_ok=True,
        duplicate_order_count=0,
        broker_order_count=1,
        database_order_count=1,
        filled_order_count=1,
        strategy_drift=False,
        emergency_halt=False,
        notes=["postgres concurrency healthy"],
        correlation_id="corr-observation-pg",
        metadata={"source": "postgres-concurrency-test"},
    )


def _cleanup(db: TradingDB, promotion_id: str) -> None:
    with db.connection_scope() as conn:
        cursor = db.get_cursor(conn)
        try:
            cursor.execute(
                "DELETE FROM backtest_promotion_observations "
                "WHERE promotion_id = %s",
                (promotion_id,),
            )
            cursor.execute(
                "DELETE FROM backtest_promotion_transitions "
                "WHERE promotion_id = %s",
                (promotion_id,),
            )
            cursor.execute(
                "DELETE FROM backtest_promotions WHERE promotion_id = %s",
                (promotion_id,),
            )
            conn.commit()
        finally:
            cursor.close()


def test_identical_concurrent_heartbeat_is_single_write_many_replays(
    monkeypatch,
):
    monkeypatch.delenv("USE_SQLITE", raising=False)
    db = TradingDB(max_retries=3, initial_delay=0.1)
    promotion_id = _seed_observing_promotion(db)
    request = _request("heartbeat-identical")
    try:
        def apply(index: int):
            try:
                return observe_backtest_promotion(
                    db,
                    promotion_id,
                    request,
                    f"corr-identical-{index}",
                )
            except StalePromotionVersion:
                return observe_backtest_promotion(
                    db,
                    promotion_id,
                    request,
                    f"corr-identical-retry-{index}",
                )

        with ThreadPoolExecutor(max_workers=8) as executor:
            results = list(executor.map(apply, range(16)))

        assert {(item.to_state, item.to_version) for item in results} == {
            ("PAPER_OBSERVING", 7)
        }
        assert sum(not item.idempotent_replay for item in results) == 1
        assert sum(item.idempotent_replay for item in results) == 15
        assert len(
            list_backtest_promotion_observations(db, promotion_id)
        ) == 1
        assert get_backtest_promotion(db, promotion_id).version == 7
    finally:
        _cleanup(db, promotion_id)
        if db.pool is not None:
            db.pool.closeall()


def test_conflicting_concurrent_heartbeats_have_one_winner(monkeypatch):
    monkeypatch.delenv("USE_SQLITE", raising=False)
    db = TradingDB(max_retries=3, initial_delay=0.1)
    promotion_id = _seed_observing_promotion(db)
    requests = (_request("heartbeat-a"), _request("heartbeat-b"))
    try:
        def apply(request: ObserveBacktestPromotionBody):
            try:
                return observe_backtest_promotion(
                    db,
                    promotion_id,
                    request,
                    request.correlation_id,
                )
            except StalePromotionVersion as exc:
                return exc

        with ThreadPoolExecutor(max_workers=2) as executor:
            results = list(executor.map(apply, requests))

        successes = [item for item in results if not isinstance(item, Exception)]
        failures = [item for item in results if isinstance(item, Exception)]
        assert len(successes) == 1
        assert len(failures) == 1
        assert successes[0].to_version == 7
        assert len(
            list_backtest_promotion_observations(db, promotion_id)
        ) == 1
        assert get_backtest_promotion(db, promotion_id).version == 7
    finally:
        _cleanup(db, promotion_id)
        if db.pool is not None:
            db.pool.closeall()
