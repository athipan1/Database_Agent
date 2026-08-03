from __future__ import annotations

import os
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

import pytest

from backtest_models import CreateBacktestRunBody, SkillBacktestResult
from backtest_promotion_base import (
    InvalidPromotionTransition,
    PromotionTerminalState,
    StalePromotionVersion,
)
from backtest_promotion_models import (
    CreateBacktestPromotionBody,
    TransitionBacktestPromotionBody,
)
from backtest_promotion_repository import (
    create_backtest_promotion,
    list_backtest_promotion_history,
    setup_backtest_promotion_tables,
    transition_backtest_promotion,
)
from backtest_repository import create_backtest_run_detail, setup_backtest_tables
from trading_db import TradingDB


pytestmark = pytest.mark.skipif(
    not os.getenv("POSTGRES_HOST"),
    reason="PostgreSQL service is not configured for this test run",
)


def _seed_promotion(db: TradingDB):
    suffix = uuid.uuid4().hex
    run_id = f"promotion-pg-{suffix}"
    account_id = f"account-{suffix}"
    fingerprint = uuid.uuid5(uuid.NAMESPACE_URL, run_id).hex * 2
    now = datetime.now(timezone.utc)
    setup_backtest_tables(db)
    setup_backtest_promotion_tables(db)
    create_backtest_run_detail(
        db,
        CreateBacktestRunBody(
            run_id=run_id,
            account_id=account_id,
            skill_id="skill-pg-concurrency",
            strategy_id="strategy-pg-concurrency",
            symbol="AAPL",
            timeframe="1d",
            start_time=now - timedelta(days=365),
            end_time=now,
            status="completed",
            engine_version="backtest-agent-pg-test",
            metrics={"total_trades": 32, "kill_switch_events": 0},
            metadata={
                "dataset_fingerprint": fingerprint,
                "validation_profile": "nested_walk_forward_v2",
            },
            created_at=now,
            updated_at=now,
            skill_result=SkillBacktestResult(
                result_id=f"result-{run_id}",
                skill_id="skill-pg-concurrency",
                run_id=run_id,
                passed=True,
                status="backtest_passed",
                total_trades=32,
                reasons=[],
                metadata={},
                created_at=now,
            ),
        ),
    )
    promotion = create_backtest_promotion(
        db,
        CreateBacktestPromotionBody(
            account_id=account_id,
            run_id=run_id,
            skill_id="skill-pg-concurrency",
            strategy_id="strategy-pg-concurrency",
            symbol="AAPL",
            timeframe="1d",
            dataset_fingerprint=fingerprint,
            engine_version="backtest-agent-pg-test",
            validation_profile="nested_walk_forward_v2",
        ),
        f"corr-create-{suffix}",
    )
    return promotion


def _validated_request(promotion):
    return TransitionBacktestPromotionBody(
        expected_state="GENERATED",
        expected_version=1,
        next_state="VALIDATED",
        reason_code="evidence_contract_passed",
        reason="Exact evidence identity and base validation passed",
        evidence_run_id=promotion.run_id,
        evidence_version=promotion.evidence_version,
    )


def _cleanup(db: TradingDB, promotion) -> None:
    statements = [
        ("DELETE FROM backtest_promotion_transitions WHERE promotion_id = %s", (promotion.promotion_id,)),
        ("DELETE FROM backtest_promotions WHERE promotion_id = %s", (promotion.promotion_id,)),
        ("DELETE FROM skill_backtest_results WHERE run_id = %s", (promotion.run_id,)),
        ("DELETE FROM backtest_equity_curve WHERE run_id = %s", (promotion.run_id,)),
        ("DELETE FROM backtest_trades WHERE run_id = %s", (promotion.run_id,)),
        ("DELETE FROM backtest_runs WHERE run_id = %s", (promotion.run_id,)),
    ]
    with db.connection_scope() as conn:
        cursor = db.get_cursor(conn)
        try:
            for statement, params in statements:
                cursor.execute(statement, params)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cursor.close()


def test_postgres_identical_concurrent_transition_is_one_write_many_replays(monkeypatch):
    monkeypatch.delenv("USE_SQLITE", raising=False)
    db = TradingDB(max_retries=3, initial_delay=0.1)
    promotion = _seed_promotion(db)
    request = _validated_request(promotion)
    try:
        with ThreadPoolExecutor(max_workers=8) as executor:
            results = list(
                executor.map(
                    lambda index: transition_backtest_promotion(
                        db,
                        promotion.promotion_id,
                        request,
                        f"corr-identical-{index}",
                    ),
                    range(16),
                )
            )

        assert {(item.state, item.version) for item in results} == {("VALIDATED", 2)}
        assert sum(not item.idempotent_replay for item in results) == 1
        assert sum(item.idempotent_replay for item in results) == 15
        history = list_backtest_promotion_history(db, promotion.promotion_id)
        assert len(history) == 1
        assert history[0].from_version == 1
        assert history[0].to_version == 2
    finally:
        _cleanup(db, promotion)
        if db.pool is not None:
            db.pool.closeall()


def test_postgres_conflicting_concurrent_transition_has_single_winner(monkeypatch):
    monkeypatch.delenv("USE_SQLITE", raising=False)
    db = TradingDB(max_retries=3, initial_delay=0.1)
    promotion = _seed_promotion(db)
    validated = _validated_request(promotion)
    failed = TransitionBacktestPromotionBody(
        expected_state="GENERATED",
        expected_version=1,
        next_state="FAILED",
        reason_code="database_test_failure",
        reason="Conflicting transition used to prove compare-and-swap isolation",
        evidence_run_id=promotion.run_id,
        evidence_version=promotion.evidence_version,
    )
    try:
        def apply(body):
            try:
                return transition_backtest_promotion(
                    db,
                    promotion.promotion_id,
                    body,
                    f"corr-conflict-{body.next_state.lower()}",
                )
            except (StalePromotionVersion, PromotionTerminalState, InvalidPromotionTransition) as exc:
                return exc

        with ThreadPoolExecutor(max_workers=2) as executor:
            results = list(executor.map(apply, (validated, failed)))

        successes = [result for result in results if not isinstance(result, Exception)]
        failures = [result for result in results if isinstance(result, Exception)]
        assert len(successes) == 1
        assert len(failures) == 1
        assert successes[0].version == 2
        assert successes[0].state in {"VALIDATED", "FAILED"}
        assert len(list_backtest_promotion_history(db, promotion.promotion_id)) == 1
    finally:
        _cleanup(db, promotion)
        if db.pool is not None:
            db.pool.closeall()
