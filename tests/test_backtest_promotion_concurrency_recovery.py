from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

import backtest_promotion_concurrency as concurrency
from backtest_promotion_base import StalePromotionVersion
from backtest_promotion_models import (
    BacktestPromotionRecord,
    TransitionBacktestPromotionBody,
)


def _record(*, replay: bool) -> BacktestPromotionRecord:
    now = datetime.now(timezone.utc)
    return BacktestPromotionRecord(
        promotion_id="promotion-1",
        account_id="account-1",
        run_id="run-1",
        skill_id="skill-1",
        strategy_id="strategy-1",
        symbol="AAPL",
        timeframe="1d",
        dataset_fingerprint="a" * 64,
        engine_version="engine-1",
        validation_profile="nested_walk_forward_v2",
        state="VALIDATED",
        version=2,
        evidence_version=1,
        created_at=now,
        updated_at=now,
        metadata={},
        idempotent_replay=replay,
    )


def _body() -> TransitionBacktestPromotionBody:
    return TransitionBacktestPromotionBody(
        expected_state="GENERATED",
        expected_version=1,
        next_state="VALIDATED",
        reason_code="evidence_validated",
        reason="evidence contract passed",
        evidence_run_id="run-1",
    )


def test_identical_post_lock_stale_version_recovers_completed_snapshot(monkeypatch):
    expected = _record(replay=True)

    def raw_transition(*args, **kwargs):
        raise StalePromotionVersion("lost compare-and-swap")

    monkeypatch.setattr(concurrency, "_raw_transition_backtest_promotion", raw_transition)
    monkeypatch.setattr(
        concurrency,
        "_recover_completed_replay",
        lambda db, transition_id: expected,
    )

    result = concurrency.transition_backtest_promotion(
        SimpleNamespace(),
        "promotion-1",
        _body(),
        "corr-retry",
    )
    assert result == expected
    assert result.idempotent_replay is True


def test_different_stale_request_remains_blocked(monkeypatch):
    def raw_transition(*args, **kwargs):
        raise StalePromotionVersion("genuinely stale")

    monkeypatch.setattr(concurrency, "_raw_transition_backtest_promotion", raw_transition)
    monkeypatch.setattr(
        concurrency,
        "_recover_completed_replay",
        lambda db, transition_id: None,
    )

    with pytest.raises(StalePromotionVersion):
        concurrency.transition_backtest_promotion(
            SimpleNamespace(),
            "promotion-1",
            _body(),
            "corr-stale",
        )
