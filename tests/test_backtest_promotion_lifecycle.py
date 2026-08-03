from __future__ import annotations

import os
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient
from pydantic import ValidationError

os.environ.setdefault("USE_SQLITE", "1")

from backtest_models import CreateBacktestRunBody, SkillBacktestResult
from backtest_promotion_models import (
    CreateBacktestPromotionBody,
    RevokeBacktestPromotionBody,
    TransitionBacktestPromotionBody,
)
from backtest_promotion_repository import (
    InvalidPromotionTransition,
    PromotionApprovalRequired,
    PromotionTerminalState,
    StalePromotionVersion,
    create_backtest_promotion,
    get_latest_exact_backtest_promotion,
    list_backtest_promotion_history,
    revoke_backtest_promotion,
    transition_backtest_promotion,
)
from backtest_promotion_routes import create_backtest_promotion_routes
from backtest_repository import create_backtest_run_detail, setup_backtest_tables


class PromotionDB:
    db_type = "sqlite"
    param_style = "?"

    def __init__(self):
        self.conn = sqlite3.connect(":memory:", check_same_thread=False)
        self.conn.row_factory = sqlite3.Row

    @contextmanager
    def connection_scope(self):
        yield self.conn

    def get_cursor(self, conn):
        return conn.cursor()


@pytest.fixture()
def db(monkeypatch):
    monkeypatch.setenv("BACKTEST_PROMOTION_EVIDENCE_MAX_AGE_HOURS", "168")
    monkeypatch.setenv("BACKTEST_PROMOTION_APPROVAL_REQUIRED", "true")
    monkeypatch.setenv("BACKTEST_PROMOTION_AUTO_APPROVE_PAPER", "false")
    database = PromotionDB()
    setup_backtest_tables(database)
    yield database
    database.conn.close()


def _metadata(strategy_id: str = "strategy-1") -> dict:
    return {
        "dataset_fingerprint": "a" * 64,
        "validation_profile": "nested_walk_forward_v2",
        "walk_forward_validation": {
            "status": "completed",
            "passed": True,
            "evaluated_windows": 4,
            "overlapping_test_windows": False,
            "latest_selection_eligible": True,
            "latest_selected_strategy_id": strategy_id,
            "total_kill_switch_events": 0,
        },
        "walk_forward_criteria": {"min_windows": 4},
        "promotion_gates": {
            "nested_validation_passed": True,
            "latest_selection_eligible": True,
            "exact_strategy_match": True,
            "independent_test_windows": True,
            "statistical_validation_enabled": True,
        },
        "statistical_criteria": {
            "enabled": True,
            "max_adjusted_p_value": 0.05,
            "min_probabilistic_sharpe_ratio": 0.95,
            "min_deflated_sharpe_probability": 0.90,
            "min_bootstrap_annualized_return": 0.0,
        },
        "statistical_evidence": {
            "status": "completed",
            "passed": True,
            "adjusted_p_value": 0.01,
            "probabilistic_sharpe_ratio": 0.98,
            "deflated_sharpe_probability": 0.94,
            "bootstrap_annualized_return_lower": 0.02,
            "gates": {
                "observation_count": True,
                "trade_count": True,
                "adjusted_p_value": True,
                "probabilistic_sharpe_ratio": True,
                "deflated_sharpe_probability": True,
                "bootstrap_lower_bound": True,
            },
        },
        "selection_gates": {
            "statistical_adjusted_p_value": True,
            "statistical_probabilistic_sharpe_ratio": True,
            "statistical_deflated_sharpe_probability": True,
            "statistical_bootstrap_lower_bound": True,
        },
        "robustness_validation": {
            "status": "completed",
            "passed": True,
            "scenario_pass_rate": 0.9,
            "catastrophic_loss": False,
            "gates": {
                "parameter_perturbation": True,
                "fee_stress": True,
                "spread_stress": True,
                "slippage_stress": True,
                "liquidity_stress": True,
                "drawdown_stress": True,
                "minimum_scenario_pass_rate": True,
                "no_catastrophic_loss": True,
                "finite_metrics": True,
            },
        },
    }


def _seed_run(db, run_id: str = "run-1"):
    now = datetime.now(timezone.utc)
    return create_backtest_run_detail(
        db,
        CreateBacktestRunBody(
            run_id=run_id,
            account_id="1",
            skill_id="skill-1",
            strategy_id="strategy-1",
            symbol="AAPL",
            timeframe="1d",
            start_time=now - timedelta(days=365),
            end_time=now,
            status="completed",
            engine_version="backtest-agent-0.7.0",
            metrics={"total_trades": 40, "kill_switch_events": 0},
            metadata=_metadata(),
            created_at=now,
            updated_at=now,
            skill_result=SkillBacktestResult(
                result_id=f"result-{run_id}",
                skill_id="skill-1",
                run_id=run_id,
                passed=True,
                status="backtest_passed",
                total_trades=40,
                reasons=[],
                metadata={},
                created_at=now,
            ),
        ),
    )


def _create_body(run_id: str = "run-1") -> CreateBacktestPromotionBody:
    return CreateBacktestPromotionBody(
        account_id="1",
        run_id=run_id,
        skill_id="skill-1",
        strategy_id="strategy-1",
        symbol="aapl",
        timeframe="1D",
        dataset_fingerprint="a" * 64,
        engine_version="backtest-agent-0.7.0",
        validation_profile="nested_walk_forward_v2",
    )


def _transition(record, state: str, *, approver: str | None = None):
    return TransitionBacktestPromotionBody(
        expected_state=record.state,
        expected_version=record.version,
        next_state=state,
        reason_code=state.lower(),
        reason=f"advance to {state}",
        evidence_run_id=record.run_id,
        evidence_version=record.evidence_version,
        approver=approver,
    )


def test_contract_rejects_unknown_and_non_finite_metadata():
    payload = _create_body().model_dump()
    payload["unknown"] = True
    with pytest.raises(ValidationError):
        CreateBacktestPromotionBody.model_validate(payload)

    payload = _create_body().model_dump()
    payload["metadata"] = {"bad": float("nan")}
    with pytest.raises(ValidationError):
        CreateBacktestPromotionBody.model_validate(payload)


def test_full_lifecycle_and_historical_idempotent_replay(db):
    _seed_run(db)
    promotion = create_backtest_promotion(db, _create_body(), "corr-create")
    first_request = _transition(promotion, "VALIDATED")
    first = transition_backtest_promotion(
        db, promotion.promotion_id, first_request, "corr-validated"
    )
    for state in ("OOS_PASSED", "ROBUSTNESS_PASSED"):
        first = transition_backtest_promotion(
            db, first.promotion_id, _transition(first, state), "corr-advance"
        )

    with pytest.raises(PromotionApprovalRequired):
        transition_backtest_promotion(
            db,
            first.promotion_id,
            _transition(first, "APPROVED_FOR_PAPER"),
            "corr-no-approver",
        )

    approved = transition_backtest_promotion(
        db,
        first.promotion_id,
        _transition(first, "APPROVED_FOR_PAPER", approver="operator-1"),
        "corr-approved",
    )
    replay = transition_backtest_promotion(
        db, promotion.promotion_id, first_request, "corr-retry"
    )

    assert approved.state == "APPROVED_FOR_PAPER"
    assert approved.version == 5
    assert replay.state == "VALIDATED"
    assert replay.version == 2
    assert replay.idempotent_replay is True
    assert len(list_backtest_promotion_history(db, promotion.promotion_id)) == 4


def test_skip_stale_terminal_and_concurrent_retry_are_safe(db):
    _seed_run(db)
    promotion = create_backtest_promotion(db, _create_body(), "corr-create")

    with pytest.raises(InvalidPromotionTransition):
        transition_backtest_promotion(
            db, promotion.promotion_id, _transition(promotion, "OOS_PASSED"), "corr-skip"
        )
    stale = _transition(promotion, "VALIDATED").model_copy(
        update={"expected_version": 99}
    )
    with pytest.raises(StalePromotionVersion):
        transition_backtest_promotion(db, promotion.promotion_id, stale, "corr-stale")

    request = _transition(promotion, "VALIDATED")
    with ThreadPoolExecutor(max_workers=6) as executor:
        results = list(
            executor.map(
                lambda index: transition_backtest_promotion(
                    db, promotion.promotion_id, request, f"corr-{index}"
                ),
                range(12),
            )
        )
    assert len({(item.state, item.version) for item in results}) == 1
    assert sum(item.idempotent_replay is False for item in results) == 1
    assert len(list_backtest_promotion_history(db, promotion.promotion_id)) == 1

    rejected = transition_backtest_promotion(
        db,
        promotion.promotion_id,
        TransitionBacktestPromotionBody(
            expected_state="VALIDATED",
            expected_version=2,
            next_state="REJECTED",
            reason_code="policy_rejected",
            reason="policy failed",
            evidence_run_id="run-1",
        ),
        "corr-rejected",
    )
    with pytest.raises(PromotionTerminalState):
        transition_backtest_promotion(
            db, rejected.promotion_id, _transition(rejected, "FAILED"), "corr-terminal"
        )


def test_revoke_and_exact_lookup_are_authoritative(db):
    _seed_run(db)
    promotion = create_backtest_promotion(db, _create_body(), "corr-create")
    for state in ("VALIDATED", "OOS_PASSED", "ROBUSTNESS_PASSED"):
        promotion = transition_backtest_promotion(
            db, promotion.promotion_id, _transition(promotion, state), "corr-advance"
        )
    promotion = transition_backtest_promotion(
        db,
        promotion.promotion_id,
        _transition(promotion, "APPROVED_FOR_PAPER", approver="operator-1"),
        "corr-approved",
    )
    revoke = RevokeBacktestPromotionBody(
        expected_version=promotion.version,
        reason_code="manual_revoke",
        reason="controlled rollback",
        approver="operator-1",
    )
    revoked = revoke_backtest_promotion(db, promotion.promotion_id, revoke, "corr-revoke")
    replay = revoke_backtest_promotion(db, promotion.promotion_id, revoke, "corr-replay")

    assert revoked.state == "REVOKED"
    assert replay.idempotent_replay is True
    with pytest.raises(Exception):
        get_latest_exact_backtest_promotion(
            db,
            account_id="1",
            symbol="AAPL",
            strategy_id="strategy-1",
            timeframe="1d",
            required_state="APPROVED_FOR_PAPER",
        )


def test_versioned_authenticated_api_envelope(db):
    _seed_run(db)
    app = FastAPI()

    def api_key(value):
        if value != "database-test-key":
            raise HTTPException(status_code=403, detail="invalid")
        return value

    async def correlation_id():
        return "corr-api"

    app.include_router(create_backtest_promotion_routes(db, api_key, correlation_id))
    client = TestClient(app)
    created = client.post(
        "/backtests/promotions",
        headers={"X-API-KEY": "database-test-key"},
        json=_create_body().model_dump(mode="json"),
    )
    unauthorized = client.get(
        f"/backtests/promotions/{created.json()['data']['promotion_id']}"
    )

    assert created.status_code == 201
    assert created.json()["schema_version"] == "backtest-promotion.v1"
    assert created.json()["correlation_id"] == "corr-api"
    assert unauthorized.status_code == 403
    assert unauthorized.json()["error"]["code"] == "authentication_failed"


def test_migration_upgrade_and_downgrade_are_non_destructive():
    root = Path(__file__).resolve().parents[1]
    upgrade = (root / "migrations/003_backtest_promotion_lifecycle.up.sql").read_text()
    downgrade = (root / "migrations/003_backtest_promotion_lifecycle.down.sql").read_text()

    assert "historical backtest runs are not promoted automatically" in upgrade
    assert "UNIQUE (account_id, run_id)" in upgrade
    assert "to_version = from_version + 1" in upgrade
    assert "DROP TABLE IF EXISTS backtest_promotion_transitions" in downgrade
    assert "DROP TABLE IF EXISTS backtest_promotions" in downgrade
    assert "backtest_runs" not in downgrade
