from __future__ import annotations

import math
from datetime import timedelta, timezone
from typing import Any, Iterable, Optional

from backtest_promotion_base import (
    TERMINAL_STATES,
    InvalidPromotionTransition,
    PromotionApprovalRequired,
    PromotionEvidenceMismatch,
    PromotionExpired,
    PromotionValidationFailed,
    _assert_finite_json,
    _db_time,
    _env_bool,
    _env_float,
    _env_int,
    _require_exact_evidence,
    _utc_now,
)
from backtest_promotion_models import BacktestPromotionRecord, TransitionBacktestPromotionBody
from backtest_repository import get_backtest_run_detail, setup_backtest_tables


def _load_exact_evidence(db, promotion: BacktestPromotionRecord):
    setup_backtest_tables(db)
    detail = get_backtest_run_detail(db, promotion.run_id)
    if detail is None:
        raise PromotionEvidenceMismatch(
            f"backtest evidence run {promotion.run_id} was not found"
        )
    metadata = _require_exact_evidence(promotion, detail)
    _assert_finite_json(detail.run.parameters, path="evidence.parameters")
    _assert_finite_json(detail.run.metrics, path="evidence.metrics")
    _assert_finite_json(metadata, path="evidence.metadata")
    return detail, metadata


def _validate_base_evidence(promotion: BacktestPromotionRecord, detail: Any, metadata: dict) -> None:
    run = detail.run
    if str(run.status).lower() != "completed":
        raise PromotionValidationFailed("backtest run status must be completed")
    if detail.skill_result is None:
        raise PromotionValidationFailed("backtest skill result is required")
    if not detail.skill_result.passed:
        raise PromotionValidationFailed("backtest skill result did not pass")
    if "pass" not in str(detail.skill_result.status).lower():
        raise PromotionValidationFailed("backtest skill result status is not a pass status")
    for field_name in ("created_at", "updated_at", "start_time", "end_time"):
        value = getattr(run, field_name, None)
        if value is not None and value.tzinfo is None:
            raise PromotionValidationFailed(
                f"backtest {field_name} must include timezone information"
            )
    updated_at = run.updated_at or run.created_at
    if updated_at is None or updated_at.tzinfo is None:
        raise PromotionValidationFailed("backtest evidence timestamp is missing")
    max_age_hours = _env_int("BACKTEST_PROMOTION_EVIDENCE_MAX_AGE_HOURS", 168)
    if _utc_now() - updated_at.astimezone(timezone.utc) > timedelta(hours=max_age_hours):
        raise PromotionExpired(
            f"backtest evidence is older than {max_age_hours} hours"
        )
    if metadata.get("validation_profile") != "nested_walk_forward_v2":
        raise PromotionValidationFailed(
            "validation_profile must be nested_walk_forward_v2"
        )


def _required_true(mapping: Any, names: Iterable[str], *, prefix: str) -> None:
    if not isinstance(mapping, dict):
        raise PromotionValidationFailed(f"{prefix} must be an object")
    failures = [name for name in names if mapping.get(name) is not True]
    if failures:
        raise PromotionValidationFailed(
            f"{prefix} failed required gates: {', '.join(sorted(failures))}"
        )


def _validate_oos_evidence(promotion: BacktestPromotionRecord, metadata: dict) -> None:
    validation = metadata.get("walk_forward_validation")
    criteria = metadata.get("walk_forward_criteria")
    promotion_gates = metadata.get("promotion_gates")
    statistical_criteria = metadata.get("statistical_criteria")
    statistical_evidence = metadata.get("statistical_evidence")
    selection_gates = metadata.get("selection_gates")

    if not isinstance(validation, dict) or not isinstance(criteria, dict):
        raise PromotionValidationFailed("nested walk-forward evidence is missing")
    if validation.get("status") != "completed" or validation.get("passed") is not True:
        raise PromotionValidationFailed("nested walk-forward validation did not pass")
    evaluated = int(validation.get("evaluated_windows") or 0)
    minimum = int(criteria.get("min_windows") or 0)
    if minimum < 1 or evaluated < minimum:
        raise PromotionValidationFailed(
            f"insufficient independent test windows: evaluated={evaluated}, required={minimum}"
        )
    if validation.get("overlapping_test_windows") is not False:
        raise PromotionValidationFailed("walk-forward test windows overlap")
    if validation.get("latest_selection_eligible") is not True:
        raise PromotionValidationFailed("latest strategy selection is not eligible")
    if validation.get("latest_selected_strategy_id") != promotion.strategy_id:
        raise PromotionEvidenceMismatch(
            "latest selected strategy does not match promotion strategy"
        )
    if int(validation.get("total_kill_switch_events") or 0) != 0:
        raise PromotionValidationFailed("walk-forward evidence contains a kill-switch event")
    _required_true(
        promotion_gates,
        [
            "nested_validation_passed",
            "latest_selection_eligible",
            "exact_strategy_match",
            "independent_test_windows",
            "statistical_validation_enabled",
        ],
        prefix="promotion_gates",
    )
    if not isinstance(statistical_criteria, dict) or statistical_criteria.get("enabled") is not True:
        raise PromotionValidationFailed("statistical validation must be enabled")
    if not isinstance(statistical_evidence, dict):
        raise PromotionValidationFailed("statistical evidence is missing")
    if statistical_evidence.get("status") != "completed" or statistical_evidence.get("passed") is not True:
        raise PromotionValidationFailed("statistical evidence did not pass")
    _required_true(
        statistical_evidence.get("gates"),
        [
            "observation_count",
            "trade_count",
            "adjusted_p_value",
            "probabilistic_sharpe_ratio",
            "deflated_sharpe_probability",
            "bootstrap_lower_bound",
        ],
        prefix="statistical_evidence.gates",
    )
    thresholds = [
        (
            "adjusted_p_value",
            lambda value, limit: value <= limit,
            statistical_criteria.get("max_adjusted_p_value"),
        ),
        (
            "probabilistic_sharpe_ratio",
            lambda value, limit: value >= limit,
            statistical_criteria.get("min_probabilistic_sharpe_ratio"),
        ),
        (
            "deflated_sharpe_probability",
            lambda value, limit: value >= limit,
            statistical_criteria.get("min_deflated_sharpe_probability"),
        ),
        (
            "bootstrap_annualized_return_lower",
            lambda value, limit: value > limit,
            statistical_criteria.get("min_bootstrap_annualized_return"),
        ),
    ]
    for name, comparator, limit in thresholds:
        value = statistical_evidence.get(name)
        if not isinstance(value, (int, float)) or not isinstance(limit, (int, float)):
            raise PromotionValidationFailed(f"statistical evidence {name} is missing")
        if not math.isfinite(float(value)) or not math.isfinite(float(limit)):
            raise PromotionValidationFailed(f"statistical evidence {name} is non-finite")
        if not comparator(float(value), float(limit)):
            raise PromotionValidationFailed(
                f"statistical evidence {name} failed policy: value={value}, limit={limit}"
            )
    required_selection_gates = {
        "statistical_adjusted_p_value",
        "statistical_probabilistic_sharpe_ratio",
        "statistical_deflated_sharpe_probability",
        "statistical_bootstrap_lower_bound",
    }
    _required_true(selection_gates, required_selection_gates, prefix="selection_gates")


def _validate_robustness_evidence(metadata: dict) -> None:
    robustness = metadata.get("robustness_validation")
    if not isinstance(robustness, dict):
        raise PromotionValidationFailed("robustness_validation evidence is missing")
    if robustness.get("status") != "completed" or robustness.get("passed") is not True:
        raise PromotionValidationFailed("robustness validation did not pass")
    _required_true(
        robustness.get("gates"),
        [
            "parameter_perturbation",
            "fee_stress",
            "spread_stress",
            "slippage_stress",
            "liquidity_stress",
            "drawdown_stress",
            "minimum_scenario_pass_rate",
            "no_catastrophic_loss",
            "finite_metrics",
        ],
        prefix="robustness_validation.gates",
    )
    pass_rate = robustness.get("scenario_pass_rate")
    required_rate = _env_float(
        "BACKTEST_PROMOTION_MIN_ROBUSTNESS_PASS_RATE",
        0.80,
        minimum=0.0,
    )
    if not isinstance(pass_rate, (int, float)) or not math.isfinite(float(pass_rate)):
        raise PromotionValidationFailed("robustness scenario pass rate is missing")
    if float(pass_rate) < required_rate:
        raise PromotionValidationFailed(
            f"robustness scenario pass rate {pass_rate} is below {required_rate}"
        )
    if robustness.get("catastrophic_loss") is True:
        raise PromotionValidationFailed("robustness evidence contains catastrophic loss")


def _latest_exact_run_id(db, promotion: BacktestPromotionRecord) -> Optional[str]:
    with db.connection_scope() as conn:
        cursor = db.get_cursor(conn)
        try:
            cursor.execute(
                f"""
                SELECT run_id FROM backtest_runs
                WHERE account_id = {db.param_style}
                  AND skill_id = {db.param_style}
                  AND strategy_id = {db.param_style}
                  AND symbol = {db.param_style}
                  AND timeframe = {db.param_style}
                ORDER BY updated_at DESC, created_at DESC, run_id DESC
                LIMIT 1
                """,
                (
                    promotion.account_id,
                    promotion.skill_id,
                    promotion.strategy_id,
                    promotion.symbol,
                    promotion.timeframe,
                ),
            )
            row = cursor.fetchone()
            return str(row[0]) if row else None
        finally:
            cursor.close()


def _newer_blocking_promotion(db, promotion: BacktestPromotionRecord) -> Optional[str]:
    with db.connection_scope() as conn:
        cursor = db.get_cursor(conn)
        try:
            cursor.execute(
                f"""
                SELECT promotion_id FROM backtest_promotions
                WHERE account_id = {db.param_style}
                  AND symbol = {db.param_style}
                  AND strategy_id = {db.param_style}
                  AND timeframe = {db.param_style}
                  AND created_at > {db.param_style}
                  AND state IN ('FAILED', 'REVOKED')
                ORDER BY created_at DESC, promotion_id DESC
                LIMIT 1
                """,
                (
                    promotion.account_id,
                    promotion.symbol,
                    promotion.strategy_id,
                    promotion.timeframe,
                    _db_time(db, promotion.created_at),
                ),
            )
            row = cursor.fetchone()
            return str(row[0]) if row else None
        finally:
            cursor.close()


def _validate_approval(db, promotion: BacktestPromotionRecord, body: TransitionBacktestPromotionBody) -> None:
    if promotion.state != "ROBUSTNESS_PASSED":
        raise InvalidPromotionTransition(
            "paper approval requires ROBUSTNESS_PASSED state"
        )
    if promotion.expires_at is not None and promotion.expires_at <= _utc_now():
        raise PromotionExpired("promotion evidence expired before paper approval")
    latest_run_id = _latest_exact_run_id(db, promotion)
    if latest_run_id != promotion.run_id:
        raise PromotionEvidenceMismatch(
            f"promotion evidence was superseded by newer run {latest_run_id!r}"
        )
    newer_blocker = _newer_blocking_promotion(db, promotion)
    if newer_blocker is not None:
        raise PromotionEvidenceMismatch(
            f"newer failed or revoked promotion blocks approval: {newer_blocker}"
        )
    auto_approve = _env_bool("BACKTEST_PROMOTION_AUTO_APPROVE_PAPER", False)
    approval_required = _env_bool("BACKTEST_PROMOTION_APPROVAL_REQUIRED", True)
    if not auto_approve and approval_required and not body.approver:
        raise PromotionApprovalRequired(
            "manual approver is required because automatic paper approval is disabled"
        )


def _validate_transition_evidence(
    db,
    promotion: BacktestPromotionRecord,
    body: TransitionBacktestPromotionBody,
) -> None:
    if body.evidence_run_id != promotion.run_id:
        raise PromotionEvidenceMismatch(
            "transition evidence_run_id does not match promotion run_id"
        )
    if body.evidence_version is not None and body.evidence_version != promotion.evidence_version:
        raise PromotionEvidenceMismatch(
            "transition evidence_version does not match promotion evidence_version"
        )
    if body.next_state in TERMINAL_STATES:
        return
    detail, metadata = _load_exact_evidence(db, promotion)
    _validate_base_evidence(promotion, detail, metadata)
    if body.next_state in {
        "OOS_PASSED",
        "ROBUSTNESS_PASSED",
        "APPROVED_FOR_PAPER",
        "PAPER_OBSERVING",
    }:
        _validate_oos_evidence(promotion, metadata)
    if body.next_state in {
        "ROBUSTNESS_PASSED",
        "APPROVED_FOR_PAPER",
        "PAPER_OBSERVING",
    }:
        _validate_robustness_evidence(metadata)
    if body.next_state == "APPROVED_FOR_PAPER":
        _validate_approval(db, promotion, body)


__all__ = [
    "_load_exact_evidence",
    "_validate_base_evidence",
    "_required_true",
    "_validate_oos_evidence",
    "_validate_robustness_evidence",
    "_latest_exact_run_id",
    "_newer_blocking_promotion",
    "_validate_approval",
    "_validate_transition_evidence",
]
