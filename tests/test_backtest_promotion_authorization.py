from __future__ import annotations

from datetime import datetime

import pytest
from fastapi import HTTPException
from pydantic import ValidationError

from backtest_promotion_models import (
    CreateBacktestPromotionBody,
    TransitionBacktestPromotionBody,
)
from backtest_promotion_routes import _require_privileged_credential


def _create_payload() -> dict:
    return {
        "account_id": "account-1",
        "run_id": "run-1",
        "skill_id": "skill-1",
        "strategy_id": "strategy-1",
        "symbol": "AAPL",
        "timeframe": "1d",
        "dataset_fingerprint": "a" * 64,
        "engine_version": "engine-1",
        "validation_profile": "nested_walk_forward_v2",
    }


def _transition_payload() -> dict:
    return {
        "expected_state": "GENERATED",
        "expected_version": 1,
        "next_state": "VALIDATED",
        "reason_code": "evidence_validated",
        "reason": "evidence passed",
        "evidence_run_id": "run-1",
    }


def test_backtest_states_do_not_require_privileged_credential(monkeypatch):
    monkeypatch.delenv("BACKTEST_PROMOTION_APPROVAL_TOKEN", raising=False)
    _require_privileged_credential("VALIDATED", None)
    _require_privileged_credential("OOS_PASSED", None)
    _require_privileged_credential("ROBUSTNESS_PASSED", None)


@pytest.mark.parametrize(
    "state",
    ["APPROVED_FOR_PAPER", "PAPER_OBSERVING", "REVOKED", "EXPIRED"],
)
def test_privileged_states_fail_closed_without_configured_token(monkeypatch, state):
    monkeypatch.delenv("BACKTEST_PROMOTION_APPROVAL_TOKEN", raising=False)
    with pytest.raises(HTTPException) as exc_info:
        _require_privileged_credential(state, None)
    assert exc_info.value.status_code == 403


def test_privileged_credential_uses_exact_secret_match(monkeypatch):
    monkeypatch.setenv("BACKTEST_PROMOTION_APPROVAL_TOKEN", "approval-secret")

    with pytest.raises(HTTPException) as exc_info:
        _require_privileged_credential("APPROVED_FOR_PAPER", "approval-secreu")
    assert exc_info.value.status_code == 403

    _require_privileged_credential("APPROVED_FOR_PAPER", "approval-secret")


def test_metadata_accepts_finite_nested_json_and_preserves_shape():
    payload = _create_payload()
    payload["metadata"] = {
        "finite": 1.25,
        "list": [1, True, None, {"nested": "value"}],
    }
    model = CreateBacktestPromotionBody.model_validate(payload)
    assert model.metadata["finite"] == 1.25
    assert model.metadata["list"][3] == {"nested": "value"}


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("run_id", "bad id", "unsupported characters"),
        ("timeframe", "daily", "canonical value"),
        ("dataset_fingerprint", "not-a-digest", "hexadecimal digest"),
        ("expires_at", datetime(2026, 8, 3, 3, 0, 0), "include a timezone"),
    ],
)
def test_create_contract_rejects_malformed_identity_fields(field, value, message):
    payload = _create_payload()
    payload[field] = value
    with pytest.raises(ValidationError, match=message):
        CreateBacktestPromotionBody.model_validate(payload)


def test_metadata_rejects_non_string_key_and_unsupported_object():
    payload = _create_payload()
    payload["metadata"] = {1: "bad-key"}
    with pytest.raises(ValidationError, match="keys must be strings"):
        CreateBacktestPromotionBody.model_validate(payload)

    payload["metadata"] = {"bad": object()}
    with pytest.raises(ValidationError, match="unsupported value type"):
        CreateBacktestPromotionBody.model_validate(payload)


def test_transition_contract_rejects_bad_evidence_id_and_noop():
    payload = _transition_payload()
    payload["evidence_run_id"] = "bad id"
    with pytest.raises(ValidationError, match="unsupported characters"):
        TransitionBacktestPromotionBody.model_validate(payload)

    payload = _transition_payload()
    payload["next_state"] = "GENERATED"
    with pytest.raises(ValidationError, match="must differ"):
        TransitionBacktestPromotionBody.model_validate(payload)
