from __future__ import annotations

import pytest
from fastapi import HTTPException

from backtest_promotion_routes import _require_privileged_credential


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
