from __future__ import annotations

from datetime import datetime, timedelta, timezone

from backtest_promotion_transition import _approved_for_paper_expiry


APPROVED_AT = datetime(2026, 8, 4, 1, 0, tzinfo=timezone.utc)


def test_paper_approval_preserves_stricter_explicit_expiry(monkeypatch):
    monkeypatch.setenv("BACKTEST_PROMOTION_MAX_AGE_HOURS", "168")
    explicit_expiry = APPROVED_AT + timedelta(seconds=10)

    result = _approved_for_paper_expiry(
        current_expires_at=explicit_expiry,
        approved_at=APPROVED_AT,
    )

    assert result == explicit_expiry


def test_paper_approval_caps_later_expiry_at_policy_horizon(monkeypatch):
    monkeypatch.setenv("BACKTEST_PROMOTION_MAX_AGE_HOURS", "24")
    explicit_expiry = APPROVED_AT + timedelta(days=7)

    result = _approved_for_paper_expiry(
        current_expires_at=explicit_expiry,
        approved_at=APPROVED_AT,
    )

    assert result == APPROVED_AT + timedelta(hours=24)


def test_paper_approval_assigns_policy_horizon_without_explicit_expiry(
    monkeypatch,
):
    monkeypatch.setenv("BACKTEST_PROMOTION_MAX_AGE_HOURS", "12")

    result = _approved_for_paper_expiry(
        current_expires_at=None,
        approved_at=APPROVED_AT,
    )

    assert result == APPROVED_AT + timedelta(hours=12)
