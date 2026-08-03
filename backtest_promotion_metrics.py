from __future__ import annotations

from prometheus_client import Counter, Histogram


PROMOTIONS_CREATED = Counter(
    "backtest_promotions_created_total",
    "Backtest promotion records created.",
)
PROMOTION_TRANSITIONS = Counter(
    "backtest_promotion_transitions_total",
    "Completed backtest promotion transitions.",
    ("from_state", "to_state"),
)
PROMOTION_TRANSITION_FAILURES = Counter(
    "backtest_promotion_transition_failures_total",
    "Rejected or failed backtest promotion transition attempts.",
    ("error_code",),
)
PROMOTION_STALE_VERSION = Counter(
    "backtest_promotion_stale_version_total",
    "Backtest promotion transitions rejected for stale versions.",
)
PROMOTION_DUPLICATE_TRANSITION = Counter(
    "backtest_promotion_duplicate_transition_total",
    "Idempotent backtest promotion transition replays.",
)
PROMOTION_EXPIRED = Counter(
    "backtest_promotion_expired_total",
    "Backtest promotions moved to or rejected as expired.",
)
PROMOTION_REVOKED = Counter(
    "backtest_promotion_revoked_total",
    "Backtest promotions revoked.",
)
PROMOTION_APPROVED = Counter(
    "backtest_promotion_approved_total",
    "Backtest promotions approved for paper trading.",
)
PROMOTION_LOOKUP_FAILURES = Counter(
    "backtest_promotion_lookup_failures_total",
    "Exact backtest promotion lookup failures.",
    ("error_code",),
)
PROMOTION_TRANSITION_DURATION = Histogram(
    "backtest_promotion_transition_duration_seconds",
    "Backtest promotion transition duration in seconds.",
    ("to_state",),
)

# Labels are deliberately bounded. Symbols, run IDs, promotion IDs,
# correlation IDs, account IDs, and caller-controlled reason codes are never
# exposed as Prometheus labels.
