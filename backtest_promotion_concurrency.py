from __future__ import annotations

from typing import Optional

from backtest_promotion_base import (
    StalePromotionVersion,
    _promotion_from_transition_replay,
    _select_transition,
    deterministic_transition_id,
    setup_backtest_promotion_tables,
)
from backtest_promotion_models import (
    BacktestPromotionRecord,
    RevokeBacktestPromotionBody,
    TransitionBacktestPromotionBody,
)
from backtest_promotion_transition import (
    revoke_backtest_promotion as _raw_revoke_backtest_promotion,
)
from backtest_promotion_transition import (
    transition_backtest_promotion as _raw_transition_backtest_promotion,
)


def _recover_completed_replay(
    db,
    *,
    transition_id: str,
) -> Optional[BacktestPromotionRecord]:
    setup_backtest_promotion_tables(db)
    with db.connection_scope() as conn:
        cursor = db.get_cursor(conn)
        try:
            row = _select_transition(cursor, db, transition_id)
            if not row:
                return None
            return _promotion_from_transition_replay(row)
        finally:
            cursor.close()


def transition_backtest_promotion(
    db,
    promotion_id: str,
    body: TransitionBacktestPromotionBody,
    correlation_id: Optional[str],
) -> BacktestPromotionRecord:
    """Return the committed replay when an identical CAS contender loses.

    Two PostgreSQL requests can both miss the transition-history row before
    either acquires the promotion row lock. The loser observes a newer version
    after the winner commits. A deterministic transition lookup distinguishes
    an identical retry from a genuinely stale, different request.
    """

    transition_id = deterministic_transition_id(
        promotion_id=promotion_id,
        expected_version=body.expected_version,
        expected_state=body.expected_state,
        next_state=body.next_state,
        evidence_run_id=body.evidence_run_id,
        reason_code=body.reason_code,
    )
    try:
        return _raw_transition_backtest_promotion(
            db,
            promotion_id,
            body,
            correlation_id,
        )
    except StalePromotionVersion:
        replay = _recover_completed_replay(db, transition_id=transition_id)
        if replay is not None:
            return replay
        raise


def revoke_backtest_promotion(
    db,
    promotion_id: str,
    body: RevokeBacktestPromotionBody,
    correlation_id: Optional[str],
) -> BacktestPromotionRecord:
    """Retry the revoke reader once after a concurrent identical commit."""

    try:
        return _raw_revoke_backtest_promotion(
            db,
            promotion_id,
            body,
            correlation_id,
        )
    except StalePromotionVersion:
        return _raw_revoke_backtest_promotion(
            db,
            promotion_id,
            body,
            correlation_id,
        )
