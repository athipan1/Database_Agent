from __future__ import annotations

from datetime import timedelta
from typing import Optional

from backtest_promotion_base import (
    PromotionEvidenceMismatch,
    PromotionExpired,
    PromotionNotFound,
    _promotion_from_row,
    _utc_now,
    setup_backtest_promotion_tables,
)
from backtest_promotion_models import BacktestPromotionRecord


def get_latest_exact_backtest_promotion(
    db,
    *,
    account_id: str,
    symbol: str,
    strategy_id: str,
    timeframe: str,
    required_state: Optional[str] = None,
    max_age_hours: Optional[int] = None,
    validation_profile: Optional[str] = None,
    engine_version: Optional[str] = None,
    dataset_fingerprint: Optional[str] = None,
) -> BacktestPromotionRecord:
    """Return the newest exact evidence record, then apply policy filters.

    Creation time represents evidence arrival order. Update time is only a
    deterministic tie-breaker. This prevents an older promotion that was
    transitioned later from hiding a newer failed, revoked, or incompatible
    evidence record.
    """

    setup_backtest_promotion_tables(db)
    with db.connection_scope() as conn:
        cursor = db.get_cursor(conn)
        try:
            cursor.execute(
                f"""
                SELECT * FROM backtest_promotions
                WHERE account_id = {db.param_style}
                  AND symbol = {db.param_style}
                  AND strategy_id = {db.param_style}
                  AND timeframe = {db.param_style}
                ORDER BY created_at DESC, updated_at DESC, promotion_id DESC
                LIMIT 1
                """,
                (
                    account_id,
                    symbol.strip().upper(),
                    strategy_id.strip(),
                    timeframe.strip().lower(),
                ),
            )
            row = cursor.fetchone()
            if not row:
                raise PromotionNotFound("no exact backtest promotion was found")
            promotion = _promotion_from_row(row)
        finally:
            cursor.close()

    expected = {
        "state": required_state,
        "validation_profile": validation_profile,
        "engine_version": engine_version,
        "dataset_fingerprint": (
            dataset_fingerprint.lower() if dataset_fingerprint else None
        ),
    }
    mismatches = [
        field
        for field, expected_value in expected.items()
        if expected_value is not None and getattr(promotion, field) != expected_value
    ]
    if mismatches:
        raise PromotionEvidenceMismatch(
            "newest exact promotion does not satisfy required filters: "
            + ", ".join(sorted(mismatches)),
            metadata={
                "promotion_id": promotion.promotion_id,
                "current_state": promotion.state,
                "current_version": promotion.version,
            },
        )

    now = _utc_now()
    if promotion.expires_at is not None and promotion.expires_at <= now:
        raise PromotionExpired(f"promotion {promotion.promotion_id} is expired")
    if max_age_hours is not None and now - promotion.created_at > timedelta(
        hours=max_age_hours
    ):
        raise PromotionExpired(
            f"promotion evidence is older than requested max_age_hours={max_age_hours}"
        )
    return promotion
