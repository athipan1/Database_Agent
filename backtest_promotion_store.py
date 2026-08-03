from __future__ import annotations

from contextlib import nullcontext
from datetime import timedelta, timezone
from typing import List, Optional

from backtest_promotion_models import (
    BacktestPromotionRecord,
    BacktestPromotionTransitionRecord,
    CreateBacktestPromotionBody,
)
from backtest_promotion_metrics import PROMOTIONS_CREATED
from backtest_repository import setup_backtest_tables
from backtest_promotion_base import (
    PromotionDatabaseConflict,
    PromotionError,
    PromotionEvidenceMismatch,
    PromotionExpired,
    PromotionNotFound,
    _SQLITE_WRITE_LOCK,
    _db_time,
    _env_int,
    _json_dumps,
    _promotion_from_row,
    _promotion_id,
    _select_by_account_run,
    _select_promotion,
    _transition_from_row,
    _utc_now,
    logger,
    setup_backtest_promotion_tables,
)
from backtest_promotion_validation import _load_exact_evidence


def create_backtest_promotion(
    db,
    body: CreateBacktestPromotionBody,
    correlation_id: Optional[str],
) -> BacktestPromotionRecord:
    setup_backtest_tables(db)
    setup_backtest_promotion_tables(db)
    promotion_id = _promotion_id(body.account_id, body.run_id)
    lock = _SQLITE_WRITE_LOCK if db.db_type == "sqlite" else nullcontext()
    with lock, db.connection_scope() as conn:
        cursor = db.get_cursor(conn)
        try:
            existing = _select_by_account_run(cursor, db, body.account_id, body.run_id)
            if existing:
                record = _promotion_from_row(existing, replay=True)
                expected = {
                    "skill_id": body.skill_id,
                    "strategy_id": body.strategy_id,
                    "symbol": body.symbol,
                    "timeframe": body.timeframe,
                    "dataset_fingerprint": body.dataset_fingerprint,
                    "engine_version": body.engine_version,
                    "validation_profile": body.validation_profile,
                }
                conflicts = [
                    field
                    for field, expected_value in expected.items()
                    if getattr(record, field) != expected_value
                ]
                if conflicts:
                    raise PromotionEvidenceMismatch(
                        "promotion retry conflicts with stored identity fields: "
                        + ", ".join(conflicts)
                    )
                return record

            now = _utc_now()
            provisional = BacktestPromotionRecord(
                promotion_id=promotion_id,
                account_id=body.account_id,
                run_id=body.run_id,
                skill_id=body.skill_id,
                strategy_id=body.strategy_id,
                symbol=body.symbol,
                timeframe=body.timeframe,
                dataset_fingerprint=body.dataset_fingerprint,
                engine_version=body.engine_version,
                validation_profile=body.validation_profile,
                state="GENERATED",
                version=1,
                evidence_version=body.evidence_version,
                created_at=now,
                updated_at=now,
                expires_at=body.expires_at,
                reason_code=body.reason_code,
                reason=body.reason,
                correlation_id=correlation_id or body.correlation_id,
                metadata=body.metadata,
            )
            detail, _ = _load_exact_evidence(db, provisional)
            expires_at = body.expires_at
            if expires_at is None:
                evidence_time = detail.run.updated_at or detail.run.created_at or now
                expires_at = evidence_time.astimezone(timezone.utc) + timedelta(
                    hours=_env_int("BACKTEST_PROMOTION_EVIDENCE_MAX_AGE_HOURS", 168)
                )
            if expires_at <= now:
                raise PromotionExpired("promotion evidence is already expired")
            merged_metadata = {
                **body.metadata,
                "source": "database-agent-promotion-lifecycle",
                "safe_for_trading": False,
                "evidence_updated_at": (
                    detail.run.updated_at or detail.run.created_at
                ).isoformat(),
            }
            cursor.execute(
                f"""
                INSERT INTO backtest_promotions (
                    promotion_id, account_id, run_id, skill_id, strategy_id,
                    symbol, timeframe, dataset_fingerprint, engine_version,
                    validation_profile, state, version, evidence_version,
                    created_at, updated_at, expires_at, reason_code, reason,
                    correlation_id, metadata
                ) VALUES ({', '.join([db.param_style] * 20)})
                """,
                (
                    promotion_id,
                    body.account_id,
                    body.run_id,
                    body.skill_id,
                    body.strategy_id,
                    body.symbol,
                    body.timeframe,
                    body.dataset_fingerprint,
                    body.engine_version,
                    body.validation_profile,
                    "GENERATED",
                    1,
                    body.evidence_version,
                    _db_time(db, now),
                    _db_time(db, now),
                    _db_time(db, expires_at),
                    body.reason_code,
                    body.reason,
                    correlation_id or body.correlation_id,
                    _json_dumps(merged_metadata),
                ),
            )
            row = _select_promotion(cursor, db, promotion_id)
            conn.commit()
            record = _promotion_from_row(row)
            PROMOTIONS_CREATED.inc()
            logger.info(
                "promotion_created",
                extra={
                    "event": "promotion_created",
                    "state": record.state,
                    "validation_profile": record.validation_profile,
                },
            )
            return record
        except PromotionError:
            conn.rollback()
            raise
        except Exception as exc:
            conn.rollback()
            message = str(exc).lower()
            if "unique" in message or "duplicate" in message:
                raise PromotionDatabaseConflict(
                    "promotion identity conflicts with an existing record"
                ) from exc
            raise
        finally:
            cursor.close()


def get_backtest_promotion(db, promotion_id: str) -> BacktestPromotionRecord:
    setup_backtest_promotion_tables(db)
    with db.connection_scope() as conn:
        cursor = db.get_cursor(conn)
        try:
            row = _select_promotion(cursor, db, promotion_id)
            if not row:
                raise PromotionNotFound(f"promotion {promotion_id} was not found")
            return _promotion_from_row(row)
        finally:
            cursor.close()


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
                ORDER BY updated_at DESC, created_at DESC, promotion_id DESC
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
    if max_age_hours is not None and now - promotion.updated_at > timedelta(
        hours=max_age_hours
    ):
        raise PromotionExpired(
            f"promotion is older than requested max_age_hours={max_age_hours}"
        )
    return promotion


def list_backtest_promotion_history(
    db,
    promotion_id: str,
) -> List[BacktestPromotionTransitionRecord]:
    setup_backtest_promotion_tables(db)
    get_backtest_promotion(db, promotion_id)
    with db.connection_scope() as conn:
        cursor = db.get_cursor(conn)
        try:
            cursor.execute(
                f"SELECT * FROM backtest_promotion_transitions "
                f"WHERE promotion_id = {db.param_style} "
                "ORDER BY from_version ASC, created_at ASC",
                (promotion_id,),
            )
            return [_transition_from_row(row) for row in (cursor.fetchall() or [])]
        finally:
            cursor.close()


__all__ = [
    "create_backtest_promotion",
    "get_backtest_promotion",
    "get_latest_exact_backtest_promotion",
    "list_backtest_promotion_history",
]
