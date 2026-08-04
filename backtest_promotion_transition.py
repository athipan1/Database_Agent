from __future__ import annotations

import time
from contextlib import nullcontext
from datetime import datetime, timedelta
from typing import Any, Optional

from backtest_promotion_models import (
    BacktestPromotionRecord,
    RevokeBacktestPromotionBody,
    TransitionBacktestPromotionBody,
)
from backtest_promotion_metrics import (
    PROMOTION_APPROVED,
    PROMOTION_DUPLICATE_TRANSITION,
    PROMOTION_EXPIRED,
    PROMOTION_REVOKED,
    PROMOTION_STALE_VERSION,
    PROMOTION_TRANSITIONS,
    PROMOTION_TRANSITION_DURATION,
    PROMOTION_TRANSITION_FAILURES,
)
from backtest_repository import setup_backtest_tables
from backtest_promotion_base import (
    ALLOWED_TRANSITIONS,
    APPROVED_STATES,
    STATE_TIMESTAMP_COLUMNS,
    TERMINAL_STATES,
    DuplicatePromotionTransition,
    InvalidPromotionTransition,
    PromotionDatabaseConflict,
    PromotionError,
    PromotionExpired,
    PromotionNotFound,
    PromotionTerminalState,
    StalePromotionVersion,
    _SQLITE_WRITE_LOCK,
    _db_time,
    _env_int,
    _json_dumps,
    _promotion_from_row,
    _promotion_from_transition_replay,
    _row_dict,
    _select_promotion,
    _select_transition,
    _utc_now,
    deterministic_transition_id,
    logger,
    setup_backtest_promotion_tables,
)
from backtest_promotion_store import get_backtest_promotion
from backtest_promotion_validation import _validate_transition_evidence


def _approved_for_paper_expiry(
    *,
    current_expires_at: Optional[datetime],
    approved_at: datetime,
) -> datetime:
    policy_expiry = approved_at + timedelta(
        hours=_env_int("BACKTEST_PROMOTION_MAX_AGE_HOURS", 168)
    )
    if current_expires_at is None:
        return policy_expiry
    return min(current_expires_at, policy_expiry)


def transition_backtest_promotion(
    db,
    promotion_id: str,
    body: TransitionBacktestPromotionBody,
    correlation_id: Optional[str],
) -> BacktestPromotionRecord:
    started_at = time.perf_counter()
    setup_backtest_tables(db)
    setup_backtest_promotion_tables(db)
    transition_id = deterministic_transition_id(
        promotion_id=promotion_id,
        expected_version=body.expected_version,
        expected_state=body.expected_state,
        next_state=body.next_state,
        evidence_run_id=body.evidence_run_id,
        reason_code=body.reason_code,
    )
    lock = _SQLITE_WRITE_LOCK if db.db_type == "sqlite" else nullcontext()
    with lock, db.connection_scope() as conn:
        cursor = db.get_cursor(conn)
        try:
            existing_transition = _select_transition(cursor, db, transition_id)
            if existing_transition:
                replay_record = _promotion_from_transition_replay(existing_transition)
                if replay_record is not None:
                    PROMOTION_DUPLICATE_TRANSITION.inc()
                    return replay_record
                row = _select_promotion(cursor, db, promotion_id)
                if not row:
                    raise PromotionDatabaseConflict(
                        "transition history exists without its promotion"
                    )
                current_record = _promotion_from_row(row)
                if current_record.version != int(
                    _row_dict(existing_transition).get("to_version") or 0
                ):
                    raise PromotionDatabaseConflict(
                        "legacy transition history cannot reproduce the original response"
                    )
                PROMOTION_DUPLICATE_TRANSITION.inc()
                return current_record.model_copy(update={"idempotent_replay": True})

            row = _select_promotion(cursor, db, promotion_id, lock=True)
            if not row:
                raise PromotionNotFound(f"promotion {promotion_id} was not found")
            current = _promotion_from_row(row)
            if current.state in TERMINAL_STATES:
                raise PromotionTerminalState(
                    f"promotion is terminal in state {current.state}"
                )
            if current.version != body.expected_version:
                raise StalePromotionVersion(
                    f"stale promotion version: expected current={current.version}, "
                    f"received={body.expected_version}",
                    metadata={"current_version": current.version},
                )
            if current.state != body.expected_state:
                raise InvalidPromotionTransition(
                    f"expected_state mismatch: current={current.state}, "
                    f"received={body.expected_state}"
                )
            if body.next_state not in ALLOWED_TRANSITIONS.get(current.state, set()):
                raise InvalidPromotionTransition(
                    f"transition {current.state} -> {body.next_state} is not allowed"
                )
            if current.expires_at is not None and current.expires_at <= _utc_now():
                allowed_expired_terminal = body.next_state in {
                    "FAILED",
                    "REJECTED",
                    "REVOKED",
                }
                approved_expiration = (
                    body.next_state == "EXPIRED" and current.state in APPROVED_STATES
                )
                if not (allowed_expired_terminal or approved_expiration):
                    raise PromotionExpired("promotion evidence is expired")
            _validate_transition_evidence(db, current, body)

            now = _utc_now()
            next_version = current.version + 1
            timestamp_column = STATE_TIMESTAMP_COLUMNS.get(body.next_state)
            assignments = [
                f"state = {db.param_style}",
                f"version = {db.param_style}",
                f"updated_at = {db.param_style}",
                f"reason_code = {db.param_style}",
                f"reason = {db.param_style}",
                f"correlation_id = {db.param_style}",
                f"metadata = {db.param_style}",
            ]
            params: list[Any] = [
                body.next_state,
                next_version,
                _db_time(db, now),
                body.reason_code,
                body.reason,
                correlation_id or body.correlation_id,
                _json_dumps(
                    {
                        **current.metadata,
                        **body.metadata,
                        "last_transition_id": transition_id,
                        "last_approver": body.approver,
                        "safe_for_trading": body.next_state in APPROVED_STATES,
                    }
                ),
            ]
            if timestamp_column:
                assignments.append(f"{timestamp_column} = {db.param_style}")
                params.append(_db_time(db, now))
            if body.next_state == "APPROVED_FOR_PAPER":
                assignments.append(f"expires_at = {db.param_style}")
                params.append(
                    _db_time(
                        db,
                        _approved_for_paper_expiry(
                            current_expires_at=current.expires_at,
                            approved_at=now,
                        ),
                    )
                )
            if body.next_state == "PAPER_OBSERVING":
                assignments.append(f"last_observed_at = {db.param_style}")
                params.append(_db_time(db, now))
            params.extend([promotion_id, current.state, current.version])
            cursor.execute(
                "UPDATE backtest_promotions SET "
                + ", ".join(assignments)
                + f" WHERE promotion_id = {db.param_style}"
                + f" AND state = {db.param_style}"
                + f" AND version = {db.param_style}",
                tuple(params),
            )
            if cursor.rowcount != 1:
                raise StalePromotionVersion(
                    "promotion changed concurrently before transition commit"
                )
            updated_row = _select_promotion(cursor, db, promotion_id)
            updated = _promotion_from_row(updated_row)
            cursor.execute(
                f"""
                INSERT INTO backtest_promotion_transitions (
                    transition_id, promotion_id, from_state, to_state,
                    from_version, to_version, status, reason_code, reason,
                    evidence_run_id, correlation_id, metadata, created_at
                ) VALUES ({', '.join([db.param_style] * 13)})
                """,
                (
                    transition_id,
                    promotion_id,
                    current.state,
                    body.next_state,
                    current.version,
                    next_version,
                    "COMPLETED",
                    body.reason_code,
                    body.reason,
                    body.evidence_run_id,
                    correlation_id or body.correlation_id,
                    _json_dumps(
                        {
                            **body.metadata,
                            "approver": body.approver,
                            "idempotency_model": "deterministic-transition-id-v1",
                            "result_snapshot": updated.model_dump(mode="json"),
                        }
                    ),
                    _db_time(db, now),
                ),
            )
            conn.commit()
            PROMOTION_TRANSITIONS.labels(
                from_state=current.state,
                to_state=updated.state,
            ).inc()
            if updated.state == "APPROVED_FOR_PAPER":
                PROMOTION_APPROVED.inc()
            elif updated.state == "REVOKED":
                PROMOTION_REVOKED.inc()
            elif updated.state == "EXPIRED":
                PROMOTION_EXPIRED.inc()
            logger.info(
                "promotion_transition_completed",
                extra={
                    "event": "promotion_transition_completed",
                    "from_state": current.state,
                    "to_state": updated.state,
                    "reason_code": body.reason_code,
                },
            )
            return updated
        except PromotionError as exc:
            conn.rollback()
            PROMOTION_TRANSITION_FAILURES.labels(error_code=exc.code).inc()
            if isinstance(exc, StalePromotionVersion):
                PROMOTION_STALE_VERSION.inc()
            if isinstance(exc, PromotionExpired):
                PROMOTION_EXPIRED.inc()
            logger.warning(
                "promotion_transition_rejected",
                extra={
                    "event": "promotion_transition_rejected",
                    "next_state": body.next_state,
                    "reason_code": body.reason_code,
                },
            )
            raise
        except Exception as exc:
            conn.rollback()
            message = str(exc).lower()
            if "unique" in message or "duplicate" in message:
                replay = _select_transition(cursor, db, transition_id)
                if replay:
                    replay_record = _promotion_from_transition_replay(replay)
                    if replay_record is not None:
                        PROMOTION_DUPLICATE_TRANSITION.inc()
                        return replay_record
                    row = _select_promotion(cursor, db, promotion_id)
                    if row:
                        PROMOTION_DUPLICATE_TRANSITION.inc()
                        return _promotion_from_row(row, replay=True)
                raise DuplicatePromotionTransition(
                    "promotion transition conflicts with an existing transition"
                ) from exc
            PROMOTION_TRANSITION_FAILURES.labels(
                error_code="database_conflict"
            ).inc()
            raise
        finally:
            PROMOTION_TRANSITION_DURATION.labels(to_state=body.next_state).observe(
                time.perf_counter() - started_at
            )
            cursor.close()


def revoke_backtest_promotion(
    db,
    promotion_id: str,
    body: RevokeBacktestPromotionBody,
    correlation_id: Optional[str],
) -> BacktestPromotionRecord:
    current = get_backtest_promotion(db, promotion_id)
    if current.state == "REVOKED":
        setup_backtest_promotion_tables(db)
        with db.connection_scope() as conn:
            cursor = db.get_cursor(conn)
            try:
                for expected_state in sorted(APPROVED_STATES):
                    transition_id = deterministic_transition_id(
                        promotion_id=promotion_id,
                        expected_version=body.expected_version,
                        expected_state=expected_state,
                        next_state="REVOKED",
                        evidence_run_id=current.run_id,
                        reason_code=body.reason_code,
                    )
                    row = _select_transition(cursor, db, transition_id)
                    if row:
                        replay = _promotion_from_transition_replay(row)
                        if replay is not None:
                            PROMOTION_DUPLICATE_TRANSITION.inc()
                            return replay
                raise DuplicatePromotionTransition(
                    "promotion is already revoked by a different transition"
                )
            finally:
                cursor.close()
    if current.state not in APPROVED_STATES:
        raise InvalidPromotionTransition(
            "only approved or observing promotions can be revoked"
        )
    transition = TransitionBacktestPromotionBody(
        expected_state=current.state,
        expected_version=body.expected_version,
        next_state="REVOKED",
        reason_code=body.reason_code,
        reason=body.reason,
        evidence_run_id=current.run_id,
        correlation_id=body.correlation_id,
        evidence_version=current.evidence_version,
        approver=body.approver,
        metadata={"revocation": True},
    )
    return transition_backtest_promotion(db, promotion_id, transition, correlation_id)


__all__ = ["transition_backtest_promotion", "revoke_backtest_promotion"]
