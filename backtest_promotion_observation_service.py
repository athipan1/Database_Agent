from __future__ import annotations

import hashlib
import os
from contextlib import nullcontext
from datetime import timedelta, timezone
from typing import Any, Dict, Optional, cast

from backtest_promotion_base import (
    PromotionDatabaseConflict,
    PromotionTerminalState,
    PromotionValidationFailed,
    StalePromotionVersion,
    _SQLITE_WRITE_LOCK,
    _assert_finite_json,
    _db_time,
    _json_dumps,
    _json_loads,
    _parse_datetime,
    _utc_now,
)
from backtest_promotion_models import (
    PromotionState,
    RevokeBacktestPromotionBody,
    TransitionBacktestPromotionBody,
)
from backtest_promotion_observation_models import (
    BacktestPromotionObservationRecord,
    ObservationAction,
    ObserveBacktestPromotionBody,
    ObservedPromotionState,
)
from backtest_promotion_repository import (
    get_backtest_promotion,
    revoke_backtest_promotion,
    transition_backtest_promotion,
)


OBSERVABLE_STATES = {"APPROVED_FOR_PAPER", "PAPER_OBSERVING"}


def _max_drawdown_pct() -> float:
    try:
        value = float(
            os.getenv("BACKTEST_PROMOTION_MAX_PAPER_DRAWDOWN_PCT", "0.10")
        )
    except (TypeError, ValueError):
        return 0.10
    if value <= 0 or value > 1:
        return 0.10
    return value


def deterministic_observation_id(promotion_id: str, observation_key: str) -> str:
    digest = hashlib.sha256(
        f"{promotion_id}\x1f{observation_key}".encode("utf-8")
    ).hexdigest()
    return f"promotion-observation-{digest}"


def setup_backtest_promotion_observation_tables(db) -> None:
    timestamp_type = "TEXT" if db.db_type == "sqlite" else "TIMESTAMPTZ"
    json_type = "TEXT" if db.db_type == "sqlite" else "JSONB"
    with db.connection_scope() as conn:
        cursor = db.get_cursor(conn)
        try:
            cursor.execute(
                f"""
                CREATE TABLE IF NOT EXISTS backtest_promotion_observations (
                    observation_id TEXT PRIMARY KEY,
                    promotion_id TEXT NOT NULL,
                    observation_key TEXT NOT NULL,
                    action TEXT NOT NULL CHECK (
                        action IN (
                            'START_OBSERVING',
                            'HEARTBEAT',
                            'EXPIRE',
                            'REVOKE'
                        )
                    ),
                    reason_code TEXT NOT NULL,
                    from_state TEXT NOT NULL,
                    to_state TEXT NOT NULL,
                    from_version INTEGER NOT NULL,
                    to_version INTEGER NOT NULL,
                    observed_at {timestamp_type} NOT NULL,
                    created_at {timestamp_type} NOT NULL,
                    correlation_id TEXT,
                    paper_drawdown_pct DOUBLE PRECISION NOT NULL,
                    reconciliation_ok BOOLEAN NOT NULL,
                    duplicate_order_count INTEGER NOT NULL,
                    broker_order_count INTEGER NOT NULL,
                    database_order_count INTEGER NOT NULL,
                    filled_order_count INTEGER NOT NULL,
                    strategy_drift BOOLEAN NOT NULL,
                    emergency_halt BOOLEAN NOT NULL,
                    metadata {json_type} NOT NULL,
                    result_snapshot {json_type} NOT NULL,
                    UNIQUE (promotion_id, observation_key),
                    FOREIGN KEY (promotion_id)
                        REFERENCES backtest_promotions(promotion_id)
                        ON DELETE CASCADE
                )
                """
            )
            cursor.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_promotion_observations_timeline
                ON backtest_promotion_observations (
                    promotion_id, observed_at DESC, created_at DESC
                )
                """
            )
            conn.commit()
        finally:
            cursor.close()


def _row_snapshot(
    row: Any,
    *,
    replay: Optional[bool] = None,
) -> Optional[BacktestPromotionObservationRecord]:
    if not row:
        return None
    mapping = dict(row) if not isinstance(row, dict) else row
    snapshot = _json_loads(mapping.get("result_snapshot"))
    if not snapshot:
        return None
    snapshot["observed_at"] = _parse_datetime(snapshot.get("observed_at"))
    snapshot["created_at"] = _parse_datetime(snapshot.get("created_at"))
    if replay is not None:
        snapshot["idempotent_replay"] = replay
    return BacktestPromotionObservationRecord.model_validate(snapshot)


def _select_observation(cursor, db, observation_id: str):
    cursor.execute(  # nosec B608 - only trusted adapter placeholder is interpolated
        f"""
        SELECT * FROM backtest_promotion_observations
        WHERE observation_id = {db.param_style}
        """,
        (observation_id,),
    )
    return cursor.fetchone()


def get_backtest_promotion_observation(
    db,
    observation_id: str,
) -> Optional[BacktestPromotionObservationRecord]:
    setup_backtest_promotion_observation_tables(db)
    with db.connection_scope() as conn:
        cursor = db.get_cursor(conn)
        try:
            return _row_snapshot(
                _select_observation(cursor, db, observation_id),
                replay=True,
            )
        finally:
            cursor.close()


def list_backtest_promotion_observations(
    db,
    promotion_id: str,
) -> list[BacktestPromotionObservationRecord]:
    setup_backtest_promotion_observation_tables(db)
    with db.connection_scope() as conn:
        cursor = db.get_cursor(conn)
        try:
            cursor.execute(  # nosec B608 - trusted adapter placeholder only
                f"""
                SELECT * FROM backtest_promotion_observations
                WHERE promotion_id = {db.param_style}
                ORDER BY observed_at ASC, created_at ASC, observation_id ASC
                """,
                (promotion_id,),
            )
            records = []
            for row in cursor.fetchall():
                record = _row_snapshot(row)
                if record is not None:
                    records.append(record)
            return records
        finally:
            cursor.close()


def _observation_reason(
    body: ObserveBacktestPromotionBody,
    expires_at: Any,
) -> tuple[ObservationAction, str, str]:
    if body.emergency_halt:
        return (
            "REVOKE",
            "emergency_halt",
            "Emergency halt was active during paper observation.",
        )
    if body.duplicate_order_count > 0:
        return (
            "REVOKE",
            "duplicate_order_detected",
            "Duplicate paper orders were detected during reconciliation.",
        )
    if (
        not body.reconciliation_ok
        or body.broker_order_count != body.database_order_count
        or body.filled_order_count > body.broker_order_count
    ):
        return (
            "REVOKE",
            "broker_reconciliation_failed",
            "Paper broker and Database order state did not reconcile exactly.",
        )
    if body.strategy_drift:
        return (
            "REVOKE",
            "strategy_drift",
            "Observed strategy behavior drifted from approved evidence.",
        )
    if body.paper_drawdown_pct > _max_drawdown_pct():
        return (
            "REVOKE",
            "paper_drawdown_exceeded",
            "Observed paper drawdown exceeded the configured promotion limit.",
        )
    parsed_expiry = _parse_datetime(expires_at)
    observed_at = body.observed_at.astimezone(timezone.utc)
    if parsed_expiry is not None and observed_at >= parsed_expiry:
        return (
            "EXPIRE",
            "promotion_expired",
            "Promotion evidence expired during paper observation.",
        )
    if body.expected_state == "APPROVED_FOR_PAPER":
        return (
            "START_OBSERVING",
            "paper_observation_started",
            "First reconciled paper observation started the observation window.",
        )
    return (
        "HEARTBEAT",
        "paper_observation_heartbeat",
        "Paper observation heartbeat reconciled successfully.",
    )


def _record(
    *,
    observation_id: str,
    promotion_id: str,
    body: ObserveBacktestPromotionBody,
    action: ObservationAction,
    reason_code: str,
    from_state: ObservedPromotionState,
    from_version: int,
    promotion: Dict[str, Any],
    correlation_id: Optional[str],
    replay: bool = False,
) -> BacktestPromotionObservationRecord:
    return BacktestPromotionObservationRecord(
        observation_id=observation_id,
        promotion_id=promotion_id,
        observation_key=body.observation_key,
        action=action,
        reason_code=reason_code,
        from_state=from_state,
        to_state=str(promotion["state"]),
        from_version=from_version,
        to_version=int(promotion["version"]),
        observed_at=body.observed_at,
        created_at=_utc_now(),
        correlation_id=correlation_id,
        paper_drawdown_pct=body.paper_drawdown_pct,
        reconciliation_ok=body.reconciliation_ok,
        duplicate_order_count=body.duplicate_order_count,
        broker_order_count=body.broker_order_count,
        database_order_count=body.database_order_count,
        filled_order_count=body.filled_order_count,
        strategy_drift=body.strategy_drift,
        emergency_halt=body.emergency_halt,
        metadata={
            **body.metadata,
            "notes": body.notes,
            "max_paper_drawdown_pct": _max_drawdown_pct(),
        },
        promotion=promotion,
        idempotent_replay=replay,
    )


def _persist_observation(
    db,
    record: BacktestPromotionObservationRecord,
) -> BacktestPromotionObservationRecord:
    snapshot = record.model_dump(mode="json")
    with db.connection_scope() as conn:
        cursor = db.get_cursor(conn)
        try:
            cursor.execute(  # nosec B608 - placeholders come from DB adapter only
                f"""
                INSERT INTO backtest_promotion_observations (
                    observation_id, promotion_id, observation_key, action,
                    reason_code, from_state, to_state, from_version, to_version,
                    observed_at, created_at, correlation_id, paper_drawdown_pct,
                    reconciliation_ok, duplicate_order_count, broker_order_count,
                    database_order_count, filled_order_count, strategy_drift,
                    emergency_halt, metadata, result_snapshot
                ) VALUES ({', '.join([db.param_style] * 22)})
                ON CONFLICT (observation_id) DO NOTHING
                """,
                (
                    record.observation_id,
                    record.promotion_id,
                    record.observation_key,
                    record.action,
                    record.reason_code,
                    record.from_state,
                    record.to_state,
                    record.from_version,
                    record.to_version,
                    _db_time(db, record.observed_at),
                    _db_time(db, record.created_at),
                    record.correlation_id,
                    record.paper_drawdown_pct,
                    record.reconciliation_ok,
                    record.duplicate_order_count,
                    record.broker_order_count,
                    record.database_order_count,
                    record.filled_order_count,
                    record.strategy_drift,
                    record.emergency_halt,
                    _json_dumps(record.metadata),
                    _json_dumps(snapshot),
                ),
            )
            inserted = cursor.rowcount == 1
            conn.commit()
            if inserted:
                return record
            replay = _row_snapshot(
                _select_observation(cursor, db, record.observation_id),
                replay=True,
            )
            if replay is not None:
                return replay
            raise PromotionDatabaseConflict(
                "observation id conflicted without a replayable ledger record"
            )
        except Exception:
            conn.rollback()
            raise
        finally:
            cursor.close()


def _heartbeat(
    db,
    promotion,
    body: ObserveBacktestPromotionBody,
    observation_id: str,
    correlation_id: Optional[str],
) -> BacktestPromotionObservationRecord:
    lock = _SQLITE_WRITE_LOCK if db.db_type == "sqlite" else nullcontext()
    with lock, db.connection_scope() as conn:
        cursor = db.get_cursor(conn)
        try:
            replay = _row_snapshot(
                _select_observation(cursor, db, observation_id),
                replay=True,
            )
            if replay is not None:
                return replay

            now = _utc_now()
            metadata = {
                **promotion.metadata,
                "last_observation_id": observation_id,
                "last_observation_key": body.observation_key,
                "last_reconciliation_ok": body.reconciliation_ok,
                "last_paper_drawdown_pct": body.paper_drawdown_pct,
            }
            updated = promotion.model_copy(
                update={
                    "version": promotion.version + 1,
                    "updated_at": now,
                    "last_observed_at": body.observed_at,
                    "reason_code": "paper_observation_heartbeat",
                    "reason": (
                        "Paper observation heartbeat reconciled successfully."
                    ),
                    "correlation_id": correlation_id,
                    "metadata": metadata,
                    "idempotent_replay": False,
                }
            )
            record = _record(
                observation_id=observation_id,
                promotion_id=promotion.promotion_id,
                body=body,
                action="HEARTBEAT",
                reason_code="paper_observation_heartbeat",
                from_state=cast(ObservedPromotionState, promotion.state),
                from_version=promotion.version,
                promotion=updated.model_dump(mode="json"),
                correlation_id=correlation_id,
            )
            snapshot = record.model_dump(mode="json")
            cursor.execute(  # nosec B608 - values remain bound parameters
                f"""
                UPDATE backtest_promotions
                SET version = {db.param_style},
                    updated_at = {db.param_style},
                    last_observed_at = {db.param_style},
                    reason_code = {db.param_style},
                    reason = {db.param_style},
                    correlation_id = {db.param_style},
                    metadata = {db.param_style}
                WHERE promotion_id = {db.param_style}
                  AND state = {db.param_style}
                  AND version = {db.param_style}
                """,
                (
                    updated.version,
                    _db_time(db, updated.updated_at),
                    _db_time(db, updated.last_observed_at),
                    updated.reason_code,
                    updated.reason,
                    correlation_id,
                    _json_dumps(metadata),
                    promotion.promotion_id,
                    promotion.state,
                    promotion.version,
                ),
            )
            if cursor.rowcount != 1:
                conn.rollback()
                replay = _row_snapshot(
                    _select_observation(cursor, db, observation_id),
                    replay=True,
                )
                if replay is not None:
                    return replay
                raise StalePromotionVersion(
                    "paper observation heartbeat lost optimistic concurrency race"
                )
            cursor.execute(  # nosec B608 - placeholders are adapter constants
                f"""
                INSERT INTO backtest_promotion_observations (
                    observation_id, promotion_id, observation_key, action,
                    reason_code, from_state, to_state, from_version, to_version,
                    observed_at, created_at, correlation_id, paper_drawdown_pct,
                    reconciliation_ok, duplicate_order_count, broker_order_count,
                    database_order_count, filled_order_count, strategy_drift,
                    emergency_halt, metadata, result_snapshot
                ) VALUES ({', '.join([db.param_style] * 22)})
                """,
                (
                    record.observation_id,
                    record.promotion_id,
                    record.observation_key,
                    record.action,
                    record.reason_code,
                    record.from_state,
                    record.to_state,
                    record.from_version,
                    record.to_version,
                    _db_time(db, record.observed_at),
                    _db_time(db, record.created_at),
                    record.correlation_id,
                    record.paper_drawdown_pct,
                    record.reconciliation_ok,
                    record.duplicate_order_count,
                    record.broker_order_count,
                    record.database_order_count,
                    record.filled_order_count,
                    record.strategy_drift,
                    record.emergency_halt,
                    _json_dumps(record.metadata),
                    _json_dumps(snapshot),
                ),
            )
            conn.commit()
            return record
        except Exception:
            conn.rollback()
            replay = _row_snapshot(
                _select_observation(cursor, db, observation_id),
                replay=True,
            )
            if replay is not None:
                return replay
            raise
        finally:
            cursor.close()


def _validate_current_for_heartbeat(promotion, body: ObserveBacktestPromotionBody) -> None:
    if promotion.state not in OBSERVABLE_STATES:
        raise PromotionTerminalState(
            f"promotion state {promotion.state} cannot be paper-observed"
        )
    if (
        promotion.state != body.expected_state
        or promotion.version != body.expected_version
    ):
        raise StalePromotionVersion(
            "paper observation expected state/version does not match current promotion",
            metadata={
                "current_state": promotion.state,
                "current_version": promotion.version,
            },
        )


def _validate_transition_or_replay(
    promotion,
    body: ObserveBacktestPromotionBody,
    action: ObservationAction,
) -> None:
    if (
        promotion.state == body.expected_state
        and promotion.version == body.expected_version
    ):
        return
    target_state: PromotionState
    if action == "START_OBSERVING":
        target_state = "PAPER_OBSERVING"
    elif action == "EXPIRE":
        target_state = "EXPIRED"
    else:
        target_state = "REVOKED"
    if (
        promotion.state == target_state
        and promotion.version == body.expected_version + 1
    ):
        return
    if promotion.state not in OBSERVABLE_STATES:
        raise PromotionTerminalState(
            f"promotion state {promotion.state} cannot be paper-observed"
        )
    raise StalePromotionVersion(
        "paper observation expected state/version does not match current promotion",
        metadata={
            "current_state": promotion.state,
            "current_version": promotion.version,
        },
    )


def observe_backtest_promotion(
    db,
    promotion_id: str,
    body: ObserveBacktestPromotionBody,
    correlation_id: Optional[str] = None,
) -> BacktestPromotionObservationRecord:
    setup_backtest_promotion_observation_tables(db)
    _assert_finite_json(body.metadata, path="observation.metadata")
    observation_id = deterministic_observation_id(
        promotion_id,
        body.observation_key,
    )
    existing = get_backtest_promotion_observation(db, observation_id)
    if existing is not None:
        return existing

    now = _utc_now()
    observed_at = body.observed_at.astimezone(timezone.utc)
    if observed_at > now + timedelta(minutes=5):
        raise PromotionValidationFailed("observed_at is too far in the future")

    promotion = get_backtest_promotion(db, promotion_id)
    action, reason_code, reason = _observation_reason(body, promotion.expires_at)
    effective_correlation_id = correlation_id or body.correlation_id

    if action == "HEARTBEAT":
        try:
            _validate_current_for_heartbeat(promotion, body)
        except (PromotionTerminalState, StalePromotionVersion):
            replay = get_backtest_promotion_observation(db, observation_id)
            if replay is not None:
                return replay
            raise
        return _heartbeat(
            db,
            promotion,
            body,
            observation_id,
            effective_correlation_id,
        )

    _validate_transition_or_replay(promotion, body, action)
    if action == "REVOKE":
        updated = revoke_backtest_promotion(
            db,
            promotion_id,
            RevokeBacktestPromotionBody(
                expected_version=body.expected_version,
                reason_code=cast(Any, reason_code),
                reason=reason,
                correlation_id=effective_correlation_id,
                approver="paper-observation-reconciler",
            ),
            effective_correlation_id,
        )
    else:
        next_state: PromotionState = (
            "EXPIRED" if action == "EXPIRE" else "PAPER_OBSERVING"
        )
        updated = transition_backtest_promotion(
            db,
            promotion_id,
            TransitionBacktestPromotionBody(
                expected_state=body.expected_state,
                expected_version=body.expected_version,
                next_state=next_state,
                reason_code=reason_code,
                reason=reason,
                evidence_run_id=promotion.run_id,
                correlation_id=effective_correlation_id,
                evidence_version=promotion.evidence_version,
                approver="paper-observation-reconciler",
                metadata={
                    **body.metadata,
                    "observation_id": observation_id,
                    "observation_key": body.observation_key,
                    "paper_drawdown_pct": body.paper_drawdown_pct,
                    "reconciliation_ok": body.reconciliation_ok,
                },
            ),
            effective_correlation_id,
        )

    record = _record(
        observation_id=observation_id,
        promotion_id=promotion_id,
        body=body,
        action=action,
        reason_code=reason_code,
        from_state=body.expected_state,
        from_version=body.expected_version,
        promotion=updated.model_dump(mode="json"),
        correlation_id=effective_correlation_id,
        replay=updated.idempotent_replay,
    )
    return _persist_observation(db, record)
