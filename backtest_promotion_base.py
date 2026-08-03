from __future__ import annotations

import hashlib
import json
import logging
import math
import os
import threading
import time
from contextlib import nullcontext
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional

from backtest_promotion_models import (
    BacktestPromotionRecord,
    BacktestPromotionTransitionRecord,
    CreateBacktestPromotionBody,
    RevokeBacktestPromotionBody,
    TransitionBacktestPromotionBody,
)
from backtest_promotion_metrics import (
    PROMOTIONS_CREATED,
    PROMOTION_APPROVED,
    PROMOTION_DUPLICATE_TRANSITION,
    PROMOTION_EXPIRED,
    PROMOTION_REVOKED,
    PROMOTION_STALE_VERSION,
    PROMOTION_TRANSITIONS,
    PROMOTION_TRANSITION_DURATION,
    PROMOTION_TRANSITION_FAILURES,
)
from backtest_repository import get_backtest_run_detail, setup_backtest_tables


logger = logging.getLogger(__name__)
_SQLITE_WRITE_LOCK = threading.RLock()

TERMINAL_STATES = {"REJECTED", "FAILED", "EXPIRED", "REVOKED"}
APPROVED_STATES = {"APPROVED_FOR_PAPER", "PAPER_OBSERVING"}
ALLOWED_TRANSITIONS: dict[str, set[str]] = {
    "GENERATED": {"VALIDATED", "REJECTED", "FAILED"},
    "VALIDATED": {"OOS_PASSED", "REJECTED", "FAILED"},
    "OOS_PASSED": {"ROBUSTNESS_PASSED", "REJECTED", "FAILED"},
    "ROBUSTNESS_PASSED": {"APPROVED_FOR_PAPER", "REJECTED", "FAILED"},
    "APPROVED_FOR_PAPER": {"PAPER_OBSERVING", "REVOKED", "FAILED", "EXPIRED"},
    "PAPER_OBSERVING": {"REVOKED", "FAILED", "EXPIRED"},
}

STATE_TIMESTAMP_COLUMNS = {
    "VALIDATED": "validated_at",
    "OOS_PASSED": "oos_passed_at",
    "ROBUSTNESS_PASSED": "robustness_passed_at",
    "APPROVED_FOR_PAPER": "approved_for_paper_at",
    "PAPER_OBSERVING": "paper_observing_at",
    "REVOKED": "revoked_at",
    "REJECTED": "rejected_at",
    "FAILED": "failed_at",
}


class PromotionError(RuntimeError):
    code = "database_conflict"
    http_status = 409

    def __init__(self, message: str, *, metadata: Optional[dict] = None):
        super().__init__(message)
        self.metadata = metadata or {}


class PromotionNotFound(PromotionError):
    code = "promotion_not_found"
    http_status = 404


class InvalidPromotionTransition(PromotionError):
    code = "invalid_transition"


class StalePromotionVersion(PromotionError):
    code = "stale_version"


class DuplicatePromotionTransition(PromotionError):
    code = "duplicate_transition"


class PromotionEvidenceMismatch(PromotionError):
    code = "evidence_mismatch"


class PromotionExpired(PromotionError):
    code = "promotion_expired"


class PromotionTerminalState(PromotionError):
    code = "terminal_state"


class PromotionValidationFailed(PromotionError):
    code = "validation_failed"
    http_status = 422


class PromotionDatabaseConflict(PromotionError):
    code = "database_conflict"


class PromotionApprovalRequired(PromotionError):
    code = "approval_required"
    http_status = 403


def _env_int(name: str, default: int, *, minimum: int = 1) -> int:
    try:
        value = int(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return default
    return max(minimum, value)


def _env_float(name: str, default: float, *, minimum: float = 0.0) -> float:
    try:
        value = float(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return default
    if not math.isfinite(value):
        return default
    return max(minimum, value)


def _env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _db_time(db, value: Optional[datetime] = None) -> Any:
    normalized = value or _utc_now()
    return normalized.isoformat() if db.db_type == "sqlite" else normalized


def _parse_datetime(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        parsed = value
    else:
        try:
            parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except (TypeError, ValueError):
            return None
    if parsed.tzinfo is None:
        return None
    return parsed.astimezone(timezone.utc)


def _row_dict(row: Any) -> Dict[str, Any]:
    return dict(row) if row else {}


def _json_dumps(value: Any) -> str:
    return json.dumps(value or {}, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _json_loads(value: Any) -> dict:
    if isinstance(value, dict):
        return value
    if not value:
        return {}
    try:
        parsed = json.loads(value)
    except (TypeError, ValueError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _assert_finite_json(value: Any, *, path: str = "evidence") -> None:
    if value is None or isinstance(value, (str, bool, int)):
        return
    if isinstance(value, float):
        if not math.isfinite(value):
            raise PromotionValidationFailed(f"{path} contains NaN or Infinity")
        return
    if isinstance(value, list):
        for index, item in enumerate(value):
            _assert_finite_json(item, path=f"{path}[{index}]")
        return
    if isinstance(value, dict):
        for key, item in value.items():
            if not isinstance(key, str):
                raise PromotionValidationFailed(f"{path} contains a non-string key")
            _assert_finite_json(item, path=f"{path}.{key}")
        return
    raise PromotionValidationFailed(
        f"{path} contains unsupported value type {type(value).__name__}"
    )


def _promotion_id(account_id: str, run_id: str) -> str:
    digest = hashlib.sha256(f"{account_id}\x1f{run_id}".encode("utf-8")).hexdigest()
    return f"promotion-{digest[:32]}"


def deterministic_transition_id(
    *,
    promotion_id: str,
    expected_version: int,
    expected_state: str,
    next_state: str,
    evidence_run_id: str,
    reason_code: str,
) -> str:
    identity = "\x1f".join(
        [
            promotion_id,
            str(expected_version),
            expected_state,
            next_state,
            evidence_run_id,
            reason_code,
        ]
    )
    return f"promotion-transition-{hashlib.sha256(identity.encode('utf-8')).hexdigest()}"


def setup_backtest_promotion_tables(db) -> None:
    timestamp_type = "TEXT" if db.db_type == "sqlite" else "TIMESTAMPTZ"
    json_type = "TEXT" if db.db_type == "sqlite" else "JSONB"
    promotion_states = ", ".join(
        f"'{state}'"
        for state in [
            "GENERATED",
            "VALIDATED",
            "OOS_PASSED",
            "ROBUSTNESS_PASSED",
            "APPROVED_FOR_PAPER",
            "PAPER_OBSERVING",
            "REJECTED",
            "FAILED",
            "EXPIRED",
            "REVOKED",
        ]
    )
    with db.connection_scope() as conn:
        cursor = db.get_cursor(conn)
        try:
            cursor.execute(
                f"""
                CREATE TABLE IF NOT EXISTS backtest_promotions (
                    promotion_id TEXT PRIMARY KEY,
                    account_id TEXT NOT NULL,
                    run_id TEXT NOT NULL,
                    skill_id TEXT NOT NULL,
                    strategy_id TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    timeframe TEXT NOT NULL,
                    dataset_fingerprint TEXT NOT NULL,
                    engine_version TEXT NOT NULL,
                    validation_profile TEXT NOT NULL,
                    state TEXT NOT NULL CHECK (state IN ({promotion_states})),
                    version INTEGER NOT NULL DEFAULT 1 CHECK (version >= 1),
                    evidence_version INTEGER NOT NULL DEFAULT 1 CHECK (evidence_version >= 1),
                    created_at {timestamp_type} NOT NULL,
                    updated_at {timestamp_type} NOT NULL,
                    validated_at {timestamp_type},
                    oos_passed_at {timestamp_type},
                    robustness_passed_at {timestamp_type},
                    approved_for_paper_at {timestamp_type},
                    paper_observing_at {timestamp_type},
                    expires_at {timestamp_type},
                    revoked_at {timestamp_type},
                    rejected_at {timestamp_type},
                    failed_at {timestamp_type},
                    last_observed_at {timestamp_type},
                    reason_code TEXT,
                    reason TEXT,
                    correlation_id TEXT,
                    metadata {json_type} NOT NULL,
                    CONSTRAINT uq_backtest_promotions_account_run
                        UNIQUE (account_id, run_id),
                    CONSTRAINT uq_backtest_promotions_exact_evidence
                        UNIQUE (
                            account_id, symbol, strategy_id, timeframe,
                            dataset_fingerprint, engine_version
                        ),
                    CONSTRAINT uq_backtest_promotions_version
                        UNIQUE (promotion_id, version)
                )
                """
            )
            cursor.execute(
                f"""
                CREATE TABLE IF NOT EXISTS backtest_promotion_transitions (
                    transition_id TEXT PRIMARY KEY,
                    promotion_id TEXT NOT NULL REFERENCES backtest_promotions(promotion_id),
                    from_state TEXT NOT NULL CHECK (from_state IN ({promotion_states})),
                    to_state TEXT NOT NULL CHECK (to_state IN ({promotion_states})),
                    from_version INTEGER NOT NULL CHECK (from_version >= 1),
                    to_version INTEGER NOT NULL CHECK (to_version = from_version + 1),
                    status TEXT NOT NULL CHECK (status IN ('COMPLETED', 'REJECTED')),
                    reason_code TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    evidence_run_id TEXT NOT NULL,
                    correlation_id TEXT,
                    metadata {json_type} NOT NULL,
                    created_at {timestamp_type} NOT NULL,
                    CONSTRAINT uq_backtest_promotion_transition_version
                        UNIQUE (promotion_id, from_version, to_version)
                )
                """
            )
            indexes = [
                ("idx_backtest_promotions_account", "backtest_promotions(account_id)"),
                ("idx_backtest_promotions_symbol", "backtest_promotions(symbol)"),
                ("idx_backtest_promotions_strategy", "backtest_promotions(strategy_id)"),
                ("idx_backtest_promotions_timeframe", "backtest_promotions(timeframe)"),
                ("idx_backtest_promotions_state", "backtest_promotions(state)"),
                ("idx_backtest_promotions_expires", "backtest_promotions(expires_at)"),
                ("idx_backtest_promotions_run", "backtest_promotions(run_id)"),
                ("idx_backtest_promotions_dataset", "backtest_promotions(dataset_fingerprint)"),
                ("idx_backtest_promotions_updated", "backtest_promotions(updated_at)"),
                (
                    "idx_backtest_promotions_exact_lookup",
                    "backtest_promotions(account_id, symbol, strategy_id, timeframe, updated_at)",
                ),
                (
                    "idx_backtest_promotion_transitions_history",
                    "backtest_promotion_transitions(promotion_id, created_at)",
                ),
            ]
            for name, target in indexes:
                cursor.execute(f"CREATE INDEX IF NOT EXISTS {name} ON {target}")
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cursor.close()


def _promotion_from_row(row: Any, *, replay: bool = False) -> BacktestPromotionRecord:
    item = _row_dict(row)
    if not item:
        raise PromotionNotFound("promotion was not found")
    for key in [
        "created_at",
        "updated_at",
        "validated_at",
        "oos_passed_at",
        "robustness_passed_at",
        "approved_for_paper_at",
        "paper_observing_at",
        "expires_at",
        "revoked_at",
        "rejected_at",
        "failed_at",
        "last_observed_at",
    ]:
        parsed = _parse_datetime(item.get(key))
        if key in {"created_at", "updated_at"} and parsed is None:
            raise PromotionDatabaseConflict(f"promotion {key} is missing or timezone-naive")
        item[key] = parsed
    item["metadata"] = _json_loads(item.get("metadata"))
    item["idempotent_replay"] = replay
    return BacktestPromotionRecord(**item)


def _promotion_from_transition_replay(row: Any) -> Optional[BacktestPromotionRecord]:
    item = _row_dict(row)
    metadata = _json_loads(item.get("metadata"))
    snapshot = metadata.get("result_snapshot")
    if not isinstance(snapshot, dict):
        return None
    replay_snapshot = {**snapshot, "idempotent_replay": True}
    return BacktestPromotionRecord.model_validate(replay_snapshot, strict=False)


def _transition_from_row(row: Any) -> BacktestPromotionTransitionRecord:
    item = _row_dict(row)
    if not item:
        raise PromotionDatabaseConflict("promotion transition history row is malformed")
    item["created_at"] = _parse_datetime(item.get("created_at"))
    if item["created_at"] is None:
        raise PromotionDatabaseConflict("promotion transition timestamp is malformed")
    item.pop("metadata", None)
    return BacktestPromotionTransitionRecord(**item)


def _select_promotion(cursor, db, promotion_id: str, *, lock: bool = False):
    lock_clause = " FOR UPDATE" if lock and db.db_type == "postgres" else ""
    cursor.execute(
        f"SELECT * FROM backtest_promotions WHERE promotion_id = {db.param_style}{lock_clause}",
        (promotion_id,),
    )
    return cursor.fetchone()


def _select_by_account_run(cursor, db, account_id: str, run_id: str):
    cursor.execute(
        f"SELECT * FROM backtest_promotions WHERE account_id = {db.param_style} "
        f"AND run_id = {db.param_style}",
        (account_id, run_id),
    )
    return cursor.fetchone()


def _select_transition(cursor, db, transition_id: str):
    cursor.execute(
        f"SELECT * FROM backtest_promotion_transitions "
        f"WHERE transition_id = {db.param_style}",
        (transition_id,),
    )
    return cursor.fetchone()


def _require_exact_evidence(promotion: BacktestPromotionRecord, detail: Any) -> dict:
    run = detail.run
    metadata = dict(run.metadata or {})
    mismatches: list[str] = []
    comparisons = {
        "account_id": (str(run.account_id or ""), promotion.account_id),
        "run_id": (str(run.run_id or ""), promotion.run_id),
        "skill_id": (str(run.skill_id or ""), promotion.skill_id),
        "strategy_id": (str(run.strategy_id or ""), promotion.strategy_id),
        "symbol": (str(run.symbol or "").upper(), promotion.symbol),
        "timeframe": (str(run.timeframe or "").lower(), promotion.timeframe),
        "engine_version": (str(run.engine_version or ""), promotion.engine_version),
        "dataset_fingerprint": (
            str(metadata.get("dataset_fingerprint") or "").lower(),
            promotion.dataset_fingerprint,
        ),
        "validation_profile": (
            str(metadata.get("validation_profile") or ""),
            promotion.validation_profile,
        ),
    }
    for field, (actual, expected) in comparisons.items():
        if actual != expected:
            mismatches.append(f"{field}: expected={expected!r}, actual={actual!r}")
    if mismatches:
        raise PromotionEvidenceMismatch(
            "backtest evidence identity mismatch: " + "; ".join(mismatches)
        )
    return metadata

__all__ = [
    "PromotionError", "PromotionNotFound", "InvalidPromotionTransition",
    "StalePromotionVersion", "DuplicatePromotionTransition",
    "PromotionEvidenceMismatch", "PromotionExpired", "PromotionTerminalState",
    "PromotionValidationFailed", "PromotionDatabaseConflict",
    "PromotionApprovalRequired", "TERMINAL_STATES", "APPROVED_STATES",
    "ALLOWED_TRANSITIONS", "STATE_TIMESTAMP_COLUMNS", "_SQLITE_WRITE_LOCK",
    "_env_int", "_env_float", "_env_bool", "_utc_now", "_db_time",
    "_parse_datetime", "_row_dict", "_json_dumps", "_json_loads",
    "_assert_finite_json", "_promotion_id", "deterministic_transition_id",
    "setup_backtest_promotion_tables", "_promotion_from_row",
    "_promotion_from_transition_replay", "_transition_from_row",
    "_select_promotion", "_select_by_account_run", "_select_transition",
    "_require_exact_evidence", "logger",
]
