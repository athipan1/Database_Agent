from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Union

from curator_observation_models import (
    CreateCuratorObservationBody,
    CuratorObservation,
    CuratorObservationReadiness,
)


OBSERVATION_TARGET = 50
UNSAFE_CONTRACT_CODES = {
    "advisory_only_must_be_true",
    "broker_access_must_be_false",
    "order_placement_must_be_false",
    "requires_risk_gate_must_be_true",
    "direct_execution_allowed_must_be_false",
}


def _json_dumps(value: Any) -> str:
    return json.dumps(value if value is not None else {}, default=str, sort_keys=True)


def _json_loads(value: Any, default: Any) -> Any:
    if value is None:
        return default
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(value)
    except Exception:
        return default


def _bool_or_none(value: Any) -> Optional[bool]:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "on"}:
        return True
    if text in {"0", "false", "no", "off"}:
        return False
    return None


def _row_to_observation(row: Any) -> CuratorObservation:
    item = dict(row)
    item["available"] = bool(item.get("available"))
    item["contract_valid"] = _bool_or_none(item.get("contract_valid"))
    item["would_pass_required_gate"] = _bool_or_none(
        item.get("would_pass_required_gate")
    )
    item["rejection_codes"] = _json_loads(item.get("rejection_codes"), [])
    item["metadata"] = _json_loads(item.get("metadata"), {})
    return CuratorObservation(**item)


def setup_curator_observation_table(db) -> None:
    with db.connection_scope() as conn:
        cursor = db.get_cursor(conn)
        try:
            json_type = "TEXT" if db.db_type == "sqlite" else "JSONB"
            timestamp_type = "TEXT" if db.db_type == "sqlite" else "TIMESTAMPTZ"
            boolean_type = "INTEGER" if db.db_type == "sqlite" else "BOOLEAN"
            cursor.execute(
                f"""
                CREATE TABLE IF NOT EXISTS curator_observations (
                    observation_id TEXT PRIMARY KEY,
                    account_id TEXT NOT NULL,
                    correlation_id TEXT,
                    symbol TEXT NOT NULL,
                    observed_at {timestamp_type} NOT NULL,
                    mode TEXT NOT NULL,
                    status TEXT NOT NULL,
                    available {boolean_type} NOT NULL,
                    signal TEXT NOT NULL,
                    agreement DOUBLE PRECISION,
                    contract_valid {boolean_type},
                    would_pass_required_gate {boolean_type},
                    selected_skill_count INTEGER NOT NULL DEFAULT 0,
                    execution_count INTEGER NOT NULL DEFAULT 0,
                    minimum_agreement DOUBLE PRECISION,
                    rejection_codes {json_type} NOT NULL,
                    metadata {json_type} NOT NULL,
                    created_at {timestamp_type} NOT NULL
                )
                """
            )
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_curator_observations_account_time "
                "ON curator_observations(account_id, observed_at DESC)"
            )
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_curator_observations_symbol_time "
                "ON curator_observations(symbol, observed_at DESC)"
            )
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_curator_observations_mode_time "
                "ON curator_observations(mode, observed_at DESC)"
            )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cursor.close()


def get_curator_observation(db, observation_id: str) -> Optional[CuratorObservation]:
    setup_curator_observation_table(db)
    with db.connection_scope() as conn:
        cursor = db.get_cursor(conn)
        try:
            cursor.execute(
                f"SELECT * FROM curator_observations "
                f"WHERE observation_id = {db.param_style}",
                (observation_id,),
            )
            row = cursor.fetchone()
            return _row_to_observation(row) if row else None
        finally:
            cursor.close()


def _observation_id(
    body: CreateCuratorObservationBody,
    *,
    correlation_id: Optional[str],
) -> str:
    if body.observation_id:
        return body.observation_id
    effective_correlation_id = body.correlation_id or correlation_id or "unknown"
    seed = "|".join(
        [
            str(body.account_id),
            effective_correlation_id,
            body.symbol.upper(),
            body.mode,
        ]
    )
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"curator-observation:{seed}"))


def create_curator_observation(
    db,
    body: CreateCuratorObservationBody,
    *,
    correlation_id: Optional[str] = None,
) -> CuratorObservation:
    setup_curator_observation_table(db)
    observation_id = _observation_id(body, correlation_id=correlation_id)
    existing = get_curator_observation(db, observation_id)
    if existing:
        return existing

    now = datetime.now(timezone.utc)
    record = CuratorObservation(
        **body.model_dump(exclude={"observation_id", "observed_at"}),
        observation_id=observation_id,
        correlation_id=body.correlation_id or correlation_id,
        observed_at=body.observed_at or now,
        created_at=now,
    )
    insert_keyword = "INSERT OR IGNORE" if db.db_type == "sqlite" else "INSERT"
    conflict_clause = "" if db.db_type == "sqlite" else " ON CONFLICT (observation_id) DO NOTHING"

    with db.connection_scope() as conn:
        cursor = db.get_cursor(conn)
        try:
            cursor.execute(
                f"""
                {insert_keyword} INTO curator_observations (
                    observation_id, account_id, correlation_id, symbol,
                    observed_at, mode, status, available, signal, agreement,
                    contract_valid, would_pass_required_gate,
                    selected_skill_count, execution_count, minimum_agreement,
                    rejection_codes, metadata, created_at
                ) VALUES (
                    {db.param_style}, {db.param_style}, {db.param_style},
                    {db.param_style}, {db.param_style}, {db.param_style},
                    {db.param_style}, {db.param_style}, {db.param_style},
                    {db.param_style}, {db.param_style}, {db.param_style},
                    {db.param_style}, {db.param_style}, {db.param_style},
                    {db.param_style}, {db.param_style}, {db.param_style}
                ){conflict_clause}
                """,
                (
                    record.observation_id,
                    str(record.account_id),
                    record.correlation_id,
                    record.symbol,
                    record.observed_at,
                    record.mode,
                    record.status,
                    record.available,
                    record.signal,
                    record.agreement,
                    record.contract_valid,
                    record.would_pass_required_gate,
                    record.selected_skill_count,
                    record.execution_count,
                    record.minimum_agreement,
                    _json_dumps(record.rejection_codes),
                    _json_dumps(record.metadata),
                    record.created_at,
                ),
            )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cursor.close()

    persisted = get_curator_observation(db, observation_id)
    if not persisted:
        raise RuntimeError("Curator observation was not persisted")
    return persisted


def create_curator_observation_batch(
    db,
    observations: List[CreateCuratorObservationBody],
    *,
    correlation_id: Optional[str] = None,
) -> List[CuratorObservation]:
    return [
        create_curator_observation(db, body, correlation_id=correlation_id)
        for body in observations
    ]


def list_curator_observations(
    db,
    *,
    account_id: Optional[Union[int, str]] = None,
    symbol: Optional[str] = None,
    mode: Optional[str] = None,
    status: Optional[str] = None,
    limit: int = 100,
    offset: int = 0,
) -> List[CuratorObservation]:
    setup_curator_observation_table(db)
    query = "SELECT * FROM curator_observations WHERE 1=1"
    params: List[Any] = []
    if account_id is not None:
        query += f" AND account_id = {db.param_style}"
        params.append(str(account_id))
    if symbol:
        query += f" AND symbol = {db.param_style}"
        params.append(symbol.upper())
    if mode:
        query += f" AND mode = {db.param_style}"
        params.append(mode)
    if status:
        query += f" AND status = {db.param_style}"
        params.append(status)
    query += (
        f" ORDER BY observed_at DESC, observation_id DESC "
        f"LIMIT {db.param_style} OFFSET {db.param_style}"
    )
    params.extend([max(1, int(limit)), max(0, int(offset))])

    with db.connection_scope() as conn:
        cursor = db.get_cursor(conn)
        try:
            cursor.execute(query, tuple(params))
            return [_row_to_observation(row) for row in (cursor.fetchall() or [])]
        finally:
            cursor.close()


def build_curator_observation_readiness(
    db,
    *,
    account_id: Optional[Union[int, str]] = None,
    mode: str = "shadow_ensemble",
    observation_target: int = OBSERVATION_TARGET,
) -> CuratorObservationReadiness:
    observations = list_curator_observations(
        db,
        account_id=account_id,
        mode=mode,
        limit=100000,
        offset=0,
    )
    total = len(observations)
    available = sum(1 for row in observations if row.available)
    contract_valid = sum(1 for row in observations if row.contract_valid is True)
    contract_invalid = sum(1 for row in observations if row.contract_valid is False)
    agreements = [row.agreement for row in observations if row.agreement is not None]
    unsafe_contract_count = sum(
        1
        for row in observations
        if any(code in UNSAFE_CONTRACT_CODES for code in row.rejection_codes)
    )
    pass_count = sum(
        1 for row in observations if row.would_pass_required_gate is True
    )
    signal_counts = {
        signal: sum(1 for row in observations if row.signal == signal)
        for signal in ("buy", "hold", "sell", "unknown")
    }
    availability_rate = available / total if total else None
    contract_valid_rate = contract_valid / total if total else None
    average_agreement = sum(agreements) / len(agreements) if agreements else None

    blockers: List[str] = []
    if total < observation_target:
        blockers.append("observations_below_target")
    if availability_rate is None or availability_rate < 0.99:
        blockers.append("availability_below_99_percent")
    if contract_valid_rate != 1.0:
        blockers.append("contract_valid_rate_below_100_percent")
    if unsafe_contract_count:
        blockers.append("unsafe_contracts_detected")

    return CuratorObservationReadiness(
        account_id=str(account_id) if account_id is not None else None,
        mode=mode,
        observations=total,
        observation_target=observation_target,
        available=available,
        unavailable=total - available,
        availability_rate=availability_rate,
        contract_valid=contract_valid,
        contract_invalid=contract_invalid,
        contract_valid_rate=contract_valid_rate,
        unsafe_contract_count=unsafe_contract_count,
        buy_count=signal_counts["buy"],
        hold_count=signal_counts["hold"],
        sell_count=signal_counts["sell"],
        unknown_count=signal_counts["unknown"],
        average_agreement=average_agreement,
        would_pass_required_gate=pass_count,
        would_be_blocked=total - pass_count,
        required_mode_eligible=not blockers,
        blockers=blockers,
    )
