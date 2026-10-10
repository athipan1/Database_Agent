"""Immutable research-only evidence ledger, isolated from promotion tables.

No Trading, Risk, Broker, Paper Approval, or Backtest promotion state lives here.
An authenticated writer does not imply its research packet is authentic.
"""
from __future__ import annotations

import hashlib
import json
from typing import Any

from pydantic import BaseModel, Field, field_validator

SCHEMA = "phase17-research-evidence.v1"
MAX_PAYLOAD_BYTES = 300_000


def canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


def digest_payload(payload: dict[str, Any]) -> str:
    return hashlib.sha256(canonical_bytes(payload)).hexdigest()


class CreateResearchEvidence(BaseModel):
    """A digest-pinned diagnostic only; never acceptable to promotion gates."""

    schema_version: str
    evidence_id: str
    source_agent: str
    symbol: str = Field(min_length=1, max_length=24, pattern=r"^[A-Z0-9.^-]+$")
    research_profile: str = Field(min_length=1, max_length=96)
    research_dataset_fingerprint: str = Field(min_length=1, max_length=256)
    artifact_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    phases: dict[str, Any] = Field(default_factory=dict)
    research_only: bool
    diagnostic_only: bool
    used_for_selection: bool
    promotion_allowed: bool
    execution_allowed: bool
    sealed_holdout_opened: bool

    @field_validator("evidence_id")
    @classmethod
    def validate_digest(cls, value: str) -> str:
        if len(value) != 64 or any(c not in "0123456789abcdef" for c in value):
            raise ValueError("evidence_id must be a lowercase SHA-256")
        return value

    def immutable_document(self) -> dict[str, Any]:
        return self.model_dump(mode="json", exclude={"evidence_id"})

    def verify(self) -> dict[str, Any]:
        if self.schema_version != SCHEMA or self.source_agent != "Backtest_Agent":
            raise ValueError("unrecognized research evidence source or schema")
        if (self.research_only is not True or self.diagnostic_only is not True
                or self.used_for_selection is not False
                or self.promotion_allowed is not False
                or self.execution_allowed is not False
                or self.sealed_holdout_opened is not False):
            raise ValueError("Research evidence must never authorize selection or execution")
        if set(self.phases) != {"phase13", "phase14", "phase15", "phase16"}:
            raise ValueError("Research phases 13–16 are mandatory and exhaustive")
        for name, phase in self.phases.items():
            if not isinstance(phase, dict) or not isinstance(phase.get("schema_version"), str):
                raise ValueError(f"{name} is not a versioned diagnostic report")
            if (phase.get("promotion_allowed") is not False
                    or phase.get("execution_allowed") is not False
                    or phase.get("sealed_holdout_opened") is not False):
                raise ValueError(f"{name} cannot authorize orders or access sealed holdout")
        document = self.immutable_document()
        content = canonical_bytes(document)
        if len(content) > MAX_PAYLOAD_BYTES:
            raise ValueError("Research evidence exceeds size limit")
        if digest_payload(document) != self.evidence_id:
            raise ValueError("evidence_id does not match immutable content")
        return document


def setup_research_evidence_table(db) -> None:
    payload_type = "TEXT" if db.db_type == "sqlite" else "JSONB"
    timestamp_type = "TEXT" if db.db_type == "sqlite" else "TIMESTAMPTZ"
    with db.connection_scope() as conn:
        cursor = db.get_cursor(conn)
        try:
            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS research_evidence (
                    evidence_id TEXT PRIMARY KEY,
                    source_agent TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    research_profile TEXT NOT NULL,
                    research_dataset_fingerprint TEXT NOT NULL,
                    artifact_sha256 TEXT NOT NULL,
                    payload {payload_type} NOT NULL,
                    created_at {timestamp_type} NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
            """)
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_research_evidence_symbol_time "
                "ON research_evidence(symbol, created_at DESC)"
            )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cursor.close()


def _decode(row) -> dict[str, Any] | None:
    if row is None:
        return None
    try:
        data = dict(row)
    except (TypeError, ValueError):
        return None
    value = data.get("payload")
    if isinstance(value, str):
        value = json.loads(value)
    return {
        "evidence_id": data["evidence_id"],
        "symbol": data["symbol"],
        "research_profile": data["research_profile"],
        "artifact_sha256": data["artifact_sha256"],
        "payload": value,
        "created_at": str(data["created_at"]),
        "research_only": True,
        "promotion_allowed": False,
        "execution_allowed": False,
    }


def get_research_evidence(db, evidence_id: str) -> dict[str, Any] | None:
    with db.connection_scope() as conn:
        cursor = db.get_cursor(conn)
        try:
            cursor.execute(
                f"SELECT evidence_id, symbol, research_profile, artifact_sha256, "
                f"payload, created_at FROM research_evidence "
                f"WHERE evidence_id={db.param_style}",
                (evidence_id,),
            )
            return _decode(cursor.fetchone())
        finally:
            cursor.close()


def insert_research_evidence(db, data: CreateResearchEvidence) -> tuple[dict[str, Any], bool]:
    """One immutable insert; an exact replay is success and a collision is 409."""
    payload = data.verify()
    from psycopg2.extras import Json
    value = json.dumps(payload, sort_keys=True, allow_nan=False)
    with db.connection_scope() as conn:
        cursor = db.get_cursor(conn)
        try:
            cursor.execute(
                f"INSERT INTO research_evidence "
                f"(evidence_id, source_agent, symbol, research_profile, "
                f"research_dataset_fingerprint, artifact_sha256, payload) "
                f"VALUES ({', '.join([db.param_style] * 7)}) "
                f"ON CONFLICT(evidence_id) DO NOTHING",
                (
                    data.evidence_id, data.source_agent, data.symbol,
                    data.research_profile, data.research_dataset_fingerprint,
                    data.artifact_sha256, value if db.db_type == "sqlite" else Json(payload),
                ),
            )
            inserted = cursor.rowcount == 1
            cursor.execute(
                f"SELECT evidence_id, symbol, research_profile, artifact_sha256, "
                f"payload, created_at FROM research_evidence "
                f"WHERE evidence_id={db.param_style}",
                (data.evidence_id,),
            )
            result = _decode(cursor.fetchone())
            if result is None or result["payload"] != payload:
                raise ValueError("immutable research evidence ID collision or stored content mismatch")
            conn.commit()
            return result, not inserted
        except Exception:
            conn.rollback()
            raise
        finally:
            cursor.close()
