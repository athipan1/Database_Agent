from __future__ import annotations

from datetime import datetime, timezone

import skill_performance_write_repository as writes
from skill_performance_models import (
    CreateSkillExecutionLogBody,
    CreateSkillTradeOutcomeBody,
    SkillExecutionLog,
    SkillTradeOutcome,
)


def test_execution_log_write_excludes_normalized_symbol_before_constructor(
    monkeypatch,
) -> None:
    created_at = datetime.now(timezone.utc)

    def legacy_constructor(db, body):
        return SkillExecutionLog(
            **body.model_dump(exclude={"execution_log_id", "created_at"}),
            execution_log_id="log-1",
            symbol=body.symbol.upper(),
            created_at=created_at,
        )

    monkeypatch.setattr(writes, "_create_skill_execution_log", legacy_constructor)
    body = CreateSkillExecutionLogBody(
        account_id=1,
        skill_id="skill-1",
        symbol="aapl",
        signal="hold",
        confidence=0.5,
        output_payload={"signal": "hold"},
    )

    record = writes.create_skill_execution_log(object(), body)

    assert record.execution_log_id == "log-1"
    assert record.symbol == "AAPL"
    assert record.created_at == created_at


def test_trade_outcome_write_excludes_normalized_explicit_fields(monkeypatch) -> None:
    created_at = datetime.now(timezone.utc)
    closed_at = datetime.now(timezone.utc)

    def legacy_constructor(db, body):
        return SkillTradeOutcome(
            **body.model_dump(exclude={"outcome_id"}),
            outcome_id="outcome-1",
            symbol=body.symbol.upper(),
            closed_at=body.closed_at,
            created_at=created_at,
        )

    monkeypatch.setattr(writes, "_create_skill_trade_outcome", legacy_constructor)
    body = CreateSkillTradeOutcomeBody(
        execution_log_id="log-1",
        skill_id="skill-1",
        account_id=1,
        symbol="aapl",
        outcome="win",
        closed_at=closed_at,
    )

    record = writes.create_skill_trade_outcome(object(), body)

    assert record.outcome_id == "outcome-1"
    assert record.symbol == "AAPL"
    assert record.closed_at == closed_at
    assert record.created_at == created_at


def test_model_dump_proxy_merges_mapping_exclusions() -> None:
    body = CreateSkillExecutionLogBody(skill_id="skill-1", symbol="aapl")
    proxy = writes._ModelDumpExcluding(body, always_exclude={"symbol"})

    dumped = proxy.model_dump(exclude={"metadata": True})

    assert "symbol" not in dumped
    assert "metadata" not in dumped
    assert dumped["skill_id"] == "skill-1"
