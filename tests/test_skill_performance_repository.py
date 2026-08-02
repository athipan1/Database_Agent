from __future__ import annotations

import os
from datetime import datetime, timezone

os.environ.setdefault("USE_SQLITE", "1")
os.environ.setdefault("DATABASE_DEV_MODE", "true")

from skill_performance_models import (
    CreateSkillExecutionLogBody,
    CreateSkillTradeOutcomeBody,
)
from skill_performance_repository import (
    create_skill_execution_log,
    create_skill_trade_outcome,
    list_skill_execution_logs,
    setup_skill_performance_tables,
)
from trading_db import TradingDB


def _database() -> TradingDB:
    db = TradingDB()
    db.setup_database()
    setup_skill_performance_tables(db)
    return db


def test_execution_log_persists_normalized_symbol_without_duplicate_kwargs() -> None:
    db = _database()
    body = CreateSkillExecutionLogBody(
        account_id=1,
        skill_id="soak-skill",
        skill_name="Soak Skill",
        symbol="test",
        signal="hold",
        confidence=0.5,
        reason="deterministic advisory soak",
        input_payload={"symbol": "TEST", "score": 0.5},
        output_payload={
            "signal": "hold",
            "confidence": 0.5,
            "reason": "deterministic advisory soak",
        },
        execution_status="success",
        source_agent="curator-agent",
        run_id="soak-run-1",
        metadata={"advisory_only": True},
    )

    record = create_skill_execution_log(db, body)

    assert record.execution_log_id
    assert record.symbol == "TEST"
    assert record.run_id == "soak-run-1"
    assert record.metadata["advisory_only"] is True
    persisted = list_skill_execution_logs(db, skill_id="soak-skill")
    assert len(persisted) == 1
    assert persisted[0].symbol == "TEST"


def test_trade_outcome_persists_normalized_symbol_and_closed_at_once() -> None:
    db = _database()
    execution = create_skill_execution_log(
        db,
        CreateSkillExecutionLogBody(
            account_id=1,
            skill_id="outcome-skill",
            symbol="acgl",
            execution_status="success",
        ),
    )
    closed_at = datetime(2026, 8, 2, 9, 0, tzinfo=timezone.utc)
    body = CreateSkillTradeOutcomeBody(
        execution_log_id=execution.execution_log_id,
        skill_id=execution.skill_id,
        account_id=1,
        symbol="acgl",
        realized_pl=12.5,
        realized_pl_pct=1.25,
        outcome="win",
        closed_at=closed_at,
        metadata={"source": "repository-regression"},
    )

    record = create_skill_trade_outcome(db, body)

    assert record.outcome_id
    assert record.symbol == "ACGL"
    assert record.closed_at == closed_at
    assert record.execution_log_id == execution.execution_log_id
