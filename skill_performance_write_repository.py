from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Iterable

from skill_performance_models import (
    CreateSkillExecutionLogBody,
    CreateSkillTradeOutcomeBody,
    SkillExecutionLog,
    SkillTradeOutcome,
)
from skill_performance_repository import (
    create_skill_execution_log as _create_skill_execution_log,
    create_skill_trade_outcome as _create_skill_trade_outcome,
)


class _ModelDumpExcluding:
    """Delegate a Pydantic body while excluding explicitly rebuilt fields.

    The legacy repository normalizes fields such as ``symbol`` after expanding
    ``body.model_dump()`` into a model constructor. Without excluding those
    fields first, Python receives the same keyword twice and raises ``TypeError``
    before any database write occurs.
    """

    def __init__(self, body: Any, *, always_exclude: Iterable[str]) -> None:
        self._body = body
        self._always_exclude = set(always_exclude)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._body, name)

    def model_dump(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        requested = kwargs.pop("exclude", None)
        if requested is None:
            merged: set[str] | dict[str, Any] = set(self._always_exclude)
        elif isinstance(requested, Mapping):
            merged = dict(requested)
            for field in self._always_exclude:
                merged[field] = True
        else:
            merged = set(requested) | self._always_exclude
        return self._body.model_dump(*args, exclude=merged, **kwargs)


def create_skill_execution_log(
    db: Any,
    body: CreateSkillExecutionLogBody,
) -> SkillExecutionLog:
    safe_body = _ModelDumpExcluding(body, always_exclude={"symbol"})
    return _create_skill_execution_log(db, safe_body)


def create_skill_trade_outcome(
    db: Any,
    body: CreateSkillTradeOutcomeBody,
) -> SkillTradeOutcome:
    safe_body = _ModelDumpExcluding(
        body,
        always_exclude={"symbol", "closed_at"},
    )
    return _create_skill_trade_outcome(db, safe_body)
