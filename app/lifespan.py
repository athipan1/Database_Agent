"""FastAPI lifespan adapter for the legacy startup and shutdown contracts."""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Any


def create_runtime_lifespan(runtime: Any):
    """Build a lifespan context from the runtime's existing event functions."""

    @asynccontextmanager
    async def lifespan(app):
        await runtime.startup_event()
        try:
            yield
        finally:
            await runtime.shutdown_event()

    return lifespan
