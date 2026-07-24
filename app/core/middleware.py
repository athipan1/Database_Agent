"""Correlation ID request context and middleware registration."""

from __future__ import annotations

import uuid
from contextvars import ContextVar
from typing import Optional

from fastapi import FastAPI, Request


CorrelationIdContext = ContextVar[Optional[str]]


def create_correlation_id_context() -> CorrelationIdContext:
    return ContextVar("correlation_id", default=None)


def install_correlation_id_middleware(
    app: FastAPI,
    correlation_id_context: CorrelationIdContext,
) -> None:
    """Install the existing X-Correlation-ID propagation contract."""

    @app.middleware("http")
    async def correlation_id_middleware(request: Request, call_next):
        correlation_id = (
            request.headers.get("X-Correlation-ID") or str(uuid.uuid4())
        )
        token = correlation_id_context.set(correlation_id)
        try:
            response = await call_next(request)
            response.headers["X-Correlation-ID"] = correlation_id
            return response
        finally:
            correlation_id_context.reset(token)


def create_correlation_id_dependency(
    correlation_id_context: CorrelationIdContext,
):
    async def get_correlation_id() -> str:
        return correlation_id_context.get() or str(uuid.uuid4())

    return get_correlation_id
