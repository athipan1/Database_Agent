"""Standard response envelopes and exception handler registration."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Optional

from fastapi import FastAPI, HTTPException, Request
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse


AGENT_VERSION = "1.1.0"


def wrap_response(
    data: Any = None,
    status: str = "success",
    error: Optional[dict] = None,
):
    return {
        "status": status,
        "agent_type": "database",
        "version": AGENT_VERSION,
        "timestamp": datetime.now(timezone.utc),
        "data": data,
        "error": error,
        "confidence_score": None,
    }


def install_exception_handlers(app: FastAPI, response_wrapper=wrap_response) -> None:
    """Install the legacy error envelope without exposing stack traces."""

    @app.exception_handler(HTTPException)
    async def http_exception_handler(request: Request, exc: HTTPException):
        return JSONResponse(
            status_code=exc.status_code,
            content=jsonable_encoder(
                response_wrapper(
                    status="error",
                    error={
                        "code": str(exc.status_code),
                        "message": exc.detail,
                        "retryable": False,
                    },
                )
            ),
        )

    @app.exception_handler(Exception)
    async def general_exception_handler(request: Request, exc: Exception):
        logging.error("Unhandled exception: %s", exc, exc_info=True)
        return JSONResponse(
            status_code=500,
            content=jsonable_encoder(
                response_wrapper(
                    status="error",
                    error={
                        "code": "INTERNAL_SERVER_ERROR",
                        "message": str(exc),
                        "retryable": False,
                    },
                )
            ),
        )
