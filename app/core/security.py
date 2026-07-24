"""Security dependencies for the Database Agent API."""

from __future__ import annotations

from typing import Callable

from fastapi import HTTPException, Security
from fastapi.security import APIKeyHeader

from app.core.config import Settings


SettingsProvider = Callable[[], Settings]


def create_api_key_dependency(
    settings_provider: SettingsProvider,
    *,
    header_name: str = "X-API-KEY",
):
    """Create a dependency that evaluates settings at request time.

    Evaluating through a provider preserves the existing test contract where
    runtime settings can be patched without rebuilding the FastAPI app.
    """

    api_key_header = APIKeyHeader(name=header_name, auto_error=False)

    def get_api_key(
        supplied_api_key: str | None = Security(api_key_header),
    ) -> str:
        settings = settings_provider()
        configured_api_key = settings.database_agent_api_key
        if settings.database_dev_mode and not configured_api_key:
            return "dev-mode"
        if configured_api_key and supplied_api_key == configured_api_key:
            return supplied_api_key
        raise HTTPException(
            status_code=403,
            detail="Could not validate credentials",
        )

    return get_api_key
