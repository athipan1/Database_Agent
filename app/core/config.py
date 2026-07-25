"""Typed environment configuration for the Database Agent runtime.

This module is intentionally side-effect free. Importing it never exits the
process, opens a database connection, or creates an Alpaca client. Runtime
entrypoints decide when validation should be enforced.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from decimal import Decimal
from typing import Mapping, Optional


_TRUE_VALUES = frozenset({"1", "true", "yes", "y", "on"})
_ALLOWED_DATABASE_PROVIDERS = frozenset({"postgres", "supabase"})
_ALLOWED_SSL_MODES = frozenset(
    {"disable", "allow", "prefer", "require", "verify-ca", "verify-full"}
)


def env_bool(
    name: str,
    default: bool = False,
    *,
    environ: Optional[Mapping[str, str]] = None,
) -> bool:
    """Read a permissive boolean environment variable."""

    source = os.environ if environ is None else environ
    value = source.get(name)
    if value is None:
        return default
    return value.strip().lower() in _TRUE_VALUES


@dataclass(frozen=True)
class Settings:
    """Runtime settings that were previously assembled inside ``main.py``."""

    trading_mode: str = "PAPER"
    database_dev_mode: bool = False
    database_emergency_halt: bool = False
    database_agent_api_key: Optional[str] = None
    default_dev_account_id: str = "1"
    default_dev_cash_balance: Decimal = Decimal("100000")
    alpaca_api_key: Optional[str] = None
    alpaca_secret_key: Optional[str] = None
    database_provider: str = "postgres"
    database_url_configured: bool = False
    database_ssl_mode: str = "prefer"
    database_create_if_missing: bool = True
    database_pool_min: int = 1
    database_pool_max: int = 20
    database_connect_timeout_seconds: int = 5

    @classmethod
    def from_environ(
        cls,
        environ: Optional[Mapping[str, str]] = None,
    ) -> "Settings":
        source = os.environ if environ is None else environ
        database_provider = source.get("DATABASE_PROVIDER", "postgres").strip().lower()
        default_ssl_mode = "require" if database_provider == "supabase" else "prefer"
        default_create_if_missing = database_provider != "supabase"
        return cls(
            trading_mode=source.get("TRADING_MODE", "PAPER").strip().upper(),
            database_dev_mode=env_bool(
                "DATABASE_DEV_MODE",
                False,
                environ=source,
            ),
            database_emergency_halt=env_bool(
                "DATABASE_EMERGENCY_HALT",
                False,
                environ=source,
            ),
            database_agent_api_key=(
                source.get("DATABASE_AGENT_API_KEY") or None
            ),
            default_dev_account_id=source.get("DEFAULT_DEV_ACCOUNT_ID", "1"),
            default_dev_cash_balance=Decimal(
                source.get("DEFAULT_DEV_CASH_BALANCE", "100000")
            ),
            alpaca_api_key=source.get("ALPACA_API_KEY") or None,
            alpaca_secret_key=source.get("ALPACA_SECRET_KEY") or None,
            database_provider=database_provider,
            database_url_configured=bool(source.get("DATABASE_URL")),
            database_ssl_mode=source.get(
                "DATABASE_SSL_MODE",
                default_ssl_mode,
            ).strip().lower(),
            database_create_if_missing=env_bool(
                "DATABASE_CREATE_IF_MISSING",
                default_create_if_missing,
                environ=source,
            ),
            database_pool_min=int(source.get("DATABASE_POOL_MIN", "1")),
            database_pool_max=int(source.get("DATABASE_POOL_MAX", "20")),
            database_connect_timeout_seconds=int(
                source.get("DATABASE_CONNECT_TIMEOUT_SECONDS", "5")
            ),
        )

    def validate(self) -> None:
        """Validate fail-closed production invariants."""

        if self.trading_mode not in {"PAPER", "LIVE"}:
            raise ValueError("TRADING_MODE must be PAPER or LIVE")
        if self.trading_mode == "LIVE" and self.database_dev_mode:
            raise ValueError(
                "DATABASE_DEV_MODE=true is forbidden when TRADING_MODE=LIVE"
            )
        if not self.database_agent_api_key and not self.database_dev_mode:
            raise ValueError(
                "DATABASE_AGENT_API_KEY is required outside DATABASE_DEV_MODE"
            )
        if self.database_provider not in _ALLOWED_DATABASE_PROVIDERS:
            raise ValueError("DATABASE_PROVIDER must be postgres or supabase")
        if self.database_ssl_mode not in _ALLOWED_SSL_MODES:
            raise ValueError("DATABASE_SSL_MODE is not a supported PostgreSQL sslmode")
        if self.database_pool_min < 1:
            raise ValueError("DATABASE_POOL_MIN must be at least 1")
        if self.database_pool_max < self.database_pool_min:
            raise ValueError("DATABASE_POOL_MAX must be greater than or equal to DATABASE_POOL_MIN")
        if self.database_pool_max > 50:
            raise ValueError("DATABASE_POOL_MAX must not exceed 50")
        if not 1 <= self.database_connect_timeout_seconds <= 60:
            raise ValueError(
                "DATABASE_CONNECT_TIMEOUT_SECONDS must be between 1 and 60"
            )
        if self.database_provider == "supabase":
            if not self.database_url_configured:
                raise ValueError("DATABASE_URL is required for DATABASE_PROVIDER=supabase")
            if self.database_create_if_missing:
                raise ValueError(
                    "DATABASE_CREATE_IF_MISSING must be false for DATABASE_PROVIDER=supabase"
                )
            if self.database_ssl_mode not in {"require", "verify-ca", "verify-full"}:
                raise ValueError(
                    "DATABASE_SSL_MODE must require TLS for DATABASE_PROVIDER=supabase"
                )


def load_settings(
    environ: Optional[Mapping[str, str]] = None,
    *,
    validate: bool = True,
) -> Settings:
    """Load settings and optionally enforce production invariants."""

    settings = Settings.from_environ(environ)
    if validate:
        settings.validate()
    return settings
