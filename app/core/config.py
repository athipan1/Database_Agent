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
    """Runtime settings assembled for the modular Database Agent."""

    trading_mode: str = "PAPER"
    database_dev_mode: bool = False
    database_emergency_halt: bool = False
    database_agent_api_key: Optional[str] = None
    default_dev_account_id: str = "1"
    default_dev_cash_balance: Decimal = Decimal("100000")
    alpaca_api_key: Optional[str] = None
    alpaca_secret_key: Optional[str] = None
    supabase_replication_enabled: bool = False
    supabase_url: Optional[str] = None
    supabase_secret_key: Optional[str] = None
    supabase_table: str = "database_agent_events"
    supabase_replication_interval_seconds: float = 10.0
    supabase_replication_batch_size: int = 50
    supabase_replication_max_attempts: int = 10

    @classmethod
    def from_environ(
        cls,
        environ: Optional[Mapping[str, str]] = None,
    ) -> "Settings":
        source = os.environ if environ is None else environ
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
            supabase_replication_enabled=env_bool(
                "SUPABASE_REPLICATION_ENABLED",
                False,
                environ=source,
            ),
            supabase_url=source.get("SUPABASE_URL") or None,
            supabase_secret_key=source.get("SUPABASE_SECRET_KEY") or None,
            supabase_table=(
                source.get("SUPABASE_TABLE", "database_agent_events").strip()
                or "database_agent_events"
            ),
            supabase_replication_interval_seconds=float(
                source.get("SUPABASE_REPLICATION_INTERVAL_SECONDS", "10")
            ),
            supabase_replication_batch_size=int(
                source.get("SUPABASE_REPLICATION_BATCH_SIZE", "50")
            ),
            supabase_replication_max_attempts=int(
                source.get("SUPABASE_REPLICATION_MAX_ATTEMPTS", "10")
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
        if self.supabase_replication_enabled:
            if not self.supabase_url:
                raise ValueError(
                    "SUPABASE_URL is required when SUPABASE_REPLICATION_ENABLED=true"
                )
            if not self.supabase_url.startswith("https://"):
                raise ValueError("SUPABASE_URL must use HTTPS")
            if not self.supabase_secret_key:
                raise ValueError(
                    "SUPABASE_SECRET_KEY is required when SUPABASE_REPLICATION_ENABLED=true"
                )
        if self.supabase_replication_interval_seconds < 1:
            raise ValueError(
                "SUPABASE_REPLICATION_INTERVAL_SECONDS must be at least 1"
            )
        if not 1 <= self.supabase_replication_batch_size <= 500:
            raise ValueError(
                "SUPABASE_REPLICATION_BATCH_SIZE must be between 1 and 500"
            )
        if not 1 <= self.supabase_replication_max_attempts <= 100:
            raise ValueError(
                "SUPABASE_REPLICATION_MAX_ATTEMPTS must be between 1 and 100"
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
