"""Database provider selection for local PostgreSQL and managed Supabase PostgreSQL."""

from __future__ import annotations

import logging
import os
import time
from typing import Optional

import psycopg2
import psycopg2.pool
from psycopg2.extensions import parse_dsn

from app.core.config import Settings
from trading_db import TradingDB


class ManagedPostgresTradingDB(TradingDB):
    """TradingDB connection adapter for externally managed PostgreSQL databases.

    Managed providers already own the target database. This adapter therefore
    never connects to a maintenance database and never attempts CREATE DATABASE.
    It also ignores the legacy ``USE_SQLITE`` test switch because silently
    falling back to an in-memory database would be unsafe for a managed primary.
    """

    def __init__(
        self,
        settings: Settings,
        *,
        database_url: Optional[str] = None,
        max_retries: int = 5,
        initial_delay: float = 1,
    ) -> None:
        self._managed_settings = settings
        self._managed_database_url = database_url or os.environ.get("DATABASE_URL")
        super().__init__(max_retries=max_retries, initial_delay=initial_delay)

    def _connect_with_retry(self) -> None:
        # ``TradingDB.__init__`` historically derives db_type from USE_SQLITE.
        # A managed provider must never inherit that fallback because it would
        # acknowledge writes in a transient in-memory database.
        self.db_type = "postgres"
        self.param_style = "%s"

        if not self._managed_database_url:
            raise ValueError(
                "DATABASE_URL is required for an externally managed PostgreSQL provider"
            )

        connection_parameters = parse_dsn(self._managed_database_url)
        connection_parameters.setdefault("dbname", "postgres")
        connection_parameters.update(
            {
                "sslmode": self._managed_settings.database_ssl_mode,
                "connect_timeout": self._managed_settings.database_connect_timeout_seconds,
                "application_name": "database-agent",
                "keepalives": 1,
                "keepalives_idle": 30,
                "keepalives_interval": 10,
                "keepalives_count": 5,
            }
        )

        retries = 0
        delay = float(self.initial_delay)
        while retries < self.max_retries:
            candidate_pool = None
            try:
                candidate_pool = psycopg2.pool.ThreadedConnectionPool(
                    minconn=self._managed_settings.database_pool_min,
                    maxconn=self._managed_settings.database_pool_max,
                    **connection_parameters,
                )
                connection = candidate_pool.getconn()
                try:
                    with connection.cursor() as cursor:
                        cursor.execute("SELECT 1")
                        cursor.fetchone()
                    connection.rollback()
                finally:
                    candidate_pool.putconn(connection)

                self.pool = candidate_pool
                logging.info(
                    "Initialized managed PostgreSQL pool for provider=%s.",
                    self._managed_settings.database_provider,
                )
                return
            except (psycopg2.OperationalError, psycopg2.InterfaceError) as exc:
                if candidate_pool is not None:
                    candidate_pool.closeall()
                retries += 1
                if retries >= self.max_retries:
                    logging.error(
                        "Managed PostgreSQL connection failed after %s attempts.",
                        self.max_retries,
                    )
                    raise
                logging.warning(
                    "Managed PostgreSQL connection attempt %s/%s failed: %s",
                    retries,
                    self.max_retries,
                    type(exc).__name__,
                )
                time.sleep(delay)
                delay *= 2
            except psycopg2.Error:
                if candidate_pool is not None:
                    candidate_pool.closeall()
                raise


def create_trading_db(settings: Settings) -> TradingDB:
    """Build the database client without changing the established repository API."""

    if settings.database_provider == "postgres" and settings.database_create_if_missing:
        return TradingDB()
    return ManagedPostgresTradingDB(settings)
