"""PostgreSQL TLS inspection helpers.

Prefer the client-side libpq signal because connection poolers can terminate TLS
before opening a separate backend connection. In that topology, ``pg_stat_ssl``
may describe the pooler-to-database hop rather than the application-to-pooler
connection.
"""

from __future__ import annotations

from typing import Any


def connection_uses_tls(connection: Any, cursor: Any | None = None) -> bool:
    """Return whether the active client connection uses TLS.

    psycopg2 exposes libpq's ``PQsslInUse`` value as
    ``connection.info.ssl_in_use``. That is the authoritative signal for the
    application connection, including Supabase Session Pooler connections.
    Older drivers fall back to ``pg_stat_ssl``.
    """

    info = getattr(connection, "info", None)
    ssl_in_use = getattr(info, "ssl_in_use", None) if info is not None else None
    if ssl_in_use is not None:
        return bool(ssl_in_use)

    owns_cursor = cursor is None
    if cursor is None:
        cursor = connection.cursor()

    try:
        cursor.execute(
            """
            SELECT COALESCE(
                (SELECT ssl FROM pg_stat_ssl WHERE pid = pg_backend_pid()),
                false
            ) AS tls
            """
        )
        row = cursor.fetchone()
        if not row:
            return False
        if isinstance(row, dict):
            return bool(row.get("tls"))
        try:
            return bool(row["tls"])
        except (KeyError, TypeError):
            return bool(row[0])
    finally:
        if owns_cursor:
            cursor.close()
