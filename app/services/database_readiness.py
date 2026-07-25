"""Fail-closed database readiness checks for primary cutover."""

from __future__ import annotations

from typing import Any, Dict

from app.core.postgres_tls import connection_uses_tls
from schema_identity_repository import (
    SCHEMA_SHA256,
    SCHEMA_VERSION,
    get_schema_identity,
)


def _postgres_transport_state(db) -> Dict[str, Any]:
    with db.connection_scope() as conn:
        cursor = db.get_cursor(conn)
        try:
            cursor.execute(
                """
                SELECT current_setting('server_version_num')::integer
                    AS server_version_num
                """
            )
            row = cursor.fetchone()
            tls = connection_uses_tls(conn, cursor)
            if isinstance(row, dict):
                return {
                    "server_version_num": int(row["server_version_num"]),
                    "tls": tls,
                }
            try:
                mapped = dict(row)
                return {
                    "server_version_num": int(mapped["server_version_num"]),
                    "tls": tls,
                }
            except (TypeError, ValueError):
                return {
                    "server_version_num": int(row[0]),
                    "tls": tls,
                }
        finally:
            cursor.close()


def inspect_database_readiness(db, settings) -> Dict[str, Any]:
    """Return a secret-free readiness report and explicit failure reasons."""

    reasons: list[str] = []
    try:
        connected = bool(db.check_connection())
    except Exception:
        connected = False
    if not connected:
        reasons.append("database_disconnected")

    expected_provider = settings.database_expected_provider
    provider_match = (
        not settings.database_cutover_guard_enabled
        or expected_provider == settings.database_provider
    )
    if not provider_match:
        reasons.append("provider_mismatch")

    transport = {"server_version_num": None, "tls": False}
    if connected and getattr(db, "db_type", None) == "postgres":
        try:
            transport = _postgres_transport_state(db)
        except Exception:
            reasons.append("transport_inspection_failed")

    tls_required = settings.database_provider == "supabase"
    tls_ready = not tls_required or bool(transport.get("tls"))
    if not tls_ready:
        reasons.append("tls_required")

    identity: Dict[str, Any] = {}
    schema_ready = not settings.database_require_schema_identity
    if connected and settings.database_require_schema_identity:
        try:
            identity = get_schema_identity(db)
            schema_ready = (
                identity.get("schema_version") == SCHEMA_VERSION
                and identity.get("schema_sha256") == SCHEMA_SHA256
            )
        except Exception:
            schema_ready = False
        if not schema_ready:
            reasons.append("schema_identity_mismatch")

    ready = connected and provider_match and tls_ready and schema_ready
    return {
        "ready": ready,
        "database_connection": "connected" if connected else "disconnected",
        "provider": settings.database_provider,
        "expected_provider": expected_provider,
        "cutover_guard_enabled": settings.database_cutover_guard_enabled,
        "tls": bool(transport.get("tls")),
        "server_version_num": transport.get("server_version_num"),
        "schema_identity_required": settings.database_require_schema_identity,
        "schema_version": identity.get("schema_version"),
        "schema_identity_match": schema_ready,
        "reasons": reasons,
    }
