"""Canonical Database_Agent schema identity and drift marker."""

from __future__ import annotations

from typing import Any, Dict


SCHEMA_NAME = "database_agent_primary"
SCHEMA_VERSION = "2026-07-25.1"
SCHEMA_SHA256 = "c41f00a36e0aa693acb8282d2c89cc45e69e74823e919c132c39656898c91b6b"
SOURCE_REPOSITORY = "athipan1/Database_Agent"


def setup_schema_identity_table(db) -> None:
    """Create and upsert the canonical schema marker transactionally."""

    timestamp_type = "TEXT" if db.db_type == "sqlite" else "TIMESTAMPTZ"
    with db.connection_scope() as conn:
        cursor = db.get_cursor(conn)
        try:
            cursor.execute(
                f"""
                CREATE TABLE IF NOT EXISTS database_agent_schema_metadata (
                    schema_name TEXT PRIMARY KEY,
                    schema_version TEXT NOT NULL,
                    schema_sha256 TEXT NOT NULL,
                    source_repository TEXT NOT NULL,
                    applied_at {timestamp_type} NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            if db.db_type == "sqlite":
                cursor.execute(
                    """
                    INSERT INTO database_agent_schema_metadata (
                        schema_name, schema_version, schema_sha256,
                        source_repository, applied_at
                    ) VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
                    ON CONFLICT(schema_name) DO UPDATE SET
                        schema_version = excluded.schema_version,
                        schema_sha256 = excluded.schema_sha256,
                        source_repository = excluded.source_repository,
                        applied_at = CURRENT_TIMESTAMP
                    """,
                    (SCHEMA_NAME, SCHEMA_VERSION, SCHEMA_SHA256, SOURCE_REPOSITORY),
                )
            else:
                cursor.execute(
                    """
                    INSERT INTO database_agent_schema_metadata (
                        schema_name, schema_version, schema_sha256,
                        source_repository, applied_at
                    ) VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP)
                    ON CONFLICT (schema_name) DO UPDATE SET
                        schema_version = EXCLUDED.schema_version,
                        schema_sha256 = EXCLUDED.schema_sha256,
                        source_repository = EXCLUDED.source_repository,
                        applied_at = CURRENT_TIMESTAMP
                    """,
                    (SCHEMA_NAME, SCHEMA_VERSION, SCHEMA_SHA256, SOURCE_REPOSITORY),
                )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cursor.close()


def get_schema_identity(db) -> Dict[str, Any]:
    """Return the canonical marker without exposing connection information."""

    with db.connection_scope() as conn:
        cursor = db.get_cursor(conn)
        try:
            cursor.execute(
                f"""
                SELECT schema_name, schema_version, schema_sha256,
                       source_repository, applied_at
                FROM database_agent_schema_metadata
                WHERE schema_name = {db.param_style}
                """,
                (SCHEMA_NAME,),
            )
            row = cursor.fetchone()
            if row is None:
                return {}
            if isinstance(row, dict):
                return dict(row)
            try:
                return dict(row)
            except (TypeError, ValueError):
                return {
                    "schema_name": row[0],
                    "schema_version": row[1],
                    "schema_sha256": row[2],
                    "source_repository": row[3],
                    "applied_at": row[4],
                }
        finally:
            cursor.close()


def schema_identity_matches(db) -> bool:
    identity = get_schema_identity(db)
    return (
        identity.get("schema_name") == SCHEMA_NAME
        and identity.get("schema_version") == SCHEMA_VERSION
        and identity.get("schema_sha256") == SCHEMA_SHA256
    )
