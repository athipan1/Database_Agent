"""Database runtime startup and shutdown orchestration.

Production startup is intentionally read-only with respect to schema state.
DDL belongs to the deployment migration command, not the application lifespan.
"""

from __future__ import annotations

import logging

from starlette.concurrency import run_in_threadpool

from schema_identity_repository import (
    SCHEMA_NAME,
    SCHEMA_SHA256,
    SCHEMA_VERSION,
    get_schema_identity,
)


def verify_runtime_schema(db) -> dict:
    """Fail closed unless the deployed database matches this release schema.

    This function performs SELECT-only schema identity validation. It must never
    create, alter, repair, or otherwise mutate database schema state.
    """

    identity = get_schema_identity(db)
    if not identity:
        raise RuntimeError(
            "Database schema identity is missing. Run deployment migrations before startup."
        )

    mismatches = []
    if identity.get("schema_name") != SCHEMA_NAME:
        mismatches.append(
            f"schema_name={identity.get('schema_name')!r} expected={SCHEMA_NAME!r}"
        )
    if identity.get("schema_version") != SCHEMA_VERSION:
        mismatches.append(
            f"schema_version={identity.get('schema_version')!r} expected={SCHEMA_VERSION!r}"
        )
    if identity.get("schema_sha256") != SCHEMA_SHA256:
        mismatches.append("schema_sha256 does not match the release manifest")

    if mismatches:
        raise RuntimeError(
            "Database schema identity mismatch; run deployment migrations before startup: "
            + "; ".join(mismatches)
        )
    return identity


def log_database_stats(db) -> None:
    logging.info("Collecting database statistics.")
    try:
        stats = db.get_database_stats()
        if stats:
            logging.info("Database Statistics", extra={"db_stats": stats})
    except Exception as exc:
        logging.warning("Could not collect database stats: %s", exc)


async def startup_runtime(runtime) -> None:
    logging.info("Database Agent API starting up.")
    try:
        # psycopg2 is synchronous. Keep the single read-only schema identity
        # query off the event loop, but do not run DDL or self-healing here.
        identity = await run_in_threadpool(verify_runtime_schema, runtime.db)
        logging.info(
            "Database schema identity verified.",
            extra={
                "schema_name": identity.get("schema_name"),
                "schema_version": identity.get("schema_version"),
            },
        )
        runtime.runtime_scheduler.configure(
            ingestion_job=runtime.run_ingestion_job,
            stats_job=runtime.log_database_stats,
        )
        runtime.runtime_scheduler.start()
    except Exception as exc:
        logging.critical(
            "FATAL: Application startup failed: %s",
            exc,
            exc_info=True,
        )
        if not runtime.DATABASE_DEV_MODE:
            raise
        logging.warning(
            "DATABASE_DEV_MODE is enabled, continuing startup with fallback responses."
        )


async def shutdown_runtime(runtime) -> None:
    logging.info("Database Agent API shutting down.")
    runtime.runtime_scheduler.stop()
