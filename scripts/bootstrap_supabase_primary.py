#!/usr/bin/env python3
"""Bootstrap the canonical Database_Agent schema on managed Supabase PostgreSQL."""

from __future__ import annotations

import json
import os

from app.core.config import Settings
from app.services.database_provider import create_trading_db
from app.startup import setup_runtime_tables
from schema_identity_repository import get_schema_identity, schema_identity_matches


def main() -> int:
    settings = Settings.from_environ()
    settings.validate()
    if settings.database_provider != "supabase":
        raise SystemExit("DATABASE_PROVIDER must be supabase for this bootstrap")
    if not os.environ.get("DATABASE_URL"):
        raise SystemExit("DATABASE_URL is required")

    database = create_trading_db(settings)
    try:
        setup_runtime_tables(database)
        if not database.check_connection():
            raise SystemExit("database connection verification failed")
        identity = get_schema_identity(database)
        if not schema_identity_matches(database):
            raise SystemExit("canonical schema identity did not match")
        print(
            json.dumps(
                {
                    "status": "success",
                    "provider": settings.database_provider,
                    "schema_name": identity.get("schema_name"),
                    "schema_version": identity.get("schema_version"),
                    "schema_sha256": identity.get("schema_sha256"),
                },
                sort_keys=True,
            )
        )
        return 0
    finally:
        if database.pool is not None:
            database.pool.closeall()


if __name__ == "__main__":
    raise SystemExit(main())
