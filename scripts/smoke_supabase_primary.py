#!/usr/bin/env python3
"""Exercise a non-trading write inside a transaction that is always rolled back."""

from __future__ import annotations

import json
import os
import uuid
from datetime import datetime, timezone

import psycopg2


def main() -> int:
    database_url = os.environ.get("TARGET_DATABASE_URL") or os.environ.get("DATABASE_URL")
    if not database_url:
        raise SystemExit("TARGET_DATABASE_URL or DATABASE_URL is required")

    smoke_id = f"cutover-smoke-{uuid.uuid4()}"
    connection = psycopg2.connect(
        database_url,
        sslmode=os.environ.get("DATABASE_SSL_MODE", "require"),
        connect_timeout=10,
        application_name="database-agent-cutover-smoke",
    )
    try:
        connection.autocommit = False
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO public.signal_history (
                    signal_id, account_id, symbol, timestamp, source_agent,
                    final_verdict, metadata
                ) VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb)
                """,
                (
                    smoke_id,
                    "cutover-smoke",
                    "SMOKE",
                    datetime.now(timezone.utc),
                    "database-agent-cutover-smoke",
                    "hold",
                    json.dumps({"temporary": True}),
                ),
            )
            cursor.execute(
                "SELECT count(*) FROM public.signal_history WHERE signal_id = %s",
                (smoke_id,),
            )
            inserted = int(cursor.fetchone()[0]) == 1
        connection.rollback()

        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT count(*) FROM public.signal_history WHERE signal_id = %s",
                (smoke_id,),
            )
            persisted = int(cursor.fetchone()[0])
        connection.rollback()

        result = {
            "status": "success" if inserted and persisted == 0 else "failure",
            "insert_verified": inserted,
            "rollback_verified": persisted == 0,
        }
        print(json.dumps(result, sort_keys=True))
        return 0 if result["status"] == "success" else 1
    finally:
        connection.close()


if __name__ == "__main__":
    raise SystemExit(main())
