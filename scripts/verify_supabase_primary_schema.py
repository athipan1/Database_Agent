#!/usr/bin/env python3
"""Verify the remote Supabase primary without printing connection information."""

from __future__ import annotations

import json
import os
from pathlib import Path

import psycopg2
import psycopg2.extras


MANIFEST_PATH = Path(__file__).resolve().parents[1] / "supabase" / "schema_manifest.json"


def _fetch_one(cursor, query: str, parameters=()):
    cursor.execute(query, parameters)
    row = cursor.fetchone()
    return dict(row) if row else {}


def main() -> int:
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        raise SystemExit("DATABASE_URL is required")

    manifest = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
    expected_tables = set(manifest["tables"])
    connection = psycopg2.connect(
        database_url,
        sslmode=os.environ.get("DATABASE_SSL_MODE", "require"),
        connect_timeout=int(os.environ.get("DATABASE_CONNECT_TIMEOUT_SECONDS", "10")),
        application_name="database-agent-schema-verifier",
    )
    try:
        with connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
            cursor.execute(
                """
                SELECT c.relname AS table_name, c.relrowsecurity AS rls_enabled
                FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = 'public' AND c.relkind IN ('r', 'p')
                """
            )
            table_rows = [dict(row) for row in cursor.fetchall()]
            actual_tables = {row["table_name"] for row in table_rows}
            rls_tables = {
                row["table_name"] for row in table_rows if row["rls_enabled"]
            }

            counters = _fetch_one(
                cursor,
                """
                SELECT
                    (SELECT count(*) FROM pg_inherits i
                     JOIN pg_class parent ON parent.oid = i.inhparent
                     JOIN pg_namespace n ON n.oid = parent.relnamespace
                     WHERE n.nspname = 'public' AND parent.relname = 'prices')
                        AS price_partitions,
                    (SELECT count(*) FROM pg_constraint con
                     JOIN pg_class c ON c.oid = con.conrelid
                     JOIN pg_namespace n ON n.oid = c.relnamespace
                     WHERE n.nspname = 'public') AS constraints,
                    (SELECT count(*) FROM pg_indexes
                     WHERE schemaname = 'public') AS indexes,
                    (SELECT count(*) FROM pg_trigger t
                     JOIN pg_class c ON c.oid = t.tgrelid
                     JOIN pg_namespace n ON n.oid = c.relnamespace
                     WHERE n.nspname = 'public' AND NOT t.tgisinternal)
                        AS custom_triggers
                """,
            )
            identity = _fetch_one(
                cursor,
                """
                SELECT schema_name, schema_version, schema_sha256
                FROM public.database_agent_schema_metadata
                WHERE schema_name = %s
                """,
                (manifest["schema_name"],),
            )
            ssl_state = _fetch_one(
                cursor,
                "SELECT COALESCE((SELECT ssl FROM pg_stat_ssl WHERE pid = pg_backend_pid()), false) AS ssl",
            )

            exposed = {}
            for role in ("anon", "authenticated", "service_role"):
                cursor.execute(
                    """
                    SELECT count(*) AS exposed
                    FROM pg_class c
                    JOIN pg_namespace n ON n.oid = c.relnamespace
                    WHERE n.nspname = 'public'
                      AND c.relkind IN ('r', 'p')
                      AND has_table_privilege(%s, c.oid, 'SELECT,INSERT,UPDATE,DELETE')
                    """,
                    (role,),
                )
                exposed[role] = int(cursor.fetchone()["exposed"])

        checks = {
            "expected_tables_present": expected_tables.issubset(actual_tables),
            "table_count": len(actual_tables) >= int(manifest["expected_table_count"]),
            "rls_enabled": expected_tables.issubset(rls_tables),
            "price_partitions": int(counters["price_partitions"])
            == int(manifest["expected_price_partition_count"]),
            "constraints": int(counters["constraints"])
            >= int(manifest["minimum_constraint_count"]),
            "indexes": int(counters["indexes"])
            >= int(manifest["minimum_index_count"]),
            "custom_triggers": int(counters["custom_triggers"])
            == int(manifest["expected_custom_trigger_count"]),
            "schema_identity": identity.get("schema_version")
            == manifest["schema_version"]
            and identity.get("schema_sha256") == manifest["schema_sha256"],
            "ssl": bool(ssl_state.get("ssl")),
            "data_api_denied": all(value == 0 for value in exposed.values()),
        }
        report = {
            "status": "success" if all(checks.values()) else "failure",
            "checks": checks,
            "counts": {
                "tables": len(actual_tables),
                "price_partitions": int(counters["price_partitions"]),
                "constraints": int(counters["constraints"]),
                "indexes": int(counters["indexes"]),
                "custom_triggers": int(counters["custom_triggers"]),
            },
            "data_api_exposed_tables": exposed,
            "missing_tables": sorted(expected_tables - actual_tables),
            "schema_version": identity.get("schema_version"),
        }
        print(json.dumps(report, sort_keys=True))
        return 0 if report["status"] == "success" else 1
    finally:
        connection.close()


if __name__ == "__main__":
    raise SystemExit(main())
