#!/usr/bin/env python3
"""Compare source and target table counts without reading row payloads."""

from __future__ import annotations

import json
import os
from pathlib import Path

import psycopg2
from psycopg2 import sql


MANIFEST_PATH = Path(__file__).resolve().parents[1] / "supabase" / "schema_manifest.json"


def _count_tables(database_url: str, tables: list[str]) -> dict[str, int]:
    connection = psycopg2.connect(
        database_url,
        connect_timeout=10,
        application_name="database-agent-cutover-counts",
    )
    try:
        connection.set_session(readonly=True, isolation_level="REPEATABLE READ")
        counts: dict[str, int] = {}
        with connection.cursor() as cursor:
            for table in tables:
                cursor.execute(
                    sql.SQL("SELECT count(*) FROM public.{}")
                    .format(sql.Identifier(table))
                )
                counts[table] = int(cursor.fetchone()[0])
        return counts
    finally:
        connection.close()


def main() -> int:
    source_url = os.environ.get("SOURCE_DATABASE_URL")
    target_url = os.environ.get("TARGET_DATABASE_URL")
    if not source_url or not target_url:
        raise SystemExit("SOURCE_DATABASE_URL and TARGET_DATABASE_URL are required")

    manifest = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
    # Parent `prices` includes all partition rows, so leaf partitions are excluded
    # to avoid counting the same market bars twice in the comparison report.
    tables = [
        table
        for table in manifest["tables"]
        if not table.startswith("prices_y")
        and table != "database_agent_schema_metadata"
    ]
    source = _count_tables(source_url, tables)
    target = _count_tables(target_url, tables)
    differences = {
        table: {"source": source[table], "target": target[table]}
        for table in tables
        if source[table] != target[table]
    }
    report = {
        "status": "success" if not differences else "failure",
        "tables_checked": len(tables),
        "source_total_rows": sum(source.values()),
        "target_total_rows": sum(target.values()),
        "differences": differences,
    }
    print(json.dumps(report, sort_keys=True))
    return 0 if not differences else 1


if __name__ == "__main__":
    raise SystemExit(main())
