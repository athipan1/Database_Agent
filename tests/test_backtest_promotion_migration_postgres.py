from __future__ import annotations

import os
import uuid
from pathlib import Path

import pytest


@pytest.mark.skipif(
    not os.getenv("POSTGRES_HOST"),
    reason="PostgreSQL service is not configured for this test run",
)
def test_promotion_migration_upgrade_and_downgrade_round_trip():
    import psycopg2

    root = Path(__file__).resolve().parents[1]
    upgrade = (
        root / "migrations/003_backtest_promotion_lifecycle.up.sql"
    ).read_text(encoding="utf-8")
    downgrade = (
        root / "migrations/003_backtest_promotion_lifecycle.down.sql"
    ).read_text(encoding="utf-8")
    schema = "promotion_test_" + uuid.uuid4().hex[:12]
    connection = psycopg2.connect(
        host=os.environ["POSTGRES_HOST"],
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        dbname=os.environ["POSTGRES_DB"],
    )
    connection.autocommit = True
    try:
        with connection.cursor() as cursor:
            cursor.execute(f'CREATE SCHEMA "{schema}"')
            cursor.execute(f'SET search_path TO "{schema}"')
            cursor.execute(upgrade)
            cursor.execute(
                "SELECT to_regclass('backtest_promotions'), "
                "to_regclass('backtest_promotion_transitions')"
            )
            assert cursor.fetchone() == (
                "backtest_promotions",
                "backtest_promotion_transitions",
            )
            cursor.execute(downgrade)
            cursor.execute(
                "SELECT to_regclass('backtest_promotions'), "
                "to_regclass('backtest_promotion_transitions')"
            )
            assert cursor.fetchone() == (None, None)
    finally:
        with connection.cursor() as cursor:
            cursor.execute("SET search_path TO public")
            cursor.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        connection.close()
