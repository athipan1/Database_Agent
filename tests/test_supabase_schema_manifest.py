from __future__ import annotations

import json
from pathlib import Path

from schema_identity_repository import SCHEMA_NAME, SCHEMA_SHA256, SCHEMA_VERSION


ROOT = Path(__file__).resolve().parents[1]
MANIFEST = ROOT / "supabase" / "schema_manifest.json"
SECURITY_MIGRATION = (
    ROOT / "supabase" / "migrations" / "2026072501_secure_database_agent_primary.sql"
)


def test_schema_manifest_matches_runtime_identity_and_core_contract():
    manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))

    assert manifest["schema_name"] == SCHEMA_NAME
    assert manifest["schema_version"] == SCHEMA_VERSION
    assert manifest["schema_sha256"] == SCHEMA_SHA256
    assert len(manifest["schema_sha256"]) == 64
    assert manifest["expected_table_count"] == len(manifest["tables"])
    assert manifest["expected_price_partition_count"] == 38

    required = {
        "accounts",
        "positions",
        "orders",
        "fills",
        "execution_jobs",
        "risk_approvals",
        "profit_decisions",
        "signal_history",
        "performance_metrics",
        "database_agent_schema_metadata",
    }
    assert required.issubset(set(manifest["tables"]))

    price_partitions = [
        name for name in manifest["tables"] if name.startswith("prices_y")
    ]
    assert len(price_partitions) == manifest["expected_price_partition_count"]


def test_security_migration_denies_data_api_and_enables_rls():
    sql = SECURITY_MIGRATION.read_text(encoding="utf-8").lower()

    assert "revoke all privileges on all tables" in sql
    assert "anon, authenticated, service_role" in sql
    assert "enable row level security" in sql
    assert "using (false) with check (false)" in sql
    assert "set search_path = public, pg_temp" in sql
    assert "grant all" not in sql
