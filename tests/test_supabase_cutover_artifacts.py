from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_migration_script_requires_confirmation_and_protects_target_state():
    script = (ROOT / "scripts" / "migrate_postgres_to_supabase.sh").read_text(
        encoding="utf-8"
    )

    assert "MIGRATE_TO_SUPABASE" in script
    assert "target core tables are not empty" in script
    assert "--exclude-table-data=public.database_agent_schema_metadata" in script
    assert "DISABLE TRIGGER USER" not in script  # action is passed safely as a value
    assert "set_user_triggers DISABLE" in script
    assert "set_user_triggers ENABLE" in script
    assert "--single-transaction" in script
    assert "--exit-on-error" in script


def test_container_healthcheck_uses_provider_aware_readiness():
    dockerfile = (ROOT / "Dockerfile").read_text(encoding="utf-8")

    assert "http://localhost:8004/ready" in dockerfile
    assert "http://localhost:8004/health" not in dockerfile


def test_manual_smoke_is_synthetic_and_never_marks_data_safe_for_trading():
    script = (ROOT / "scripts" / "database_primary_api_smoke.py").read_text(
        encoding="utf-8"
    )

    assert '"symbol": "ZZTEST"' in script
    assert '"synthetic": True' in script
    assert '"safe_for_trading": False' in script
    assert "DATABASE_AGENT_API_KEY" in script
    assert "DATABASE_EXPECTED_PROVIDER" in script
