import json
from pathlib import Path


def test_railway_runs_database_migration_before_release_start():
    config = json.loads(Path("railway.json").read_text(encoding="utf-8"))
    commands = config["deploy"]["preDeployCommand"]
    assert commands == ["python -m scripts.apply_runtime_migrations"]
