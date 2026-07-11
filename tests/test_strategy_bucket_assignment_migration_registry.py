from position_bucket_repository import _configured_strategy_bucket_assignments


EXPECTED = {
    "ACGL": "value_rebound",
    "ADBE": "value_rebound",
    "BKNG": "value_rebound",
    "CINF": "value_rebound",
}


def _as_map(rows):
    return {row["symbol"]: row["strategy_bucket"] for row in rows}


def test_compact_seed_restores_all_confirmed_held_positions(monkeypatch):
    monkeypatch.setenv(
        "STRATEGY_BUCKET_ASSIGNMENTS_JSON",
        "ACGL=value_rebound,ADBE=value_rebound,BKNG=value_rebound,CINF=value_rebound",
    )
    monkeypatch.setenv("STRATEGY_BUCKET_ASSIGNMENTS_ACCOUNT_ID", "1")

    rows = _configured_strategy_bucket_assignments()

    assert _as_map(rows) == EXPECTED
    assert {row["account_id"] for row in rows} == {1}
    assert {row["source"] for row in rows} == {"deployment_seed"}


def test_versioned_json_seed_preserves_audit_metadata(monkeypatch):
    monkeypatch.setenv(
        "STRATEGY_BUCKET_ASSIGNMENTS_JSON",
        """[
          {
            "account_id": 1,
            "symbol": "CINF",
            "strategy_bucket": "value_rebound",
            "source": "manager-strategy-bucket-v3-held-position-migration",
            "reason": "confirmed pre-existing Alpaca Paper holding"
          }
        ]""",
    )

    rows = _configured_strategy_bucket_assignments()

    assert rows == [
        {
            "account_id": 1,
            "symbol": "CINF",
            "strategy_bucket": "value_rebound",
            "source": "manager-strategy-bucket-v3-held-position-migration",
            "reason": "confirmed pre-existing Alpaca Paper holding",
        }
    ]


def test_invalid_or_unassigned_seed_entries_are_not_persisted(monkeypatch):
    monkeypatch.setenv(
        "STRATEGY_BUCKET_ASSIGNMENTS_JSON",
        "ACGL=unknown_bucket,ADBE=unassigned,CINF=value_rebound",
    )

    rows = _configured_strategy_bucket_assignments()

    assert _as_map(rows) == {"CINF": "value_rebound"}
