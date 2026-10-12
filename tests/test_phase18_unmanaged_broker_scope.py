"""Phase 18 broker parity with explicitly excluded, non-AI Paper holdings."""
import json

import pytest

from broker_sync_repository import sync_broker_state
from broker_sync_status_repository import broker_sync_status
from tests.test_broker_sync_status_repository import SQLiteStatusTestDB, broker_state


def _with_external_holding():
    state = broker_state()
    state["positions"].append({
        "symbol": "AGCO",
        "qty": "3",
        "avg_entry_price": "50",
        "current_price": "51",
        "market_value": "153",
    })
    state["summary"]["position_count"] = 3
    return state


def test_unregistered_external_position_fails_closed(monkeypatch):
    monkeypatch.delenv("UNMANAGED_BROKER_POSITION_SYMBOLS_JSON", raising=False)
    db = SQLiteStatusTestDB()
    sync_broker_state(db, _with_external_holding())
    result = broker_sync_status(db, account_id=1)
    assert result["mismatch"]["is_synced"] is False
    assert result["mismatch"]["diagnostics"]["strategy_buckets"]["unassigned_positions"] == ["AGCO"]


def test_approved_external_position_does_not_bypass_full_account_parity(monkeypatch):
    monkeypatch.setenv("UNMANAGED_BROKER_POSITION_SYMBOLS_JSON", json.dumps({"1": ["AGCO"]}))
    db = SQLiteStatusTestDB()
    sync_broker_state(db, _with_external_holding())
    result = broker_sync_status(db, account_id=1)
    assert result["mismatch"]["is_synced"] is True
    scope = result["mismatch"]["diagnostics"]["strategy_buckets"]
    assert scope["configured_unmanaged_symbols"] == ["AGCO"]
    assert scope["unmanaged_positions"] == ["AGCO"]
    assert scope["unassigned_positions"] == []
    assert result["unmanaged_exposure"]["market_value"] == "153.00"
    assert result["bucket_exposure"] != result["managed_bucket_exposure"]

    db.conn.execute("UPDATE positions SET quantity = 99 WHERE symbol = 'AGCO'")
    db.conn.commit()
    mismatched = broker_sync_status(db, account_id=1)
    assert mismatched["mismatch"]["is_synced"] is False
    assert "positions" in {x["field"] for x in mismatched["mismatch"]["mismatches"]}


def test_explicit_external_ownership_conflicts_with_strategy_assignment(monkeypatch):
    monkeypatch.setenv("UNMANAGED_BROKER_POSITION_SYMBOLS_JSON", '{"1":["AAPL"]}')
    db = SQLiteStatusTestDB()
    sync_broker_state(db, broker_state())
    result = broker_sync_status(db, account_id=1)
    assert result["mismatch"]["is_synced"] is False
    assert "unmanaged_strategy_bucket_conflicts" in {
        x["field"] for x in result["mismatch"]["mismatches"]
    }


@pytest.mark.parametrize("invalid", ['{invalid', '{"1":"AGCO"}', '{"1":["agco"]}', '{"1":["AGCO","AGCO"]}'])
def test_invalid_external_scope_fails_closed(monkeypatch, invalid):
    monkeypatch.setenv("UNMANAGED_BROKER_POSITION_SYMBOLS_JSON", invalid)
    db = SQLiteStatusTestDB()
    sync_broker_state(db, broker_state())
    with pytest.raises(ValueError):
        broker_sync_status(db, account_id=1)


@pytest.mark.parametrize("snapshot_override", [
    {"paper": False},
    {"broker": "OTHER", "paper": True},
])
def test_external_scope_requires_verified_alpaca_paper_snapshot(monkeypatch, snapshot_override):
    monkeypatch.setenv("UNMANAGED_BROKER_POSITION_SYMBOLS_JSON", '{"1":["AGCO"]}')
    db = SQLiteStatusTestDB()
    state = _with_external_holding()
    state.update(snapshot_override)
    sync_broker_state(db, state)
    status = broker_sync_status(db, account_id=1)
    assert status["mismatch"]["is_synced"] is False
    assert "unassigned_position_buckets" in {
        row["field"] for row in status["mismatch"]["mismatches"]
    }
    assert status["mismatch"]["diagnostics"]["strategy_buckets"]["configured_unmanaged_symbols"] == []
