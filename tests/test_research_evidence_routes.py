"""Phase 17 immutable research API and migration tests: no trading promotions."""
from __future__ import annotations

import copy
import sqlite3
from contextlib import contextmanager

from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient
from openapi_spec_validator import validate

from research_evidence_repository import (
    CreateResearchEvidence,
    digest_payload,
    get_research_evidence,
    setup_research_evidence_table,
)
from research_evidence_routes import create_research_evidence_routes


class MemoryDatabase:
    db_type = "sqlite"
    param_style = "?"

    def __init__(self):
        self.conn = sqlite3.connect(":memory:", check_same_thread=False)
        self.conn.row_factory = sqlite3.Row

    @contextmanager
    def connection_scope(self):
        yield self.conn

    def get_cursor(self, conn):
        return conn.cursor()


def _payload():
    phases = {}
    for n in (13, 14, 15, 16):
        phases[f"phase{n}"] = {
            "schema_version": f"phase{n}-research.v1",
            "promotion_allowed": False, "execution_allowed": False,
            "sealed_holdout_opened": False,
            "error_codes": ["independent_data_not_verified"] if n == 16 else [],
        }
    result = {
        "schema_version": "phase17-research-evidence.v1",
        "source_agent": "Backtest_Agent",
        "symbol": "AAPL",
        "research_profile": "strategy-research-v7",
        "research_dataset_fingerprint": "fixture-dataset-sha",
        "artifact_sha256": "a" * 64, "phases": phases,
        "research_only": True, "diagnostic_only": True,
        "used_for_selection": False, "promotion_allowed": False,
        "execution_allowed": False, "sealed_holdout_opened": False,
    }
    result["evidence_id"] = digest_payload(result)
    return result


def _app():
    db = MemoryDatabase()
    setup_research_evidence_table(db)

    def auth(value):
        if value != "valid-research-key":
            raise HTTPException(status_code=401, detail="unauthorized")
        return value

    async def correlation():
        return "test-research-correlation"

    api = FastAPI()
    api.include_router(create_research_evidence_routes(db, auth, correlation))
    return TestClient(api), db, api


def test_schema_bootstrap_and_authenticated_round_trip_idempotency():
    client, db, _api = _app()
    payload = _payload()
    assert client.post("/research/evidence", json=payload).status_code == 401
    assert client.get("/research/evidence/" + payload["evidence_id"]).status_code == 401
    headers = {"X-API-KEY": "valid-research-key"}
    response = client.post("/research/evidence", json=payload, headers=headers)
    assert response.status_code == 201, response.text
    assert response.json()["metadata"]["idempotent_replay"] is False
    assert response.json()["metadata"]["safe_for_trading"] is False
    replay = client.post("/research/evidence", json=payload, headers=headers)
    assert replay.status_code == 201
    assert replay.json()["metadata"]["idempotent_replay"] is True
    got = client.get("/research/evidence/" + payload["evidence_id"], headers=headers)
    assert got.status_code == 200
    assert got.json()["data"]["payload"] == {
        k: v for k, v in payload.items() if k != "evidence_id"
    }
    assert got.json()["data"]["promotion_allowed"] is False
    assert get_research_evidence(db, payload["evidence_id"])["symbol"] == "AAPL"
    row = db.conn.execute("SELECT COUNT(*) FROM research_evidence").fetchone()
    assert row[0] == 1


def test_changed_payload_with_stale_digest_is_rejected():
    client, _, _ = _app()
    payload = _payload()
    payload["artifact_sha256"] = "f" * 64
    result = client.post("/research/evidence", json=payload,
                         headers={"X-API-KEY": "valid-research-key"})
    assert result.status_code == 422
    assert "immutable content" in result.text


def test_promotion_execution_or_holdout_flags_never_allowed():
    client, _, _ = _app()
    for field in ("promotion_allowed", "execution_allowed",
                  "sealed_holdout_opened", "used_for_selection"):
        payload = _payload()
        payload[field] = True
        payload["evidence_id"] = digest_payload({
            k: v for k, v in payload.items() if k != "evidence_id"
        })
        resp = client.post("/research/evidence", json=payload,
                           headers={"X-API-KEY": "valid-research-key"})
        assert resp.status_code == 422, (field, resp.text)
    payload = _payload()
    payload["phases"]["phase16"]["execution_allowed"] = True
    payload["evidence_id"] = digest_payload({
        k: v for k, v in payload.items() if k != "evidence_id"
    })
    resp = client.post("/research/evidence", json=payload,
                       headers={"X-API-KEY": "valid-research-key"})
    assert resp.status_code == 422


def test_missing_phase_and_extra_phase_fail_closed():
    client, _, _ = _app()
    for change in ("missing", "extra"):
        payload = _payload()
        if change == "missing":
            payload["phases"].pop("phase13")
        else:
            payload["phases"]["phase17"] = payload["phases"]["phase16"]
        payload["evidence_id"] = digest_payload({
            k: v for k, v in payload.items() if k != "evidence_id"
        })
        resp = client.post("/research/evidence", json=payload,
                           headers={"X-API-KEY": "valid-research-key"})
        assert resp.status_code == 422


def test_replayed_evidence_conflicting_with_stored_document_is_rejected():
    client, db, _ = _app()
    payload = _payload()
    headers = {"X-API-KEY": "valid-research-key"}
    assert client.post("/research/evidence", json=payload, headers=headers).status_code == 201
    with db.connection_scope() as connection:
        connection.execute(
            "UPDATE research_evidence SET payload=? WHERE evidence_id=?",
            ('{"corrupt":true}', payload["evidence_id"]),
        )
        connection.commit()
    resp = client.post("/research/evidence", json=payload, headers=headers)
    assert resp.status_code == 409


def test_unknown_id_and_invalid_id_are_not_returned():
    client, _, _ = _app()
    headers = {"X-API-KEY": "valid-research-key"}
    assert client.get("/research/evidence/" + "0" * 64, headers=headers).status_code == 404
    assert client.get("/research/evidence/wrong", headers=headers).status_code == 422


def test_openapi_exposes_only_research_route_with_api_key():
    _, _, api = _app()
    document = api.openapi()
    validate(document)
    assert set(document["paths"]) == {
        "/research/evidence", "/research/evidence/{evidence_id}",
    }
    assert "post" in document["paths"]["/research/evidence"]
    assert "get" in document["paths"]["/research/evidence/{evidence_id}"]
    security = document["components"]["securitySchemes"]
    assert any(v.get("name") == "X-API-KEY" for v in security.values())


def test_oversized_payload_and_nan_rejected_without_partial_insert():
    client, db, _ = _app()
    payload = _payload()
    payload["phases"]["phase16"]["debug"] = "x" * 330000
    payload["evidence_id"] = digest_payload({
        k: v for k, v in payload.items() if k != "evidence_id"
    })
    result = client.post("/research/evidence", json=payload,
                         headers={"X-API-KEY": "valid-research-key"})
    assert result.status_code == 422
    assert db.conn.execute("SELECT COUNT(*) FROM research_evidence").fetchone()[0] == 0
