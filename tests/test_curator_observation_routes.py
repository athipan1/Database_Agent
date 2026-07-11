import os
import uuid

os.environ.setdefault("USE_SQLITE", "1")
os.environ.setdefault("DATABASE_DEV_MODE", "true")

from fastapi import FastAPI
from fastapi.testclient import TestClient

from curator_observation_routes import create_curator_observation_routes
from trading_db import TradingDB


def _app():
    db = TradingDB()
    db.setup_database()

    def api_key_dependency(value):
        return value or "test-key"

    async def correlation_id_dependency():
        return "route-correlation-id"

    app = FastAPI()
    app.include_router(
        create_curator_observation_routes(
            db,
            api_key_dependency,
            correlation_id_dependency,
        )
    )
    return app


def test_curator_observation_batch_and_readiness_endpoints():
    client = TestClient(_app())
    account_id = f"route-account-{uuid.uuid4()}"
    observation_id = str(uuid.uuid4())
    payload = {
        "observations": [
            {
                "observation_id": observation_id,
                "account_id": account_id,
                "symbol": "acgl",
                "mode": "shadow_ensemble",
                "status": "success",
                "available": True,
                "signal": "buy",
                "agreement": 0.8,
                "contract_valid": True,
                "would_pass_required_gate": True,
                "selected_skill_count": 3,
                "execution_count": 3,
                "minimum_agreement": 0.6,
                "rejection_codes": [],
                "metadata": {"schema": "curator_observation.v1"},
            },
            {
                "account_id": account_id,
                "symbol": "adbe",
                "mode": "shadow_ensemble",
                "status": "success",
                "available": True,
                "signal": "hold",
                "agreement": 0.5,
                "contract_valid": True,
                "would_pass_required_gate": False,
                "selected_skill_count": 2,
                "execution_count": 2,
                "minimum_agreement": 0.6,
                "rejection_codes": ["agreement_below_threshold"],
                "metadata": {"schema": "curator_observation.v1"},
            },
        ]
    }

    response = client.post("/curator/observations/batch", json=payload)
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "success"
    assert body["data"]["created_count"] == 2
    assert body["data"]["observations"][0]["symbol"] == "ACGL"

    duplicate = client.post(
        "/curator/observations",
        json=payload["observations"][0],
    )
    assert duplicate.status_code == 200
    assert duplicate.json()["data"]["observation_id"] == observation_id

    listed = client.get(
        "/curator/observations",
        params={"account_id": account_id, "mode": "shadow_ensemble"},
    )
    assert listed.status_code == 200
    assert len(listed.json()["data"]) == 2

    readiness = client.get(
        "/curator/observations/readiness",
        params={"account_id": account_id, "observation_target": 50},
    )
    assert readiness.status_code == 200
    summary = readiness.json()["data"]
    assert summary["observations"] == 2
    assert summary["would_pass_required_gate"] == 1
    assert summary["would_be_blocked"] == 1
    assert summary["required_mode_eligible"] is False
    assert "observations_below_target" in summary["blockers"]

    fetched = client.get(f"/curator/observations/{observation_id}")
    assert fetched.status_code == 200
    assert fetched.json()["data"]["symbol"] == "ACGL"
