import os
from datetime import datetime, timezone
from unittest.mock import patch

os.environ.setdefault("ALPACA_API_KEY", "test-alpaca-key")
os.environ.setdefault("ALPACA_SECRET_KEY", "test-alpaca-secret")

from fastapi.testclient import TestClient

import main


client = TestClient(main.app)


def test_session_risk_endpoint_returns_snapshot():
    generated_at = datetime(2026, 6, 21, 15, 0, tzinfo=timezone.utc).isoformat()
    snapshot = {
        "account_id": 1,
        "symbol": "AAPL",
        "date": "2026-06-21",
        "daily_realized_pnl": -12.5,
        "weekly_realized_pnl": -20.0,
        "consecutive_losses": 1,
        "trades_today": 2,
        "symbol_trades_today": 1,
        "minutes_since_last_loss": 80,
        "minutes_since_last_symbol_trade": 45,
        "emergency_halt": False,
        "source": "database_agent",
        "generated_at": generated_at,
    }

    with patch.object(main, "DATABASE_AGENT_API_KEY", "test-key"), \
            patch.object(main, "build_session_risk_snapshot", return_value=snapshot) as build_snapshot:
        response = client.get(
            "/accounts/1/risk/session?symbol=AAPL",
            headers={"X-API-KEY": "test-key", "X-Correlation-ID": "corr-session-risk"},
        )

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "success"
    assert body["data"]["account_id"] == 1
    assert body["data"]["symbol"] == "AAPL"
    assert body["data"]["date"] == "2026-06-21"
    assert body["data"]["consecutive_losses"] == 1
    build_snapshot.assert_called_once_with(main.db, "1", symbol="AAPL", emergency_halt=main.DATABASE_EMERGENCY_HALT)


def test_health_exposes_database_emergency_halt_flag():
    with patch.object(main, "DATABASE_AGENT_API_KEY", "test-key"), \
            patch.object(main.db, "check_connection", return_value=True), \
            patch.object(main, "DATABASE_EMERGENCY_HALT", True):
        response = client.get("/health", headers={"X-API-KEY": "test-key"})

    assert response.status_code == 200
    assert response.json()["data"]["database_emergency_halt"] is True
