import os

os.environ.setdefault("USE_SQLITE", "1")
os.environ.setdefault("ALPACA_API_KEY", "test-alpaca-key")
os.environ.setdefault("ALPACA_SECRET_KEY", "test-alpaca-secret")

from fastapi import FastAPI
from fastapi.testclient import TestClient

from backtest_routes import create_backtest_routes
from trading_db import TradingDB


HEADERS = {"X-API-KEY": "test-key", "X-Correlation-ID": "corr-backtest"}


def _build_client():
    db = TradingDB()

    def get_api_key(api_key):
        assert api_key == "test-key"
        return api_key

    async def get_correlation_id():
        return "corr-backtest"

    app = FastAPI()
    app.include_router(create_backtest_routes(db, get_api_key, get_correlation_id))
    return TestClient(app)


def test_market_data_bars_round_trip():
    client = _build_client()
    response = client.post(
        "/market-data/bars",
        headers=HEADERS,
        json={
            "bars": [
                {
                    "symbol": "aapl",
                    "timeframe": "1d",
                    "bar_time": "2026-01-02T00:00:00Z",
                    "open": 100,
                    "high": 110,
                    "low": 95,
                    "close": 108,
                    "volume": 12345,
                    "source": "unit-test",
                    "metadata": {"provider": "fixture"},
                }
            ]
        },
    )

    assert response.status_code == 200
    assert response.json()["data"][0]["symbol"] == "AAPL"

    response = client.get("/market-data/bars?symbol=AAPL&timeframe=1d", headers=HEADERS)

    assert response.status_code == 200
    data = response.json()["data"]
    assert len(data) == 1
    assert data[0]["close"] == 108
    assert data[0]["metadata"] == {"provider": "fixture"}


def test_backtest_run_creates_skill_status():
    client = _build_client()
    response = client.post(
        "/backtests/runs",
        headers=HEADERS,
        json={
            "run_id": "run-1",
            "account_id": "1",
            "skill_id": "skill-1",
            "strategy_id": "strategy-alpha",
            "symbol": "aapl",
            "timeframe": "1d",
            "start_time": "2026-01-01T00:00:00Z",
            "end_time": "2026-03-01T00:00:00Z",
            "status": "completed",
            "engine_version": "backtest-agent-test",
            "metrics": {
                "win_rate": 0.55,
                "profit_factor": 1.45,
                "expectancy": 0.25,
                "max_drawdown": 0.12,
                "total_trades": 24,
            },
            "trades": [
                {
                    "symbol": "AAPL",
                    "side": "buy",
                    "quantity": 10,
                    "entry_price": 100,
                    "exit_price": 110,
                    "realized_pl": 100,
                    "outcome": "win",
                }
            ],
            "equity_curve": [
                {
                    "timestamp": "2026-01-01T00:00:00Z",
                    "equity": 100000,
                    "drawdown": 0,
                }
            ],
        },
    )

    assert response.status_code == 200
    detail = response.json()["data"]
    assert detail["run"]["run_id"] == "run-1"
    assert detail["skill_result"]["passed"] is True
    assert detail["skill_result"]["status"] == "backtest_passed"

    response = client.get("/backtests/runs/run-1", headers=HEADERS)
    assert response.status_code == 200
    assert response.json()["data"]["trades"][0]["realized_pl"] == 100

    response = client.get("/skills/skill-1/backtest-status", headers=HEADERS)
    assert response.status_code == 200
    status = response.json()["data"]
    assert status["skill_id"] == "skill-1"
    assert status["passed"] is True
    assert status["latest_run_id"] == "run-1"


def test_unknown_skill_backtest_status_is_not_backtested():
    client = _build_client()
    response = client.get("/skills/missing-skill/backtest-status", headers=HEADERS)

    assert response.status_code == 200
    data = response.json()["data"]
    assert data["status"] == "not_backtested"
    assert data["passed"] is False
