import os
from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import patch

os.environ.setdefault("ALPACA_API_KEY", "test-alpaca-key")
os.environ.setdefault("ALPACA_SECRET_KEY", "test-alpaca-secret")

from fastapi.testclient import TestClient

import main

client = TestClient(main.app)


def test_create_fill_endpoint_returns_realized_pnl():
    fill = {
        "fill_id": 1,
        "account_id": 1,
        "order_id": 10,
        "trade_id": "t-1",
        "symbol": "AAPL",
        "side": "sell",
        "quantity": 2,
        "fill_price": Decimal("110"),
        "average_entry_price": Decimal("100"),
        "gross_pnl": Decimal("20"),
        "fees": Decimal("1"),
        "realized_pnl": Decimal("19"),
        "broker_fill_id": "bf-1",
        "broker_order_id": "bo-1",
        "liquidity": "taker",
        "filled_at": datetime(2026, 6, 21, 15, 0, tzinfo=timezone.utc),
        "correlation_id": "corr-1",
        "metadata": {"source": "test"},
        "created_at": datetime(2026, 6, 21, 15, 0, tzinfo=timezone.utc),
    }

    with patch.object(main, "DATABASE_AGENT_API_KEY", "test-key"), \
            patch.object(main, "create_fill_record", return_value=fill) as create_fill:
        response = client.post(
            "/accounts/1/fills",
            json={
                "order_id": 10,
                "trade_id": "t-1",
                "symbol": "AAPL",
                "side": "sell",
                "quantity": 2,
                "fill_price": 110,
                "average_entry_price": 100,
                "fees": 1,
                "broker_fill_id": "bf-1",
                "broker_order_id": "bo-1",
                "liquidity": "taker",
                "metadata": {"source": "test"},
            },
            headers={"X-API-KEY": "test-key", "X-Correlation-ID": "corr-1"},
        )

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "success"
    assert body["data"]["realized_pnl"] == 19.0
    create_fill.assert_called_once()


def test_get_fills_endpoint_returns_records():
    fill = {
        "fill_id": 1,
        "account_id": 1,
        "order_id": 10,
        "trade_id": "t-1",
        "symbol": "AAPL",
        "side": "sell",
        "quantity": 2,
        "fill_price": Decimal("110"),
        "average_entry_price": Decimal("100"),
        "gross_pnl": Decimal("20"),
        "fees": Decimal("1"),
        "realized_pnl": Decimal("19"),
        "broker_fill_id": "bf-1",
        "broker_order_id": "bo-1",
        "liquidity": "taker",
        "filled_at": datetime(2026, 6, 21, 15, 0, tzinfo=timezone.utc),
        "correlation_id": "corr-1",
        "metadata": {},
        "created_at": datetime(2026, 6, 21, 15, 0, tzinfo=timezone.utc),
    }
    with patch.object(main, "DATABASE_AGENT_API_KEY", "test-key"), \
            patch.object(main, "get_fill_records", return_value=[fill]) as get_fills:
        response = client.get("/accounts/1/fills?symbol=AAPL", headers={"X-API-KEY": "test-key"})

    assert response.status_code == 200
    assert response.json()["data"][0]["realized_pnl"] == 19.0
    get_fills.assert_called_once_with(main.db, "1", symbol="AAPL", limit=100)
