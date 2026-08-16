from datetime import datetime, timezone, timedelta

from session_risk_repository import build_session_risk_snapshot


class FakeDB:
    def __init__(self, trades=None, orders=None, fills=None, managed_fill_ids=None):
        self.trades = trades or []
        self.orders = orders or []
        self.fills = fills or []
        self.managed_fill_ids = set(managed_fill_ids or [])

    def get_trade_history(self, account_id):
        return self.trades

    def get_orders(self, account_id):
        return self.orders

    def get_fills(self, account_id, limit=10000):
        return self.fills[:limit]

    def verify_system_managed_fill(self, account_id, fill):
        return fill.get("fill_id") in self.managed_fill_ids


def test_session_snapshot_calculates_daily_weekly_and_streak():
    now = datetime(2026, 6, 21, 15, 0, tzinfo=timezone.utc)
    trades = [
        {"symbol": "AAPL", "status": "executed", "executed_at": (now - timedelta(minutes=20)).isoformat(), "realized_pnl": -10},
        {"symbol": "AAPL", "status": "executed", "executed_at": (now - timedelta(hours=2)).isoformat(), "realized_pnl": -5},
        {"symbol": "MSFT", "status": "executed", "executed_at": (now - timedelta(days=2)).isoformat(), "realized_pnl": -20},
        {"symbol": "AAPL", "status": "executed", "executed_at": (now - timedelta(days=10)).isoformat(), "realized_pnl": -99},
    ]

    snapshot = build_session_risk_snapshot(FakeDB(trades=trades), 1, symbol="AAPL", now=now)

    assert snapshot["daily_realized_pnl"] == -15.0
    assert snapshot["weekly_realized_pnl"] == -35.0
    assert snapshot["consecutive_losses"] == 4
    assert snapshot["trades_today"] == 2
    assert snapshot["symbol_trades_today"] == 2
    assert snapshot["minutes_since_last_loss"] == 20
    assert snapshot["minutes_since_last_symbol_trade"] == 20
    assert snapshot["system_provenance_verified"] is False
    assert snapshot["system_managed_trades_today"] == 0
    assert snapshot["system_unverified_trades_today"] == 2


def test_session_snapshot_falls_back_to_order_history():
    now = datetime(2026, 6, 21, 15, 0, tzinfo=timezone.utc)
    orders = [
        {"symbol": "AAPL", "status": "executed", "executed_at": (now - timedelta(minutes=30)).isoformat(), "metadata": {"realized_pnl": -7}},
        {"symbol": "AAPL", "status": "pending", "created_at": now.isoformat(), "metadata": {"realized_pnl": -99}},
    ]

    snapshot = build_session_risk_snapshot(FakeDB(trades=[], orders=orders), 1, symbol="AAPL", emergency_halt=True, now=now)

    assert snapshot["daily_realized_pnl"] == -7.0
    assert snapshot["weekly_realized_pnl"] == -7.0
    assert snapshot["trades_today"] == 1
    assert snapshot["symbol_trades_today"] == 1
    assert snapshot["emergency_halt"] is True
    assert snapshot["system_provenance_verified"] is False
    assert snapshot["system_provenance_source"] == "unavailable_without_fill_records"


def test_session_snapshot_does_not_invent_pnl_when_missing_fields():
    now = datetime(2026, 6, 21, 15, 0, tzinfo=timezone.utc)
    trades = [
        {"symbol": "AAPL", "status": "executed", "executed_at": (now - timedelta(minutes=5)).isoformat()},
    ]

    snapshot = build_session_risk_snapshot(FakeDB(trades=trades), 1, symbol="AAPL", now=now)

    assert snapshot["daily_realized_pnl"] == 0.0
    assert snapshot["weekly_realized_pnl"] == 0.0
    assert snapshot["consecutive_losses"] == 0
    assert snapshot["trades_today"] == 1
    assert snapshot["system_provenance_verified"] is False


def test_session_snapshot_verifies_complete_system_managed_fill_chain():
    now = datetime(2026, 8, 16, 15, 0, tzinfo=timezone.utc)
    fills = [
        {
            "fill_id": 101,
            "order_id": 11,
            "trade_id": "trade-11",
            "correlation_id": "corr-11",
            "symbol": "AAPL",
            "side": "sell",
            "quantity": 2,
            "fill_price": 125,
            "average_entry_price": 100,
            "realized_pnl": 50,
            "filled_at": (now - timedelta(minutes=30)).isoformat(),
        },
        {
            "fill_id": 102,
            "order_id": 12,
            "trade_id": "trade-12",
            "correlation_id": "corr-12",
            "symbol": "MSFT",
            "side": "sell",
            "quantity": 1,
            "fill_price": 110,
            "average_entry_price": 100,
            "realized_pnl": 10,
            "filled_at": (now - timedelta(minutes=10)).isoformat(),
        },
    ]

    snapshot = build_session_risk_snapshot(
        FakeDB(fills=fills, managed_fill_ids={101, 102}),
        1,
        now=now,
    )

    assert snapshot["daily_realized_pnl"] == 60.0
    assert snapshot["trades_today"] == 2
    assert snapshot["system_provenance_verified"] is True
    assert snapshot["system_managed_trades_today"] == 2
    assert snapshot["system_unverified_trades_today"] == 0
    assert snapshot["system_managed_realized_pnl"] == 60.0
    assert snapshot["system_provenance_source"] == (
        "database_fill_order_execution_risk_chain.v1"
    )


def test_one_unverified_fill_makes_daily_system_provenance_fail_closed():
    now = datetime(2026, 8, 16, 15, 0, tzinfo=timezone.utc)
    fills = [
        {
            "fill_id": 201,
            "order_id": 21,
            "trade_id": "trade-21",
            "correlation_id": "corr-21",
            "symbol": "AAPL",
            "realized_pnl": 40,
            "filled_at": (now - timedelta(minutes=30)).isoformat(),
        },
        {
            "fill_id": 202,
            "order_id": None,
            "trade_id": "broker:manual-1",
            "correlation_id": None,
            "symbol": "TSLA",
            "realized_pnl": 100,
            "filled_at": (now - timedelta(minutes=15)).isoformat(),
        },
    ]

    snapshot = build_session_risk_snapshot(
        FakeDB(fills=fills, managed_fill_ids={201}),
        1,
        now=now,
    )

    assert snapshot["daily_realized_pnl"] == 140.0
    assert snapshot["system_managed_realized_pnl"] == 40.0
    assert snapshot["system_managed_trades_today"] == 1
    assert snapshot["system_unverified_trades_today"] == 1
    assert snapshot["system_provenance_verified"] is False
