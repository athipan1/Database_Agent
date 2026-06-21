from datetime import datetime, timezone, timedelta

from session_risk_repository import build_session_risk_snapshot


class FakeDB:
    def __init__(self, trades=None, orders=None):
        self.trades = trades or []
        self.orders = orders or []

    def get_trade_history(self, account_id):
        return self.trades

    def get_orders(self, account_id):
        return self.orders


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
