from datetime import datetime, timezone, timedelta
from decimal import Decimal
from unittest.mock import patch

from session_risk_repository import build_session_risk_snapshot


class FakeDB:
    pass


def test_session_snapshot_prefers_fill_realized_pnl():
    now = datetime(2026, 6, 21, 15, 0, tzinfo=timezone.utc)
    fills = [
        {"symbol": "AAPL", "side": "sell", "quantity": 1, "fill_price": Decimal("90"), "average_entry_price": Decimal("100"), "realized_pnl": Decimal("-10"), "filled_at": (now - timedelta(minutes=10)).isoformat()},
        {"symbol": "MSFT", "side": "sell", "quantity": 1, "fill_price": Decimal("120"), "average_entry_price": Decimal("100"), "realized_pnl": Decimal("20"), "filled_at": (now - timedelta(minutes=20)).isoformat()},
    ]

    with patch("session_risk_repository._safe_rows_from") as rows_from:
        rows_from.side_effect = lambda db, method, *args, **kwargs: fills if method == "get_fills" else []
        snapshot = build_session_risk_snapshot(FakeDB(), 1, symbol="AAPL", now=now)

    assert snapshot["daily_realized_pnl"] == 10.0
    assert snapshot["weekly_realized_pnl"] == 10.0
    assert snapshot["symbol_trades_today"] == 1
    assert snapshot["consecutive_losses"] == 0
    assert snapshot["source"] == "database_agent_fills"


def test_session_snapshot_counts_fill_loss_streak():
    now = datetime(2026, 6, 21, 15, 0, tzinfo=timezone.utc)
    fills = [
        {"symbol": "AAPL", "side": "sell", "quantity": 1, "realized_pnl": Decimal("-5"), "filled_at": (now - timedelta(minutes=5)).isoformat()},
        {"symbol": "AAPL", "side": "sell", "quantity": 1, "realized_pnl": Decimal("-7"), "filled_at": (now - timedelta(minutes=30)).isoformat()},
        {"symbol": "AAPL", "side": "sell", "quantity": 1, "realized_pnl": Decimal("8"), "filled_at": (now - timedelta(hours=2)).isoformat()},
    ]

    with patch("session_risk_repository._safe_rows_from") as rows_from:
        rows_from.side_effect = lambda db, method, *args, **kwargs: fills if method == "get_fills" else []
        snapshot = build_session_risk_snapshot(FakeDB(), 1, symbol="AAPL", now=now)

    assert snapshot["daily_realized_pnl"] == -4.0
    assert snapshot["consecutive_losses"] == 2
    assert snapshot["minutes_since_last_loss"] == 5
