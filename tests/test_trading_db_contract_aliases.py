from trading_db import TradingDB


def test_trading_db_exposes_canonical_order_and_trade_history_names():
    db = TradingDB.__new__(TradingDB)
    db.conn = None
    db.pool = None
    db.get_order_history = lambda account_id: [{"account_id": account_id, "kind": "order"}]
    db.get_executions = lambda account_id, limit=50: [
        {"account_id": account_id, "kind": "trade", "limit": limit}
    ]

    assert db.get_orders(7) == [{"account_id": 7, "kind": "order"}]
    assert db.get_trade_history(7, limit=12) == [
        {"account_id": 7, "kind": "trade", "limit": 12}
    ]
