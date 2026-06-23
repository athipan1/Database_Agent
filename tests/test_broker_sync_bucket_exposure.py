from tests.test_broker_sync_status_repository import SQLiteStatusTestDB
from broker_sync_repository import sync_broker_state
from broker_sync_status_repository import broker_sync_status


def bucket_state():
    return {
        "source": "execution_agent",
        "account_id": 1,
        "broker": "ALPACA",
        "paper": True,
        "captured_at": "2026-06-23T15:30:00Z",
        "account": {"cash": "50000", "buying_power": "200000", "equity": "100000", "portfolio_value": "100000"},
        "positions": [
            {
                "symbol": "KO",
                "qty": "100",
                "avg_entry_price": "60",
                "current_price": "60",
                "market_value": "6000",
                "strategy_bucket": "core_dividend",
            },
            {
                "symbol": "ACGL",
                "qty": "50",
                "avg_entry_price": "100",
                "current_price": "100",
                "market_value": "5000",
                "strategy_bucket": "value_rebound",
            },
            {
                "symbol": "NEWS",
                "qty": "10",
                "avg_entry_price": "100",
                "current_price": "100",
                "market_value": "1000",
                "metadata": {"strategy_bucket": "news_momentum"},
            },
        ],
        "open_orders": [
            {
                "id": "order-news-1",
                "symbol": "NEWS",
                "side": "buy",
                "qty": "5",
                "type": "market",
                "status": "new",
                "strategy_bucket": "news_momentum",
            }
        ],
        "summary": {"position_count": 3, "open_order_count": 1},
    }


def test_broker_sync_persists_strategy_bucket_on_positions_and_orders():
    db = SQLiteStatusTestDB()
    sync_broker_state(db, bucket_state())

    status = broker_sync_status(db, account_id=1)

    positions = {row["symbol"]: row for row in status["database"]["positions"]}
    assert positions["KO"]["strategy_bucket"] == "core_dividend"
    assert positions["ACGL"]["strategy_bucket"] == "value_rebound"
    assert positions["NEWS"]["strategy_bucket"] == "news_momentum"

    orders = status["database"]["open_orders"]
    assert orders[0]["strategy_bucket"] == "news_momentum"


def test_broker_sync_status_reports_bucket_exposure():
    db = SQLiteStatusTestDB()
    sync_broker_state(db, bucket_state())

    status = broker_sync_status(db, account_id=1)
    buckets = status["bucket_exposure"]["buckets"]

    assert status["bucket_exposure"]["equity_basis"] == "100000"
    assert buckets["core_dividend"]["target_weight"] == "0.50"
    assert buckets["core_dividend"]["target_value"] == "50000.00"
    assert buckets["core_dividend"]["exposure"] == "6000.00"
    assert buckets["value_rebound"]["target_value"] == "30000.00"
    assert buckets["value_rebound"]["exposure"] == "5000.00"
    assert buckets["news_momentum"]["target_value"] == "20000.00"
    assert buckets["news_momentum"]["exposure"] == "1000.00"
