import os

os.environ.setdefault("USE_SQLITE", "1")
os.environ.setdefault("DATABASE_DEV_MODE", "true")

from portfolio_audit_repository import create_portfolio_audit, get_portfolio_audit, list_portfolio_audits, setup_portfolio_audit_table
from trading_db import TradingDB


def test_portfolio_audit_persists_full_portfolio_trail():
    db = TradingDB()
    db.setup_database()
    setup_portfolio_audit_table(db)

    record = create_portfolio_audit(
        db,
        account_id=1,
        correlation_id="corr-portfolio-1",
        allocation_plan={
            "policy_name": "core_satellite_50_30_20",
            "buckets": {
                "core_dividend": {"target_weight": 0.5},
                "value_rebound": {"target_weight": 0.3},
                "news_momentum": {"target_weight": 0.2},
            },
        },
        portfolio_snapshot={"cash": 100000, "positions": []},
        selected_positions=[
            {"symbol": "KO", "strategy_bucket": "core_dividend", "target_weight": 0.5},
            {"symbol": "ACGL", "strategy_bucket": "value_rebound", "target_weight": 0.3},
        ],
        risk_approvals=[
            {"symbol": "KO", "approved": True, "risk_approval_id": "risk-ko", "final_quantity": 10},
            {"symbol": "ACGL", "approved": True, "risk_approval_id": "risk-acgl", "final_quantity": 5},
        ],
        execution_orders=[
            {"symbol": "KO", "order_id": 101, "status": "submitted"},
            {"symbol": "ACGL", "order_id": 102, "status": "submitted"},
        ],
        metadata={"flow": "discover_analyze_trade_portfolio"},
        status="executed",
    )

    assert record["portfolio_audit_id"]
    assert record["policy_name"] == "core_satellite_50_30_20"
    assert record["status"] == "executed"
    assert record["allocation_plan"]["buckets"]["core_dividend"]["target_weight"] == 0.5
    assert record["selected_positions"][0]["symbol"] == "KO"
    assert record["risk_approvals"][1]["risk_approval_id"] == "risk-acgl"
    assert record["execution_orders"][0]["order_id"] == 101
    assert record["metadata"]["flow"] == "discover_analyze_trade_portfolio"

    fetched = get_portfolio_audit(db, record["portfolio_audit_id"])
    assert fetched == record


def test_list_portfolio_audits_is_scoped_by_account_and_limited():
    db = TradingDB()
    db.setup_database()
    setup_portfolio_audit_table(db)

    first = create_portfolio_audit(db, account_id=1, allocation_plan={"policy_name": "first"})
    second = create_portfolio_audit(db, account_id=1, allocation_plan={"policy_name": "second"})
    create_portfolio_audit(db, account_id=2, allocation_plan={"policy_name": "other-account"})

    records = list_portfolio_audits(db, account_id=1, limit=10)

    ids = {row["portfolio_audit_id"] for row in records}
    assert first["portfolio_audit_id"] in ids
    assert second["portfolio_audit_id"] in ids
    assert len(records) == 2
    assert all(str(row["account_id"]) == "1" for row in records)

    limited = list_portfolio_audits(db, account_id=1, limit=1)
    assert len(limited) == 1
