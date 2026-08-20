import os
import uuid

os.environ.setdefault("USE_SQLITE", "1")
os.environ.setdefault("DATABASE_DEV_MODE", "true")

from pydantic import ValidationError

from shadow_observation_models import CreateShadowObservationBody
from shadow_observation_repository import (
    create_shadow_observation,
    list_shadow_observations,
    setup_shadow_observation_table,
)
from trading_db import TradingDB


def _account_id() -> str:
    return f"shadow-test-{uuid.uuid4()}"


def _body(account_id: str, event_type: str = "signal_decision", **overrides):
    payload = {
        "account_id": account_id,
        "correlation_id": f"corr-{account_id}",
        "signal_id": "signal-1",
        "strategy_id": "trend_following",
        "strategy_version": "v1",
        "symbol": "aapl",
        "side": "buy",
        "event_type": event_type,
        "decision_price": 100.0,
        "bid": 99.99,
        "ask": 100.01,
        "spread_bps": 2.0,
        "opportunity_score": 0.62,
        "scanner_score": 0.66,
        "execution_mode": "shadow",
        "broker_order_authorized": False,
        "metadata": {"lane": "research"},
    }
    payload.update(overrides)
    return CreateShadowObservationBody(**payload)


def test_shadow_observation_is_append_only_and_idempotent():
    db = TradingDB()
    db.setup_database()
    setup_shadow_observation_table(db)
    account_id = _account_id()
    event_id = str(uuid.uuid4())
    body = _body(account_id, event_id=event_id)

    first = create_shadow_observation(db, body)
    replay = create_shadow_observation(db, body)

    assert first.event_id == replay.event_id == event_id
    assert first.shadow_trade_id == replay.shadow_trade_id
    assert first.symbol == "AAPL"
    assert first.execution_mode == "shadow"
    assert first.broker_order_authorized is False
    assert len(list_shadow_observations(db, account_id=account_id)) == 1


def test_shadow_trade_accepts_multiple_immutable_lifecycle_events():
    db = TradingDB()
    db.setup_database()
    setup_shadow_observation_table(db)
    account_id = _account_id()

    signal = create_shadow_observation(db, _body(account_id, event_id="signal-event"))
    entry = create_shadow_observation(
        db,
        _body(
            account_id,
            event_type="entry_simulated",
            event_id="entry-event",
            shadow_trade_id=signal.shadow_trade_id,
            simulated_fill_price=100.02,
            simulated_slippage_bps=2.0,
        ),
    )
    exit_event = create_shadow_observation(
        db,
        _body(
            account_id,
            event_type="exit_simulated",
            event_id="exit-event",
            shadow_trade_id=signal.shadow_trade_id,
            simulated_fill_price=100.02,
            exit_price=102.0,
            mfe_pct=0.035,
            mae_pct=-0.008,
            gross_return_pct=0.0198,
            estimated_cost_pct=0.0008,
            net_return_pct=0.019,
            holding_period_seconds=3600,
        ),
    )

    records = list_shadow_observations(
        db,
        shadow_trade_id=signal.shadow_trade_id,
        limit=10,
    )
    assert {record.event_type for record in records} == {
        "signal_decision",
        "entry_simulated",
        "exit_simulated",
    }
    assert entry.broker_order_authorized is False
    assert exit_event.net_return_pct == 0.019


def test_shadow_contract_cannot_authorize_broker_or_change_execution_mode():
    account_id = _account_id()

    try:
        _body(account_id, broker_order_authorized=True)
    except ValidationError:
        pass
    else:
        raise AssertionError("shadow contract accepted broker_order_authorized=true")

    try:
        _body(account_id, execution_mode="paper")
    except ValidationError:
        pass
    else:
        raise AssertionError("shadow contract accepted non-shadow execution mode")
