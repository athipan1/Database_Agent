import os
import uuid

os.environ.setdefault("USE_SQLITE", "1")
os.environ.setdefault("DATABASE_DEV_MODE", "true")

from pydantic import ValidationError

from shadow_observation_models import CreateShadowObservationBody
from shadow_observation_repository import (
    create_shadow_observation,
    list_closed_shadow_outcomes,
    list_shadow_observations,
    setup_shadow_observation_table,
    shadow_trade_lifecycle,
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
    body = _body(account_id, event_key="signal")

    first = create_shadow_observation(db, body)
    replay = create_shadow_observation(db, body)

    assert first.event_id == replay.event_id
    assert first.event_key == replay.event_key == "signal"
    assert first.shadow_trade_id == replay.shadow_trade_id
    assert first.symbol == "AAPL"
    assert first.execution_mode == "shadow"
    assert first.broker_order_authorized is False
    assert len(list_shadow_observations(db, account_id=account_id)) == 1


def test_shadow_trade_requires_signal_entry_mark_exit_order_and_replay_is_safe():
    db = TradingDB()
    db.setup_database()
    setup_shadow_observation_table(db)
    account_id = _account_id()

    signal = create_shadow_observation(
        db,
        _body(account_id, event_key="signal"),
    )
    entry = create_shadow_observation(
        db,
        _body(
            account_id,
            event_type="entry_simulated",
            event_key="entry",
            shadow_trade_id=signal.shadow_trade_id,
            simulated_fill_price=100.02,
            simulated_slippage_bps=2.0,
        ),
    )
    mark = create_shadow_observation(
        db,
        _body(
            account_id,
            event_type="mark",
            event_key="cycle-2026-08-20T16",
            shadow_trade_id=signal.shadow_trade_id,
            simulated_fill_price=100.02,
            mfe_pct=0.035,
            mae_pct=-0.008,
            metadata={"lane": "research", "mark_price": 102.0},
        ),
    )
    mark_replay = create_shadow_observation(
        db,
        _body(
            account_id,
            event_type="mark",
            event_key="cycle-2026-08-20T16",
            shadow_trade_id=signal.shadow_trade_id,
            simulated_fill_price=100.02,
            mfe_pct=0.035,
            mae_pct=-0.008,
            metadata={"lane": "research", "mark_price": 102.0},
        ),
    )
    exit_event = create_shadow_observation(
        db,
        _body(
            account_id,
            event_type="exit_simulated",
            event_key="exit",
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
    exit_replay = create_shadow_observation(
        db,
        _body(
            account_id,
            event_type="exit_simulated",
            event_key="exit",
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
    assert [record.event_type for record in reversed(records)] == [
        "signal_decision",
        "entry_simulated",
        "mark",
        "exit_simulated",
    ]
    assert mark.event_id == mark_replay.event_id
    assert exit_event.event_id == exit_replay.event_id
    assert entry.broker_order_authorized is False
    assert exit_event.net_return_pct == 0.019

    lifecycle = shadow_trade_lifecycle(db, signal.shadow_trade_id)
    assert lifecycle["closed"] is True
    assert lifecycle["next_expected"] is None

    outcomes = list_closed_shadow_outcomes(db, account_id=account_id)
    assert len(outcomes) == 1
    assert outcomes[0]["shadow_trade_id"] == signal.shadow_trade_id
    assert outcomes[0]["net_return_pct"] == 0.019


def test_shadow_exit_without_mark_is_rejected():
    db = TradingDB()
    db.setup_database()
    setup_shadow_observation_table(db)
    account_id = _account_id()
    signal = create_shadow_observation(db, _body(account_id, event_key="signal"))
    create_shadow_observation(
        db,
        _body(
            account_id,
            event_type="entry_simulated",
            event_key="entry",
            shadow_trade_id=signal.shadow_trade_id,
            simulated_fill_price=100.02,
        ),
    )

    try:
        create_shadow_observation(
            db,
            _body(
                account_id,
                event_type="exit_simulated",
                event_key="exit",
                shadow_trade_id=signal.shadow_trade_id,
                exit_price=101.0,
            ),
        )
    except ValueError as exc:
        assert "at least one mark" in str(exc)
    else:
        raise AssertionError("shadow lifecycle allowed exit without mark")


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
