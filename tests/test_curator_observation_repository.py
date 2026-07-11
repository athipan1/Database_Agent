import os
import uuid

os.environ.setdefault("USE_SQLITE", "1")
os.environ.setdefault("DATABASE_DEV_MODE", "true")

from curator_observation_models import CreateCuratorObservationBody
from curator_observation_repository import (
    build_curator_observation_readiness,
    create_curator_observation,
    create_curator_observation_batch,
    get_curator_observation,
    list_curator_observations,
    setup_curator_observation_table,
)
from trading_db import TradingDB


def _account_id() -> str:
    return f"curator-test-{uuid.uuid4()}"


def _body(
    account_id: str,
    symbol: str,
    *,
    signal: str = "buy",
    agreement: float | None = 0.8,
    available: bool = True,
    contract_valid: bool = True,
    allowed: bool = True,
    rejection_codes=None,
    observation_id: str | None = None,
):
    return CreateCuratorObservationBody(
        observation_id=observation_id,
        account_id=account_id,
        correlation_id=f"corr-{account_id}",
        symbol=symbol,
        mode="shadow_ensemble",
        status="success" if available else "unavailable",
        available=available,
        signal=signal,
        agreement=agreement,
        contract_valid=contract_valid,
        would_pass_required_gate=allowed,
        selected_skill_count=3,
        execution_count=3,
        minimum_agreement=0.60,
        rejection_codes=rejection_codes or [],
        metadata={"schema": "curator_observation.v1"},
    )


def test_curator_observation_is_persisted_and_idempotent():
    db = TradingDB()
    db.setup_database()
    setup_curator_observation_table(db)
    account_id = _account_id()
    observation_id = str(uuid.uuid4())
    body = _body(
        account_id,
        "acgl",
        observation_id=observation_id,
    )

    first = create_curator_observation(db, body, correlation_id="request-corr")
    second = create_curator_observation(db, body, correlation_id="request-corr")

    assert first.observation_id == observation_id
    assert second.observation_id == observation_id
    assert first.symbol == "ACGL"
    assert first.metadata["schema"] == "curator_observation.v1"
    assert get_curator_observation(db, observation_id) == first

    records = list_curator_observations(db, account_id=account_id)
    assert len(records) == 1


def test_curator_observation_batch_is_scoped_and_filterable():
    db = TradingDB()
    db.setup_database()
    setup_curator_observation_table(db)
    account_id = _account_id()
    other_account = _account_id()

    create_curator_observation_batch(
        db,
        [
            _body(account_id, "ACGL"),
            _body(
                account_id,
                "ADBE",
                signal="hold",
                agreement=0.55,
                allowed=False,
                rejection_codes=["agreement_below_threshold"],
            ),
            _body(other_account, "CINF"),
        ],
        correlation_id="batch-corr",
    )

    records = list_curator_observations(
        db,
        account_id=account_id,
        mode="shadow_ensemble",
        limit=10,
    )
    assert {record.symbol for record in records} == {"ACGL", "ADBE"}
    assert all(str(record.account_id) == account_id for record in records)

    adbe = list_curator_observations(
        db,
        account_id=account_id,
        symbol="adbe",
    )
    assert len(adbe) == 1
    assert adbe[0].rejection_codes == ["agreement_below_threshold"]


def test_curator_readiness_reports_safety_blockers_and_gate_impact():
    db = TradingDB()
    db.setup_database()
    setup_curator_observation_table(db)
    account_id = _account_id()

    create_curator_observation_batch(
        db,
        [
            _body(account_id, "ACGL", signal="buy", agreement=0.80, allowed=True),
            _body(
                account_id,
                "ADBE",
                signal="hold",
                agreement=0.55,
                allowed=False,
                rejection_codes=["agreement_below_threshold"],
            ),
            _body(
                account_id,
                "CINF",
                signal="hold",
                agreement=None,
                contract_valid=False,
                allowed=False,
                rejection_codes=["broker_access_must_be_false"],
            ),
            _body(
                account_id,
                "BKNG",
                signal="hold",
                agreement=None,
                available=False,
                contract_valid=False,
                allowed=False,
                rejection_codes=["curator_shadow_ensemble_unavailable"],
            ),
        ],
        correlation_id="readiness-corr",
    )

    readiness = build_curator_observation_readiness(
        db,
        account_id=account_id,
        observation_target=50,
    )

    assert readiness.observations == 4
    assert readiness.available == 3
    assert readiness.unavailable == 1
    assert readiness.availability_rate == 0.75
    assert readiness.contract_valid == 2
    assert readiness.contract_invalid == 2
    assert readiness.contract_valid_rate == 0.5
    assert readiness.unsafe_contract_count == 1
    assert readiness.buy_count == 1
    assert readiness.hold_count == 3
    assert readiness.average_agreement == 0.675
    assert readiness.would_pass_required_gate == 1
    assert readiness.would_be_blocked == 3
    assert readiness.required_mode_eligible is False
    assert "observations_below_target" in readiness.blockers
    assert "availability_below_99_percent" in readiness.blockers
    assert "contract_valid_rate_below_100_percent" in readiness.blockers
    assert "unsafe_contracts_detected" in readiness.blockers
