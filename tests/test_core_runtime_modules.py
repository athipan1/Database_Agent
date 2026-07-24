from decimal import Decimal

import pytest
from fastapi import Depends, FastAPI
from fastapi.testclient import TestClient

from app.core.config import Settings, env_bool, load_settings
from app.core.middleware import (
    create_correlation_id_context,
    create_correlation_id_dependency,
    install_correlation_id_middleware,
)
from app.core.security import create_api_key_dependency


def test_load_settings_normalizes_and_validates_environment():
    settings = load_settings(
        {
            "TRADING_MODE": " paper ",
            "DATABASE_DEV_MODE": "false",
            "DATABASE_EMERGENCY_HALT": "yes",
            "DATABASE_AGENT_API_KEY": "secret",
            "DEFAULT_DEV_ACCOUNT_ID": "diagnostic",
            "DEFAULT_DEV_CASH_BALANCE": "123.45",
        }
    )

    assert settings.trading_mode == "PAPER"
    assert settings.database_dev_mode is False
    assert settings.database_emergency_halt is True
    assert settings.database_agent_api_key == "secret"
    assert settings.default_dev_account_id == "diagnostic"
    assert settings.default_dev_cash_balance == Decimal("123.45")


def test_live_mode_rejects_database_dev_mode():
    with pytest.raises(ValueError, match="forbidden"):
        load_settings(
            {
                "TRADING_MODE": "LIVE",
                "DATABASE_DEV_MODE": "true",
                "DATABASE_AGENT_API_KEY": "secret",
            }
        )


def test_env_bool_preserves_legacy_truthy_values():
    for value in ("1", "true", "TRUE", "yes", "y", "on"):
        assert env_bool("FLAG", environ={"FLAG": value}) is True
    assert env_bool("FLAG", environ={"FLAG": "false"}) is False


def test_api_key_dependency_reads_settings_at_request_time():
    current = {
        "settings": Settings(
            database_dev_mode=False,
            database_agent_api_key="first-key",
        )
    }
    get_api_key = create_api_key_dependency(lambda: current["settings"])
    app = FastAPI()

    @app.get("/protected")
    async def protected(api_key: str = Depends(get_api_key)):
        return {"api_key": api_key}

    client = TestClient(app)
    assert client.get(
        "/protected",
        headers={"X-API-KEY": "first-key"},
    ).status_code == 200

    current["settings"] = Settings(
        database_dev_mode=False,
        database_agent_api_key="second-key",
    )
    assert client.get(
        "/protected",
        headers={"X-API-KEY": "first-key"},
    ).status_code == 403
    assert client.get(
        "/protected",
        headers={"X-API-KEY": "second-key"},
    ).status_code == 200


def test_correlation_middleware_propagates_header_and_dependency():
    app = FastAPI()
    context = create_correlation_id_context()
    get_correlation_id = create_correlation_id_dependency(context)
    install_correlation_id_middleware(app, context)

    @app.get("/correlation")
    async def correlation(
        correlation_id: str = Depends(get_correlation_id),
    ):
        return {"correlation_id": correlation_id}

    response = TestClient(app).get(
        "/correlation",
        headers={"X-Correlation-ID": "corr-modular-1"},
    )

    assert response.status_code == 200
    assert response.headers["X-Correlation-ID"] == "corr-modular-1"
    assert response.json() == {"correlation_id": "corr-modular-1"}
