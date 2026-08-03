from __future__ import annotations

from types import SimpleNamespace

import pytest

import app.services.market_data_client as market_data_client


def test_missing_credentials_disable_optional_client():
    assert market_data_client.create_optional_market_data_client(None, None) is None


@pytest.mark.parametrize(
    ("api_key", "secret_key"),
    [("key-only", None), (None, "secret-only"), ("", "secret-only")],
)
def test_incomplete_credential_pair_fails_closed(api_key, secret_key):
    assert (
        market_data_client.create_optional_market_data_client(api_key, secret_key)
        is None
    )


def test_complete_credentials_create_historical_data_client(monkeypatch):
    created = []

    def fake_client(*, api_key, secret_key):
        client = SimpleNamespace(api_key=api_key, secret_key=secret_key)
        created.append(client)
        return client

    monkeypatch.setattr(market_data_client, "AlpacaClient", fake_client)
    client = market_data_client.create_optional_market_data_client(
        "data-key",
        "data-secret",
    )

    assert client is created[0]
    assert client.api_key == "data-key"
    assert client.secret_key == "data-secret"


def test_explicit_ingestion_requires_configured_client():
    with pytest.raises(RuntimeError, match="ingestion is unavailable"):
        market_data_client.require_market_data_client(None)
