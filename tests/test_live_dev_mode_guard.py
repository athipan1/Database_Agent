import importlib
import sys
import types
from decimal import Decimal
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


class DummyInstrumentator:
    def instrument(self, app):
        return self

    def expose(self, app):
        return app


def install_stubs(monkeypatch):
    trading_db = types.ModuleType("trading_db")

    class TradingDB:
        def check_connection(self):
            return False

        def setup_database(self):
            return None

        def get_account_balance(self, account_id):
            return None

    trading_db.TradingDB = TradingDB
    monkeypatch.setitem(sys.modules, "trading_db", trading_db)

    alpaca_client = types.ModuleType("alpaca_client")

    class AlpacaClient:
        def __init__(self, *args, **kwargs):
            pass

    alpaca_client.AlpacaClient = AlpacaClient
    monkeypatch.setitem(sys.modules, "alpaca_client", alpaca_client)

    history_repository = types.ModuleType("history_repository")
    history_repository.create_performance_record = lambda *args, **kwargs: None
    history_repository.create_signal_record = lambda *args, **kwargs: None
    history_repository.get_performance_records = lambda *args, **kwargs: []
    history_repository.get_signal_records = lambda *args, **kwargs: []
    history_repository.setup_history_tables = lambda *args, **kwargs: None
    monkeypatch.setitem(sys.modules, "history_repository", history_repository)

    prometheus_module = types.ModuleType("prometheus_fastapi_instrumentator")
    prometheus_module.Instrumentator = DummyInstrumentator
    monkeypatch.setitem(sys.modules, "prometheus_fastapi_instrumentator", prometheus_module)


def reload_main(monkeypatch):
    sys.modules.pop("main", None)
    install_stubs(monkeypatch)
    return importlib.import_module("main")


def test_live_mode_forbids_database_dev_mode(monkeypatch):
    monkeypatch.setenv("TRADING_MODE", "LIVE")
    monkeypatch.setenv("DATABASE_DEV_MODE", "true")
    monkeypatch.setenv("DATABASE_AGENT_API_KEY", "test-key")

    with pytest.raises(SystemExit):
        reload_main(monkeypatch)


def test_paper_mode_allows_database_dev_mode(monkeypatch):
    monkeypatch.setenv("TRADING_MODE", "PAPER")
    monkeypatch.setenv("DATABASE_DEV_MODE", "true")
    monkeypatch.delenv("DATABASE_AGENT_API_KEY", raising=False)

    module = reload_main(monkeypatch)

    assert module.TRADING_MODE == "PAPER"
    assert module.DATABASE_DEV_MODE is True
    assert module.DEFAULT_DEV_CASH_BALANCE == Decimal("100000")


def test_invalid_trading_mode_exits(monkeypatch):
    monkeypatch.setenv("TRADING_MODE", "REAL")
    monkeypatch.setenv("DATABASE_DEV_MODE", "false")
    monkeypatch.setenv("DATABASE_AGENT_API_KEY", "test-key")

    with pytest.raises(SystemExit):
        reload_main(monkeypatch)
