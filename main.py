"""Compatibility facade for the modular Database Agent runtime.

The previous monolithic implementation is retained in ``legacy_main.py`` while
routes are migrated behind ``app.application.create_application``. Public module
symbols remain patchable because existing tests and runtime adapters import and
modify attributes on ``main`` directly.
"""

from __future__ import annotations

import sys
import types

from dotenv import load_dotenv

from app.core.config import Settings


# Preserve the historical import-time fail-closed guards without reloading the
# shared legacy module. Reloading that module would mutate the database object
# held by already-created FastAPI route closures and contaminate later requests.
load_dotenv()
_runtime_settings = Settings.from_environ()
try:
    _runtime_settings.validate()
except ValueError as exc:
    raise SystemExit(str(exc)) from exc

import legacy_main as _legacy
from app.application import create_application


# Preserve the historical import surface (`main.db`, `main.create_fill_record`,
# constants, helpers, and models) while the implementation is migrated.
for _name in dir(_legacy):
    if not _name.startswith("__"):
        globals()[_name] = getattr(_legacy, _name)

# A fresh import of the facade must reflect the current environment even when
# the shared legacy implementation is intentionally kept alive.
TRADING_MODE = _runtime_settings.trading_mode
DATABASE_DEV_MODE = _runtime_settings.database_dev_mode
DATABASE_EMERGENCY_HALT = _runtime_settings.database_emergency_halt
DATABASE_AGENT_API_KEY = _runtime_settings.database_agent_api_key
DEFAULT_DEV_ACCOUNT_ID = _runtime_settings.default_dev_account_id
DEFAULT_DEV_CASH_BALANCE = _runtime_settings.default_dev_cash_balance


app = create_application(_legacy)
_legacy.app = app


class _RuntimeFacade(types.ModuleType):
    """Forward patched legacy attributes to the active runtime module."""

    def __setattr__(self, name, value):
        super().__setattr__(name, value)
        legacy = self.__dict__.get("_legacy")
        if legacy is not None and name != "app" and hasattr(legacy, name):
            setattr(legacy, name, value)


sys.modules[__name__].__class__ = _RuntimeFacade
