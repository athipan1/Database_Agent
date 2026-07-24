"""Compatibility facade for the modular Database Agent runtime.

The previous monolithic implementation is retained in ``legacy_main.py`` while
routes are migrated behind ``app.application.create_application``. Public module
symbols remain patchable because existing tests and runtime adapters import and
modify attributes on ``main`` directly.
"""

from __future__ import annotations

import sys
import types

import legacy_main as _legacy
from app.application import create_application


# Preserve the historical import surface (`main.db`, `main.create_fill_record`,
# constants, helpers, and models) while the implementation is migrated.
for _name in dir(_legacy):
    if not _name.startswith("__"):
        globals()[_name] = getattr(_legacy, _name)


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
