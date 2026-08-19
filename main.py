"""Database Agent modular runtime entrypoint.

The module keeps the historical patchable import surface used by tests and
runtime adapters, while all implementation now lives in focused ``app`` modules.
"""

from __future__ import annotations

import sys
import types

from dotenv import load_dotenv

from app.core.config import Settings


load_dotenv()
_runtime_settings = Settings.from_environ()
try:
    _runtime_settings.validate()
except ValueError as exc:
    raise SystemExit(str(exc)) from exc

from app import runtime as _runtime
from app.application import create_application
from position_bucket_repository import register_position_bucket_routes


_runtime.apply_settings(_runtime_settings)

for _name in dir(_runtime):
    if not _name.startswith("__"):
        globals()[_name] = getattr(_runtime, _name)

app = create_application(_runtime)
_runtime.app = app
register_position_bucket_routes(_runtime.db)


class _RuntimeFacade(types.ModuleType):
    """Forward patched public attributes to the active runtime module."""

    def __setattr__(self, name, value):
        super().__setattr__(name, value)
        runtime = self.__dict__.get("_runtime")
        if runtime is not None and name != "app" and hasattr(runtime, name):
            setattr(runtime, name, value)


sys.modules[__name__].__class__ = _RuntimeFacade
