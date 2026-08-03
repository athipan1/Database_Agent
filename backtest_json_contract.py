from __future__ import annotations

import json
from typing import Any


def strict_backtest_json_dumps(value: Any) -> str:
    """Serialize JSON without changing empty lists into objects.

    The legacy repository used ``value or {}``, which silently persisted an
    empty list as ``{}``. Promotion evidence must preserve JSON shape and must
    reject non-finite values rather than stringify them.
    """

    normalized = {} if value is None else value
    return json.dumps(
        normalized,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def install_strict_backtest_json_contract() -> None:
    """Install the strict serializer at the legacy repository boundary.

    This compatibility hook is loaded by the promotion repository facade so
    existing write functions retain their public API while using safe JSON.
    """

    import backtest_repository

    backtest_repository._json_dumps = strict_backtest_json_dumps
