"""Optional historical market-data client lifecycle.

Database_Agent is the promotion and persistence authority. Its API, migrations,
and readiness must not depend on broker credentials. Alpaca credentials enable
an optional historical-data ingestion capability only; they never enable order
submission in this repository.
"""

from __future__ import annotations

import logging
from typing import Optional

from alpaca_client import AlpacaClient


logger = logging.getLogger(__name__)


def create_optional_market_data_client(
    api_key: Optional[str],
    secret_key: Optional[str],
) -> Optional[AlpacaClient]:
    """Create the historical-data client only when both credentials exist.

    Missing or partially configured credentials disable ingestion without
    preventing the Database_Agent API from booting. Secret values are never
    included in logs.
    """

    if not api_key and not secret_key:
        logger.info(
            "Optional Alpaca historical-data ingestion is disabled.",
            extra={"event": "market_data_client_disabled", "reason": "credentials_missing"},
        )
        return None
    if not api_key or not secret_key:
        logger.error(
            "Optional Alpaca historical-data ingestion is disabled because its credential pair is incomplete.",
            extra={"event": "market_data_client_disabled", "reason": "credentials_incomplete"},
        )
        return None
    return AlpacaClient(api_key=api_key, secret_key=secret_key)


def require_market_data_client(client: Optional[AlpacaClient]) -> AlpacaClient:
    """Fail closed when an explicit ingestion request lacks a data client."""

    if client is None:
        raise RuntimeError(
            "Historical market-data ingestion is unavailable because Alpaca data credentials are not configured."
        )
    return client
