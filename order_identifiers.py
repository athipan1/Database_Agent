"""Stable identifiers used when persisting orders across database backends."""

from __future__ import annotations

from uuid import NAMESPACE_URL, UUID, uuid5


def client_order_id_for_trade_id(trade_id: int | str) -> str:
    """Return the UUID-compatible compatibility ID for an arbitrary trade ID.

    ``orders.trade_id`` is the canonical idempotency key and intentionally
    accepts integers and strings. PostgreSQL keeps the legacy
    ``client_order_id`` column as UUID, so non-UUID trade IDs need a stable
    projection instead of being inserted verbatim.
    """
    normalized = str(trade_id)
    try:
        return str(UUID(normalized))
    except ValueError:
        return str(uuid5(NAMESPACE_URL, f"database-agent:trade-id:{normalized}"))
