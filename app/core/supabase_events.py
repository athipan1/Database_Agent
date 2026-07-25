"""ASGI middleware that records successful writes into the durable outbox."""

from __future__ import annotations

import hashlib
import json
import logging
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Optional

from fastapi import FastAPI


_WRITE_METHODS = frozenset({"POST", "PUT", "PATCH", "DELETE"})
_MAX_CAPTURE_BYTES = 1_000_000


def _first_value(payload: Any, keys: tuple[str, ...]) -> Optional[str]:
    if isinstance(payload, dict):
        for key in keys:
            value = payload.get(key)
            if value is not None:
                return str(value)
        for value in payload.values():
            found = _first_value(value, keys)
            if found is not None:
                return found
    elif isinstance(payload, list):
        for value in payload:
            found = _first_value(value, keys)
            if found is not None:
                return found
    return None


def _event_metadata(method: str, path: str, payload: Any) -> Dict[str, Optional[str]]:
    normalized = path.lower()
    if "risk-approvals" in normalized:
        entity_type = "risk_approval"
        entity_id = _first_value(payload, ("approval_id",))
    elif "execution-jobs" in normalized or "execution-job" in normalized:
        entity_type = "execution_job"
        entity_id = _first_value(payload, ("job_id", "execution_job_id"))
    elif "/fills" in normalized:
        entity_type = "fill"
        entity_id = _first_value(payload, ("fill_id", "broker_fill_id"))
    elif "/orders" in normalized:
        entity_type = "order"
        entity_id = _first_value(payload, ("order_id", "trade_id"))
    elif "broker-sync" in normalized:
        entity_type = "broker_sync"
        entity_id = _first_value(payload, ("sync_id", "account_id"))
    elif "history" in normalized:
        entity_type = "history"
        entity_id = _first_value(payload, ("record_id", "signal_id"))
    elif "plan" in normalized:
        entity_type = "plan"
        entity_id = _first_value(payload, ("plan_id", "record_id"))
    elif "policy" in normalized:
        entity_type = "policy_review"
        entity_id = _first_value(payload, ("review_id", "record_id"))
    elif "profit" in normalized:
        entity_type = "profit_lifecycle"
        entity_id = _first_value(payload, ("position_id", "decision_id"))
    else:
        entity_type = "database_write"
        entity_id = _first_value(payload, ("id",))

    if method == "POST" and normalized.endswith("/claim-next"):
        operation = "claim"
    elif "execute" in normalized:
        operation = "execute"
    elif "sync" in normalized:
        operation = "sync"
    elif method == "POST":
        operation = "create"
    elif method in {"PATCH", "PUT"}:
        operation = "update"
    else:
        operation = "delete"

    return {
        "entity_type": entity_type,
        "entity_id": entity_id,
        "operation": operation,
        "event_type": f"{entity_type}.{operation}",
        "account_id": _first_value(payload, ("account_id",)),
    }


def build_supabase_event(
    *,
    method: str,
    route: str,
    correlation_id: str,
    payload: Any,
) -> Dict[str, Any]:
    """Build an idempotent, versioned mirror event."""

    occurred_at = datetime.now(timezone.utc).isoformat()
    encoded_payload = json.dumps(
        payload,
        default=str,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    payload_hash = hashlib.sha256(encoded_payload).hexdigest()
    event_key = "|".join(
        (
            "database-agent-event.v1",
            correlation_id,
            method.upper(),
            route,
            payload_hash,
        )
    )
    metadata = _event_metadata(method.upper(), route, payload)
    return {
        "event_id": hashlib.sha256(event_key.encode("utf-8")).hexdigest(),
        "source": "database_agent",
        "event_type": metadata["event_type"],
        "entity_type": metadata["entity_type"],
        "entity_id": metadata["entity_id"],
        "operation": metadata["operation"],
        "route": route,
        "http_method": method.upper(),
        "correlation_id": correlation_id,
        "account_id": metadata["account_id"],
        "schema_version": "database-agent-event.v1",
        "payload": payload,
        "payload_sha256": payload_hash,
        "occurred_at": occurred_at,
    }


class SupabaseEventCaptureMiddleware:
    """Capture response envelopes after successful writes and enqueue them locally."""

    def __init__(
        self,
        app,
        *,
        enabled_provider: Callable[[], bool],
        enqueue_event: Callable[[Dict[str, Any]], bool],
    ) -> None:
        self.app = app
        self._enabled_provider = enabled_provider
        self._enqueue_event = enqueue_event

    async def __call__(self, scope, receive, send) -> None:
        if scope.get("type") != "http":
            await self.app(scope, receive, send)
            return

        method = str(scope.get("method") or "GET").upper()
        if method not in _WRITE_METHODS or not self._enabled_provider():
            await self.app(scope, receive, send)
            return

        status_code = 500
        response_headers: Dict[str, str] = {}
        chunks: list[bytes] = []
        captured_bytes = 0

        async def send_wrapper(message) -> None:
            nonlocal status_code, captured_bytes
            if message.get("type") == "http.response.start":
                status_code = int(message.get("status") or 500)
                response_headers.update(
                    {
                        key.decode("latin-1").lower(): value.decode("latin-1")
                        for key, value in message.get("headers", [])
                    }
                )
            elif message.get("type") == "http.response.body":
                body = message.get("body") or b""
                if captured_bytes + len(body) <= _MAX_CAPTURE_BYTES:
                    chunks.append(body)
                    captured_bytes += len(body)
            await send(message)

        await self.app(scope, receive, send_wrapper)

        if not 200 <= status_code < 300 or not chunks:
            return
        try:
            envelope = json.loads(b"".join(chunks).decode("utf-8"))
            payload = envelope.get("data") if isinstance(envelope, dict) else envelope
            route_object = scope.get("route")
            route = str(
                getattr(route_object, "path", None)
                or scope.get("path")
                or "/unknown"
            )
            correlation_id = (
                response_headers.get("x-correlation-id")
                or dict(scope.get("headers") or []).get(b"x-correlation-id", b"").decode(
                    "latin-1"
                )
                or "missing-correlation-id"
            )
            event = build_supabase_event(
                method=method,
                route=route,
                correlation_id=correlation_id,
                payload=payload,
            )
            self._enqueue_event(event)
        except Exception as exc:
            logging.warning(
                "Could not enqueue Supabase mirror event; response remains successful: %s",
                type(exc).__name__,
            )


def install_supabase_event_capture(app: FastAPI, runtime: Any) -> None:
    app.add_middleware(
        SupabaseEventCaptureMiddleware,
        enabled_provider=lambda: bool(runtime.SUPABASE_REPLICATION_ENABLED),
        enqueue_event=runtime.enqueue_supabase_event,
    )
