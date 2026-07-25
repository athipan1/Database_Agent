"""Fail-open asynchronous delivery of durable outbox events to Supabase."""

from __future__ import annotations

import json
import logging
import threading
from typing import Any, Dict, Optional
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen

from supabase_replication_repository import (
    claim_supabase_events,
    get_supabase_outbox_stats,
    mark_supabase_event_failed,
    mark_supabase_event_sent,
)


class SupabaseDeliveryError(RuntimeError):
    """Raised when Supabase rejects or cannot receive one replication event."""


class SupabaseEventClient:
    """Minimal REST client supporting modern secret keys and legacy service-role JWTs."""

    def __init__(
        self,
        *,
        url: str,
        secret_key: str,
        table: str = "database_agent_events",
        timeout_seconds: float = 15.0,
    ) -> None:
        self._url = url.rstrip("/")
        self._secret_key = secret_key.strip()
        self._table = table.strip() or "database_agent_events"
        self._timeout_seconds = max(1.0, float(timeout_seconds))

    @property
    def configured(self) -> bool:
        return bool(self._url and self._secret_key)

    def upsert(self, event: Dict[str, Any]) -> None:
        """Idempotently upsert one event by `event_id`."""

        if not self.configured:
            raise SupabaseDeliveryError("Supabase client is not configured")

        endpoint = (
            f"{self._url}/rest/v1/{quote(self._table, safe='')}"
            "?on_conflict=event_id"
        )
        headers = {
            "apikey": self._secret_key,
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Prefer": "resolution=merge-duplicates,return=minimal",
            "User-Agent": "database-agent-supabase-replication/1.0",
        }
        # Legacy service_role keys are JWTs and may also be used as bearer tokens.
        # Modern sb_secret_ keys belong only in the apikey header.
        if self._secret_key.startswith("eyJ"):
            headers["Authorization"] = f"Bearer {self._secret_key}"

        request = Request(
            endpoint,
            data=json.dumps(event, default=str).encode("utf-8"),
            headers=headers,
            method="POST",
        )
        try:
            with urlopen(request, timeout=self._timeout_seconds) as response:
                status = int(response.status)
                if status not in {200, 201, 204}:
                    raise SupabaseDeliveryError(
                        f"Supabase returned unexpected HTTP status {status}"
                    )
        except HTTPError as exc:
            raise SupabaseDeliveryError(
                f"Supabase rejected event with HTTP {exc.code}"
            ) from exc
        except URLError as exc:
            raise SupabaseDeliveryError("Supabase connection failed") from exc


class SupabaseReplicationWorker:
    """Drain a Railway/PostgreSQL outbox without blocking trading requests."""

    def __init__(
        self,
        *,
        db,
        enabled: bool,
        url: Optional[str],
        secret_key: Optional[str],
        table: str = "database_agent_events",
        interval_seconds: float = 10.0,
        batch_size: int = 50,
        max_attempts: int = 10,
    ) -> None:
        self._db = db
        self._enabled = bool(enabled)
        self._interval_seconds = max(1.0, float(interval_seconds))
        self._batch_size = max(1, min(int(batch_size), 500))
        self._max_attempts = max(1, int(max_attempts))
        self._client = SupabaseEventClient(
            url=url or "",
            secret_key=secret_key or "",
            table=table,
        )
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._state_lock = threading.Lock()
        self._last_success_at: Optional[str] = None
        self._last_error: Optional[str] = None
        self._delivered_total = 0
        self._failed_total = 0

    @property
    def configured(self) -> bool:
        return self._enabled and self._client.configured

    @property
    def is_running(self) -> bool:
        return bool(self._thread and self._thread.is_alive())

    def start(self) -> bool:
        if not self.configured:
            logging.info(
                "Supabase replication is disabled or missing server credentials."
            )
            return False
        if self.is_running:
            return True
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._run,
            name="database-agent-supabase-replication",
            daemon=True,
        )
        self._thread.start()
        logging.info("Supabase replication worker started.")
        return True

    def stop(self, timeout: float = 3.0) -> None:
        self._stop_event.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=timeout)
        self._thread = None

    def flush_once(self) -> Dict[str, int]:
        """Deliver one batch. Individual delivery failures remain fail-open."""

        delivered = 0
        failed = 0
        events = claim_supabase_events(self._db, limit=self._batch_size)
        for item in events:
            event_id = str(item["event_id"])
            try:
                self._client.upsert(item["event"])
                mark_supabase_event_sent(self._db, event_id)
                delivered += 1
                with self._state_lock:
                    from datetime import datetime, timezone

                    self._last_success_at = datetime.now(timezone.utc).isoformat()
                    self._last_error = None
                    self._delivered_total += 1
            except Exception as exc:
                failed += 1
                status = mark_supabase_event_failed(
                    self._db,
                    event_id,
                    attempts=int(item.get("attempts") or 0),
                    error=type(exc).__name__,
                    max_attempts=self._max_attempts,
                )
                logging.warning(
                    "Supabase event delivery failed; event retained for %s.",
                    status,
                )
                with self._state_lock:
                    self._last_error = type(exc).__name__
                    self._failed_total += 1
        return {"claimed": len(events), "delivered": delivered, "failed": failed}

    def health(self) -> Dict[str, Any]:
        try:
            outbox = get_supabase_outbox_stats(self._db)
        except Exception:
            outbox = {"status": "unavailable"}
        with self._state_lock:
            return {
                "enabled": self._enabled,
                "configured": self.configured,
                "running": self.is_running,
                "last_success_at": self._last_success_at,
                "last_error": self._last_error,
                "delivered_total": self._delivered_total,
                "failed_total": self._failed_total,
                "outbox": outbox,
            }

    def _run(self) -> None:
        while not self._stop_event.is_set():
            try:
                self.flush_once()
            except Exception as exc:
                logging.exception(
                    "Supabase replication loop failed without stopping Database_Agent: %s",
                    exc,
                )
                with self._state_lock:
                    self._last_error = type(exc).__name__
            self._stop_event.wait(self._interval_seconds)
