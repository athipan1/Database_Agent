"""Bind Supabase replication components to the active database runtime."""

from __future__ import annotations

from typing import Any, Callable, Dict, Tuple

from app.core.config import Settings
from app.services.supabase_replication import SupabaseReplicationWorker
from supabase_replication_repository import enqueue_supabase_event


EventEnqueuer = Callable[[Dict[str, Any]], bool]


def build_supabase_replication_runtime(
    db,
    settings: Settings,
) -> Tuple[SupabaseReplicationWorker, EventEnqueuer]:
    """Create the worker and a database-bound durable enqueue function."""

    worker = SupabaseReplicationWorker(
        db=db,
        enabled=settings.supabase_replication_enabled,
        url=settings.supabase_url,
        secret_key=settings.supabase_secret_key,
        table=settings.supabase_table,
        interval_seconds=settings.supabase_replication_interval_seconds,
        batch_size=settings.supabase_replication_batch_size,
        max_attempts=settings.supabase_replication_max_attempts,
    )

    def enqueue(event: Dict[str, Any]) -> bool:
        return enqueue_supabase_event(db, event)

    return worker, enqueue
