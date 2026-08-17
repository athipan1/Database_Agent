"""Lifecycle-managed background scheduler for operational maintenance jobs."""

from __future__ import annotations

import logging
import threading
from typing import Callable, Optional

import schedule


class RuntimeScheduler:
    """Own a private schedule registry and a stoppable daemon thread."""

    def __init__(self) -> None:
        self._scheduler = schedule.Scheduler()
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None

    @property
    def is_running(self) -> bool:
        return bool(self._thread and self._thread.is_alive())

    def configure(
        self,
        *,
        ingestion_job: Callable[[], None],
        stats_job: Callable[[], None],
    ) -> None:
        self._scheduler.clear()
        self._scheduler.every().day.at("00:00").do(ingestion_job)
        self._scheduler.every(1).hours.do(stats_job)

    def start(self) -> None:
        if self.is_running:
            return
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._run,
            name="database-agent-scheduler",
            daemon=True,
        )
        self._thread.start()
        logging.info("Scheduler started in a background thread.")

    def stop(self, timeout: float = 2.0) -> None:
        self._stop_event.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=timeout)
        self._thread = None

    def _run(self) -> None:
        while not self._stop_event.is_set():
            self._scheduler.run_pending()
            self._stop_event.wait(1.0)
