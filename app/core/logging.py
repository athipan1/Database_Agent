"""Structured logging configuration for the Database Agent runtime."""

from __future__ import annotations

import logging
import sys

from pythonjsonlogger import jsonlogger


_HANDLER_MARKER = "_database_agent_json_handler"
_FILTER_MARKER = "_database_agent_correlation_filter"


class CorrelationIdFilter(logging.Filter):
    """Attach the active correlation ID to every log record."""

    def __init__(self, correlation_id_context) -> None:
        super().__init__()
        self._correlation_id_context = correlation_id_context

    def filter(self, record: logging.LogRecord) -> bool:
        record.correlation_id = self._correlation_id_context.get()
        return True


def configure_logging(correlation_id_context, *, level: int = logging.INFO) -> None:
    """Configure JSON logging once without duplicating handlers on reload."""

    root = logging.getLogger()
    if not any(getattr(handler, _HANDLER_MARKER, False) for handler in root.handlers):
        handler = logging.StreamHandler(sys.stdout)
        setattr(handler, _HANDLER_MARKER, True)
        handler.setFormatter(
            jsonlogger.JsonFormatter(
                "%(asctime)s %(levelname)s %(name)s %(message)s %(correlation_id)s",
                timestamp=True,
            )
        )
        root.addHandler(handler)

    if not any(getattr(item, _FILTER_MARKER, False) for item in root.filters):
        correlation_filter = CorrelationIdFilter(correlation_id_context)
        setattr(correlation_filter, _FILTER_MARKER, True)
        root.addFilter(correlation_filter)

    root.setLevel(level)
