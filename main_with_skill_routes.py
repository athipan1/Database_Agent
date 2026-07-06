"""Runtime entrypoint with skill performance and backtest routes mounted.

This module keeps the existing ``main.py`` application intact while adding the
routes needed by Curator_Agent:

- POST /skills/execution-logs
- GET  /skills/performance/rank
- GET  /skills/{skill_id}/backtest-status

Docker uses this entrypoint so the Curator -> Database integration works at
runtime without changing the legacy monolithic main module in this PR.
"""

from __future__ import annotations

import logging

from main import app, db, get_api_key, get_correlation_id
from skill_performance_repository import setup_skill_performance_tables
from skill_performance_routes import create_skill_performance_routes
from backtest_repository import setup_backtest_tables
from backtest_routes import create_backtest_routes


app.include_router(create_skill_performance_routes(db, get_api_key, get_correlation_id))
app.include_router(create_backtest_routes(db, get_api_key, get_correlation_id))


@app.on_event("startup")
async def startup_skill_and_backtest_tables() -> None:
    """Ensure Curator-facing tables exist when the API starts."""
    try:
        setup_skill_performance_tables(db)
        setup_backtest_tables(db)
        logging.info("Skill performance and backtest tables verification/creation complete.")
    except Exception:
        logging.exception("Failed to verify/create skill performance and backtest tables.")
        raise
