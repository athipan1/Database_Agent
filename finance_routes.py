from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, Depends, Query, Security
from fastapi.security import APIKeyHeader

from finance_models import CreateFinanceEntryBody, UpsertFinanceBudgetsBody
from finance_repository import (
    create_finance_entry,
    delete_finance_entry,
    get_personal_finance_state,
    upsert_finance_budgets,
)

router = APIRouter(prefix="/personal-finance", tags=["Personal Finance"])


def wrap_response(data: Any = None):
    return {
        "status": "success",
        "agent_type": "database",
        "version": "1.1.0",
        "timestamp": datetime.now(timezone.utc),
        "data": data,
        "error": None,
        "confidence_score": None,
    }


def create_finance_routes(db, get_api_key_dependency, get_correlation_id_dependency):
    api_key_header = APIKeyHeader(name="X-API-KEY", auto_error=False)

    async def _api_key(api_key_header_value: str = Security(api_key_header)):
        return get_api_key_dependency(api_key_header_value)

    async def _correlation_id():
        return await get_correlation_id_dependency()

    @router.get("/state", response_model=dict)
    async def get_finance_state_endpoint(
        account_id: str,
        limit: int = Query(default=2000, ge=1, le=5000),
        api_key: str = Depends(_api_key),
        correlation_id: str = Depends(_correlation_id),
    ):
        return wrap_response(get_personal_finance_state(db, account_id, limit=limit))

    @router.post("/entries", response_model=dict)
    async def create_finance_entry_endpoint(
        body: CreateFinanceEntryBody,
        api_key: str = Depends(_api_key),
        correlation_id: str = Depends(_correlation_id),
    ):
        return wrap_response(create_finance_entry(db, body))

    @router.delete("/entries/{entry_id}", response_model=dict)
    async def delete_finance_entry_endpoint(
        entry_id: str,
        account_id: str,
        api_key: str = Depends(_api_key),
        correlation_id: str = Depends(_correlation_id),
    ):
        delete_finance_entry(db, entry_id, account_id)
        return wrap_response({"entry_id": entry_id, "deleted": True})

    @router.post("/budgets/{account_id}", response_model=dict)
    async def upsert_finance_budgets_endpoint(
        account_id: str,
        body: UpsertFinanceBudgetsBody,
        api_key: str = Depends(_api_key),
        correlation_id: str = Depends(_correlation_id),
    ):
        return wrap_response(upsert_finance_budgets(db, account_id, body))

    return router
