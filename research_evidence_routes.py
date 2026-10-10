"""Research-only API independent of /backtests/runs and promotion routes."""
from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Security
from fastapi.security import APIKeyHeader

from research_evidence_repository import (
    CreateResearchEvidence, get_research_evidence, insert_research_evidence,
)


def create_research_evidence_routes(db, get_api_key_dependency, get_correlation_id_dependency):
    router = APIRouter(tags=["research-evidence"])
    api_key_header = APIKeyHeader(name="X-API-KEY", auto_error=False)

    async def _api_key(value: str = Security(api_key_header)):
        return get_api_key_dependency(value)

    async def _correlation_id():
        return await get_correlation_id_dependency()

    @router.post("/research/evidence", response_model=dict, status_code=201)
    def store_evidence(
        body: CreateResearchEvidence,
        _authorized: str = Depends(_api_key),
        correlation_id: str = Depends(_correlation_id),
    ):
        try:
            result, replay = insert_research_evidence(db, body)
        except ValueError as exc:
            msg = str(exc)
            if "collision" in msg or "stored content" in msg:
                raise HTTPException(status_code=409, detail="immutable evidence ID conflict") from exc
            raise HTTPException(status_code=422, detail=msg) from exc
        return {
            "status": "success", "schema_version": "phase17-research-evidence.v1",
            "correlation_id": correlation_id,
            "data": result,
            "metadata": {
                "idempotent_replay": replay, "safe_for_trading": False,
                "promotion_allowed": False, "execution_allowed": False,
            },
        }

    @router.get("/research/evidence/{evidence_id}", response_model=dict)
    def retrieve_evidence(
        evidence_id: str,
        _authorized: str = Depends(_api_key),
        correlation_id: str = Depends(_correlation_id),
    ):
        if len(evidence_id) != 64 or any(x not in "0123456789abcdef" for x in evidence_id):
            raise HTTPException(status_code=422, detail="invalid evidence identifier")
        result = get_research_evidence(db, evidence_id)
        if result is None:
            raise HTTPException(status_code=404, detail="research evidence not found")
        return {
            "status": "success", "schema_version": "phase17-research-evidence.v1",
            "correlation_id": correlation_id, "data": result,
            "metadata": {"safe_for_trading": False,
                         "promotion_allowed": False, "execution_allowed": False},
        }

    return router
