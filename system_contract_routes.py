from datetime import datetime, timezone
from typing import Any, Optional

from fastapi import APIRouter, Depends


DATABASE_AGENT_TYPE = "database"
DATABASE_AGENT_VERSION = "1.1.0"
SCHEMA_VERSION = "1.0"


def wrap_contract_response(
    *,
    data: Any = None,
    status: str = "success",
    correlation_id: Optional[str] = None,
    metadata: Optional[dict] = None,
    error: Optional[dict] = None,
):
    return {
        "status": status,
        "agent_type": DATABASE_AGENT_TYPE,
        "version": DATABASE_AGENT_VERSION,
        "schema_version": SCHEMA_VERSION,
        "timestamp": datetime.now(timezone.utc),
        "correlation_id": correlation_id,
        "data": data,
        "metadata": metadata or {},
        "error": error,
        "confidence_score": None,
    }


def create_system_contract_routes(
    *,
    trading_mode: str,
    database_dev_mode: bool,
    database_emergency_halt: bool,
    database_agent_api_key_configured: bool,
    get_correlation_id_dependency,
):
    router = APIRouter()

    async def _correlation_id():
        return await get_correlation_id_dependency()

    @router.get("/version", response_model=dict)
    async def version_endpoint(correlation_id: str = Depends(_correlation_id)):
        return wrap_contract_response(
            correlation_id=correlation_id,
            data={
                "agent_type": DATABASE_AGENT_TYPE,
                "version": DATABASE_AGENT_VERSION,
                "schema_version": SCHEMA_VERSION,
                "api_contract": "multi-agent-trading-api-contract",
            },
            metadata={
                "required_operational_endpoints": ["/health", "/ready", "/version"],
            },
        )

    @router.get("/ready", response_model=dict)
    async def ready_endpoint(correlation_id: str = Depends(_correlation_id)):
        live_dev_mode_violation = trading_mode == "LIVE" and database_dev_mode
        ready = not live_dev_mode_violation and not database_emergency_halt

        return wrap_contract_response(
            status="success" if ready else "error",
            correlation_id=correlation_id,
            data={
                "ready": ready,
                "trading_mode": trading_mode,
                "dev_mode": database_dev_mode,
                "database_emergency_halt": database_emergency_halt,
                "database_agent_api_key_configured": database_agent_api_key_configured,
                "live_dev_mode_violation": live_dev_mode_violation,
            },
            metadata={
                "contract_source": "system_contract_routes",
            },
            error=None
            if ready
            else {
                "code": "DATABASE_AGENT_NOT_READY",
                "message": "Database Agent readiness check failed",
                "retryable": False,
            },
        )

    return router
