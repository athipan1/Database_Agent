from types import SimpleNamespace

from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.application import create_application
from app.route_registry import route_signature


def _runtime():
    source = FastAPI(title="Legacy Database Agent", version="1.1.0")

    @source.get("/health")
    async def health():
        return {"source": "legacy-health"}

    @source.get("/history/signals")
    async def legacy_history():
        return {"source": "legacy-history"}

    state = {"startup": 0, "shutdown": 0}

    async def startup_event():
        state["startup"] += 1

    async def shutdown_event():
        state["shutdown"] += 1

    async def get_correlation_id():
        return "corr-factory"

    def get_api_key():
        return "test-key"

    runtime = SimpleNamespace(
        app=source,
        startup_event=startup_event,
        shutdown_event=shutdown_event,
        get_api_key=get_api_key,
        get_correlation_id=get_correlation_id,
    )
    return runtime, state


def test_application_factory_replaces_owned_routes_without_duplicates():
    runtime, _ = _runtime()
    app = create_application(runtime)

    signatures = [route_signature(route) for route in app.router.routes]
    assert len(signatures) == len(set(signatures))
    assert sum(
        1
        for route in app.router.routes
        if route.path == "/history/signals"
        and "GET" in (route.methods or set())
    ) == 1
    assert sum(
        1
        for route in app.router.routes
        if route.path == "/health"
        and "GET" in (route.methods or set())
    ) == 1


def test_application_factory_runs_runtime_lifespan_once():
    runtime, state = _runtime()
    app = create_application(runtime)

    with TestClient(app) as client:
        response = client.get("/health")
        assert response.status_code == 200
        assert state == {"startup": 1, "shutdown": 0}

    assert state == {"startup": 1, "shutdown": 1}
