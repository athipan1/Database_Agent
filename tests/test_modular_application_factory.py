from app import runtime as base_runtime
from app.application import create_application
from app.route_registry import is_http_signature, route_signature
from fastapi.testclient import TestClient


class RuntimeStub:
    def __init__(self):
        self.state = {"startup": 0, "shutdown": 0}

    def __getattr__(self, name):
        return getattr(base_runtime, name)

    async def startup_event(self):
        self.state["startup"] += 1

    async def shutdown_event(self):
        self.state["shutdown"] += 1


def test_application_factory_registers_modular_routes_without_duplicates():
    runtime = RuntimeStub()
    app = create_application(runtime)

    signatures = [
        signature
        for route in app.router.routes
        if is_http_signature(signature := route_signature(route))
    ]
    assert len(signatures) == len(set(signatures))
    for path, method in (
        ("/health", "GET"),
        ("/metrics", "GET"),
        ("/history/signals", "GET"),
        ("/accounts/{account_id}/orders", "POST"),
    ):
        assert sum(
            1
            for route in app.router.routes
            if route.path == path and method in (route.methods or set())
        ) == 1


def test_application_factory_runs_runtime_lifespan_once():
    runtime = RuntimeStub()
    app = create_application(runtime)

    with TestClient(app) as client:
        response = client.get("/health")
        assert response.status_code == 200
        assert runtime.state == {"startup": 1, "shutdown": 0}

    assert runtime.state == {"startup": 1, "shutdown": 1}
