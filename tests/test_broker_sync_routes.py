from __future__ import annotations

from types import SimpleNamespace

from app.routers.broker_sync import ROUTE_SIGNATURES, create_broker_sync_router


def test_broker_sync_routes_are_declared_by_api_router():
    runtime = SimpleNamespace(
        db=object(),
        get_api_key=lambda: None,
        get_correlation_id=lambda: "test-correlation-id",
        wrap_response=lambda **kwargs: kwargs,
    )

    router = create_broker_sync_router(runtime)
    signatures = {
        (route.path, next(iter(route.methods)))
        for route in router.routes
    }

    assert signatures == set(ROUTE_SIGNATURES)
