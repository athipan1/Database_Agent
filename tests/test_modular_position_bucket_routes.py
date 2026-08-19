from main import app


def _route_signatures():
    return {
        (str(getattr(route, "path", "")), frozenset(getattr(route, "methods", None) or set()))
        for route in app.router.routes
    }


def test_modular_runtime_exposes_strategy_bucket_assignment_routes():
    routes = _route_signatures()

    assert (
        "/accounts/{account_id}/strategy-bucket-assignments",
        frozenset({"GET"}),
    ) in routes
    assert (
        "/accounts/{account_id}/position-buckets/bulk",
        frozenset({"POST"}),
    ) in routes
    assert (
        "/accounts/{account_id}/position-buckets/{symbol}",
        frozenset({"PATCH"}),
    ) in routes
