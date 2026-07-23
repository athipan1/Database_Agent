from __future__ import annotations

import json
import sys
import traceback
from pathlib import Path

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
if str(REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(0, str(REPOSITORY_ROOT))


def _route_details(routes) -> list[dict]:
    return [
        {
            "path": str(getattr(route, "path", "")),
            "methods": sorted(getattr(route, "methods", None) or []),
            "name": getattr(route, "name", None),
        }
        for route in routes
    ]


def inspect_routes() -> dict:
    import main_with_skill_routes as runtime
    from backtest_routes import create_backtest_routes
    from skill_performance_routes import create_skill_performance_routes

    targets = [
        ("/skills/performance/rank", "GET"),
        ("/backtests/runs/latest", "GET"),
        ("/skills/{skill_id}/backtest-status", "GET"),
    ]
    candidate_skill = create_skill_performance_routes(
        runtime.db,
        runtime.get_api_key,
        runtime.get_correlation_id,
    )
    candidate_backtest = create_backtest_routes(
        runtime.db,
        runtime.get_api_key,
        runtime.get_correlation_id,
    )

    route_inventory = [
        detail
        for detail in _route_details(runtime.app.router.routes)
        if "skill" in detail["path"].lower()
        or "backtest" in detail["path"].lower()
    ]
    return {
        "counts": {
            path: sum(
                1
                for route in runtime.app.router.routes
                if getattr(route, "path", None) == path
                and method in (getattr(route, "methods", None) or set())
            )
            for path, method in targets
        },
        "routes": route_inventory,
        "candidate_skill_routes": _route_details(candidate_skill.routes),
        "candidate_backtest_routes": _route_details(candidate_backtest.routes),
        "module_file": str(Path(runtime.__file__).resolve()),
        "total_routes": len(runtime.app.router.routes),
    }


def main() -> None:
    try:
        result = inspect_routes()
    except BaseException as exc:
        result = {
            "error_type": type(exc).__name__,
            "error": str(exc),
            "traceback": traceback.format_exc(),
        }

    payload = json.dumps(result, indent=2, sort_keys=True)
    Path("route-counts.json").write_text(payload, encoding="utf-8")
    print(payload)


if __name__ == "__main__":
    main()
