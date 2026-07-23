from __future__ import annotations

import json
import sys
import traceback
from pathlib import Path

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
if str(REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(0, str(REPOSITORY_ROOT))


def inspect_routes() -> dict:
    import main_with_skill_routes as runtime

    targets = [
        ("/skills/performance/rank", "GET"),
        ("/backtests/runs/latest", "GET"),
        ("/skills/{skill_id}/backtest-status", "GET"),
    ]
    route_inventory = []
    for route in runtime.app.routes:
        path = str(getattr(route, "path", ""))
        if "skill" in path.lower() or "backtest" in path.lower():
            route_inventory.append(
                {
                    "path": path,
                    "methods": sorted(getattr(route, "methods", None) or []),
                    "name": getattr(route, "name", None),
                }
            )

    return {
        "counts": {
            path: sum(
                1
                for route in runtime.app.routes
                if getattr(route, "path", None) == path
                and method in (getattr(route, "methods", None) or set())
            )
            for path, method in targets
        },
        "routes": route_inventory,
        "module_file": str(Path(runtime.__file__).resolve()),
        "total_routes": len(runtime.app.routes),
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
