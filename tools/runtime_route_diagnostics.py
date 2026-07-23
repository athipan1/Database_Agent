from __future__ import annotations

import json
import sys
from pathlib import Path

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
if str(REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(0, str(REPOSITORY_ROOT))

import main_with_skill_routes as runtime


def main() -> None:
    targets = [
        ("/skills/performance/rank", "GET"),
        ("/backtests/runs/latest", "GET"),
        ("/skills/{skill_id}/backtest-status", "GET"),
    ]
    counts = {
        path: sum(
            1
            for route in runtime.app.routes
            if getattr(route, "path", None) == path
            and method in (getattr(route, "methods", None) or set())
        )
        for path, method in targets
    }
    Path("route-counts.json").write_text(
        json.dumps(counts, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    print(json.dumps(counts, sort_keys=True))


if __name__ == "__main__":
    main()
