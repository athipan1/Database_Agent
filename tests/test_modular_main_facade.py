import os
import subprocess
import sys
import textwrap


def _run_isolated(script: str) -> None:
    env = os.environ.copy()
    env.update(
        {
            "DATABASE_AGENT_API_KEY": "test-key",
            "DATABASE_DEV_MODE": "true",
            "TRADING_MODE": "PAPER",
            "ALPACA_API_KEY": "test-alpaca-key",
            "ALPACA_SECRET_KEY": "test-alpaca-secret",
        }
    )
    result = subprocess.run(
        [sys.executable, "-c", textwrap.dedent(script)],
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, (
        f"isolated modular runtime check failed\nstdout:\n{result.stdout}\n"
        f"stderr:\n{result.stderr}"
    )


def test_main_is_small_facade_and_legacy_module_is_removed():
    _run_isolated(
        """
        from pathlib import Path
        from unittest.mock import patch
        import main

        assert Path(main.__file__).name == "main.py"
        assert not Path("legacy_main.py").exists()
        assert main.app is main._runtime.app
        assert main.db is main._runtime.db
        assert len(Path(main.__file__).read_text().splitlines()) < 60
        assert len(Path(main._runtime.__file__).read_text().splitlines()) < 250

        original = main._runtime.DATABASE_AGENT_API_KEY
        with patch.object(main, "DATABASE_AGENT_API_KEY", "patched-key"):
            assert main.DATABASE_AGENT_API_KEY == "patched-key"
            assert main._runtime.DATABASE_AGENT_API_KEY == "patched-key"
        assert main._runtime.DATABASE_AGENT_API_KEY == original
        """
    )


def test_modular_runtime_registers_all_core_routes_once():
    _run_isolated(
        """
        import main

        targets = [
            ("/health", "GET"),
            ("/metrics", "GET"),
            ("/history/signals", "GET"),
            ("/history/performance", "POST"),
            ("/accounts/{account_id}/fills", "POST"),
            ("/execution-jobs/{job_id}", "PATCH"),
            ("/accounts/{account_id}/orders", "POST"),
            ("/accounts/{account_id}/orders/{order_id}/execute", "POST"),
        ]
        openapi_paths = main.app.openapi()["paths"]
        for path, method in targets:
            matches = [
                route
                for route in main.app.router.routes
                if getattr(route, "path", None) == path
                and method in (getattr(route, "methods", None) or set())
            ]
            assert len(matches) == 1, (path, method, len(matches))
            assert method.lower() in openapi_paths[path]
        """
    )
