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


def test_main_is_small_facade_and_legacy_surface_is_preserved():
    _run_isolated(
        """
        from pathlib import Path
        from unittest.mock import patch
        import main

        assert Path(main.__file__).name == "main.py"
        assert main.app is main._legacy.app
        assert main.db is main._legacy.db
        assert len(Path(main.__file__).read_text().splitlines()) < 80
        assert len(Path(main._legacy.__file__).read_text().splitlines()) > 1000

        original = main._legacy.DATABASE_AGENT_API_KEY
        with patch.object(main, "DATABASE_AGENT_API_KEY", "patched-key"):
            assert main.DATABASE_AGENT_API_KEY == "patched-key"
            assert main._legacy.DATABASE_AGENT_API_KEY == "patched-key"
        assert main._legacy.DATABASE_AGENT_API_KEY == original
        """
    )


def test_modular_runtime_registers_migrated_routes_once():
    _run_isolated(
        """
        import main

        targets = [
            ("/history/signals", "GET"),
            ("/history/performance", "POST"),
            ("/accounts/{account_id}/fills", "POST"),
            ("/execution-jobs/{job_id}", "PATCH"),
            ("/accounts/{account_id}/orders", "POST"),
            ("/accounts/{account_id}/orders/{order_id}/execute", "POST"),
        ]
        for path, method in targets:
            matches = [
                route
                for route in main.app.router.routes
                if getattr(route, "path", None) == path
                and method in (getattr(route, "methods", None) or set())
            ]
            assert len(matches) == 1, (path, method, len(matches))
        """
    )
