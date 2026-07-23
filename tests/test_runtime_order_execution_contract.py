import os
import subprocess
import sys
import textwrap


def _run_isolated_runtime(script: str) -> None:
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
        f"isolated runtime check failed\nstdout:\n{result.stdout}\n"
        f"stderr:\n{result.stderr}"
    )


def test_curator_routes_are_registered_exactly_once_in_runtime():
    _run_isolated_runtime(
        """
        import main_with_skill_routes as runtime

        targets = [
            ("/skills/performance/rank", "GET"),
            ("/backtests/runs/latest", "GET"),
            ("/skills/{skill_id}/backtest-status", "GET"),
        ]
        for path, method in targets:
            count = sum(
                1
                for route in runtime.app.router.routes
                if getattr(route, "path", None) == path
                and method in (getattr(route, "methods", None) or set())
            )
            assert count == 1, (path, count)
        """
    )


def test_execute_order_endpoint_returns_declared_response_object():
    _run_isolated_runtime(
        """
        from unittest.mock import patch
        from fastapi.testclient import TestClient
        import main_with_skill_routes as runtime

        client = TestClient(runtime.app)
        order_before = {
            "order_id": 42,
            "trade_id": "trade-runtime-42",
            "account_id": 1,
        }
        order_after = {**order_before, "status": "executed"}

        with patch.object(runtime.main_module, "DATABASE_AGENT_API_KEY", "test-key"), \
                patch.object(
                    runtime.db,
                    "get_order_by_id",
                    side_effect=[order_before, order_after],
                ), \
                patch.object(
                    runtime.db,
                    "_runtime_original_execute_order",
                    return_value=("executed", None, 1),
                ) as execute_order:
            response = client.post(
                "/accounts/1/orders/42/execute",
                headers={
                    "X-API-KEY": "test-key",
                    "X-Correlation-ID": "corr-runtime-execute",
                },
            )

        assert response.status_code == 200
        body = response.json()
        assert body["status"] == "success"
        assert body["data"] == {
            "order_id": 42,
            "trade_id": "trade-runtime-42",
            "account_id": 1,
            "status": "executed",
            "reason": None,
        }
        execute_order.assert_called_once_with(42)
        """
    )


def test_execute_order_endpoint_rejects_account_mismatch_without_execution():
    _run_isolated_runtime(
        """
        from unittest.mock import patch
        from fastapi.testclient import TestClient
        import main_with_skill_routes as runtime

        client = TestClient(runtime.app)
        persisted_order = {
            "order_id": 42,
            "trade_id": "trade-runtime-42",
            "account_id": 2,
        }

        with patch.object(runtime.main_module, "DATABASE_AGENT_API_KEY", "test-key"), \
                patch.object(
                    runtime.db,
                    "get_order_by_id",
                    return_value=persisted_order,
                ), \
                patch.object(
                    runtime.db,
                    "_runtime_original_execute_order",
                    return_value=("executed", None, 2),
                ) as execute_order:
            response = client.post(
                "/accounts/1/orders/42/execute",
                headers={"X-API-KEY": "test-key"},
            )

        assert response.status_code == 200
        body = response.json()
        assert body["data"]["status"] == "failed"
        assert body["data"]["reason"] == "account_mismatch"
        assert body["data"]["account_id"] == 1
        execute_order.assert_not_called()
        """
    )


def test_legacy_single_argument_execute_order_contract_is_preserved():
    _run_isolated_runtime(
        """
        from unittest.mock import patch
        import main_with_skill_routes as runtime

        expected = ("failed", "invalid_state", 1)
        with patch.object(
            runtime.db,
            "_runtime_original_execute_order",
            return_value=expected,
        ) as execute_order:
            result = runtime.db.execute_order(42)

        assert result == expected
        execute_order.assert_called_once_with(42)
        """
    )
