from __future__ import annotations

import json

import pytest

from backtest_json_contract import strict_backtest_json_dumps


def test_empty_list_remains_a_list():
    encoded = strict_backtest_json_dumps([])
    assert encoded == "[]"
    assert json.loads(encoded) == []


def test_none_uses_empty_object_default():
    assert strict_backtest_json_dumps(None) == "{}"


@pytest.mark.parametrize("value", [float("nan"), float("inf"), float("-inf")])
def test_non_finite_values_are_rejected(value):
    with pytest.raises(ValueError):
        strict_backtest_json_dumps({"metric": value})
