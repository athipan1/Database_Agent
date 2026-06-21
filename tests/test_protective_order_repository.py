from protective_order_repository import normalize_order_protective_metadata


def test_normalize_order_protective_metadata_parses_json_fields():
    order = {
        "order_id": 1,
        "guard_plan": '{"symbol":"AAPL","side":"sell","quantity":3,"trigger_price":140}',
        "protective_exit": None,
    }

    normalized = normalize_order_protective_metadata(order)

    assert normalized["guard_plan"] == {
        "symbol": "AAPL",
        "side": "sell",
        "quantity": 3,
        "trigger_price": 140,
    }
    assert normalized["protective_exit"] is None


def test_normalize_order_protective_metadata_keeps_dict_fields():
    guard_plan = {"symbol": "AAPL", "side": "sell", "quantity": 3, "trigger_price": 140}
    order = {"order_id": 1, "guard_plan": guard_plan, "protective_exit": None}

    normalized = normalize_order_protective_metadata(order)

    assert normalized["guard_plan"] == guard_plan
