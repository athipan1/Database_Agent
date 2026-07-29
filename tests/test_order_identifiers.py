from uuid import UUID

from order_identifiers import client_order_id_for_trade_id


def test_uuid_trade_id_is_preserved_for_client_order_compatibility():
    trade_id = "31678dfa-523a-43f2-9b4e-e7a865657538"

    assert client_order_id_for_trade_id(trade_id) == trade_id


def test_string_trade_id_projects_to_stable_uuid():
    trade_id = "profit:account-102:position-2:HARD:v1:hard-stop"

    first = client_order_id_for_trade_id(trade_id)
    second = client_order_id_for_trade_id(trade_id)

    assert UUID(first)
    assert first == second
    assert first != client_order_id_for_trade_id(f"{trade_id}:retry")


def test_integer_trade_id_projects_to_stable_uuid():
    projected = client_order_id_for_trade_id(1234)

    assert UUID(projected)
    assert projected == client_order_id_for_trade_id("1234")
