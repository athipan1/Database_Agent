from app import runtime as base_runtime
from app.application import create_application


class RuntimeStub:
    def __getattr__(self, name):
        return getattr(base_runtime, name)

    async def startup_event(self):
        return None

    async def shutdown_event(self):
        return None


def test_position_bucket_contracts_are_published_in_openapi():
    schema = create_application(RuntimeStub()).openapi()
    paths = schema["paths"]

    assert "get" in paths["/accounts/{account_id}/strategy-bucket-assignments"]
    assert "get" in paths["/accounts/{account_id}/position-buckets"]
    assert "patch" in paths["/accounts/{account_id}/position-buckets/{symbol}"]
    assert "post" in paths["/accounts/{account_id}/position-buckets/bulk"]
