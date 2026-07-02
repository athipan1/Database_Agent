from broker_sync_repository import _strategy_bucket_or_existing, _strategy_bucket_source_or_existing
from position_bucket_repository import normalize_strategy_bucket


def test_normalize_strategy_bucket_accepts_known_buckets():
    assert normalize_strategy_bucket("core_dividend") == "core_dividend"
    assert normalize_strategy_bucket("QUALITY_GROWTH") == "quality_growth"
    assert normalize_strategy_bucket("VALUE_REBOUND") == "value_rebound"
    assert normalize_strategy_bucket("news_momentum") == "news_momentum"


def test_normalize_strategy_bucket_rejects_unknown_bucket():
    assert normalize_strategy_bucket("swing_trade") == "unassigned"
    assert normalize_strategy_bucket(None) == "unassigned"


def test_broker_sync_preserves_existing_bucket_when_payload_is_unassigned():
    existing = {
        "strategy_bucket": "value_rebound",
        "strategy_bucket_source": "manual",
        "strategy_bucket_reason": "bucket review backfill",
    }

    assert _strategy_bucket_or_existing({"symbol": "ACGL"}, existing) == "value_rebound"
    assert _strategy_bucket_source_or_existing({"symbol": "ACGL"}, existing) == "manual"


def test_broker_sync_payload_bucket_overrides_existing_bucket():
    existing = {"strategy_bucket": "value_rebound", "strategy_bucket_source": "manual"}
    payload = {"symbol": "ADBE", "strategy_bucket": "core_dividend"}

    assert _strategy_bucket_or_existing(payload, existing) == "core_dividend"
    assert _strategy_bucket_source_or_existing(payload, existing) == "broker_sync_payload"


def test_broker_sync_accepts_quality_growth_bucket():
    existing = {"strategy_bucket": "unassigned", "strategy_bucket_source": "manual"}
    payload = {"symbol": "BKNG", "strategy_bucket": "quality_growth"}

    assert _strategy_bucket_or_existing(payload, existing) == "quality_growth"
    assert _strategy_bucket_source_or_existing(payload, existing) == "broker_sync_payload"
