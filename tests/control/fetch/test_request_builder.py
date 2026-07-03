"""Unit tests — FetchRequestBuilder chunking and manifest shape."""

from datetime import date

import pytest

from src.control.fetch.request_builder import FetchRequestBuilder
from src.control.fetch.models import FetchRequest


@pytest.fixture
def builder():
    return FetchRequestBuilder(raw_bucket="test-raw-bucket")


def _build(builder, start, end, batch_size_days=30, symbol="AAPL"):
    return builder.build(
        batch_id="batch_daily_20260704_190000",
        instrument_id=5,
        symbol=symbol,
        vendor="ibkr",
        stream="daily",
        bar_interval="1d",
        start_date=start,
        end_date=end,
        load_type="INCREMENTAL",
        batch_size_days=batch_size_days,
    )


def test_single_chunk_when_range_fits(builder):
    reqs = _build(builder, date(2026, 7, 1), date(2026, 7, 3))
    assert len(reqs) == 1
    assert reqs[0].start_date == date(2026, 7, 1)
    assert reqs[0].end_date == date(2026, 7, 3)


def test_chunking_splits_long_range(builder):
    # 90 days at 30/chunk = 3 chunks, contiguous, no overlap, no gap
    reqs = _build(builder, date(2026, 1, 1), date(2026, 3, 31), batch_size_days=30)
    assert len(reqs) == 3
    assert reqs[0].start_date == date(2026, 1, 1)
    for prev, nxt in zip(reqs, reqs[1:]):
        assert (nxt.start_date - prev.end_date).days == 1
    assert reqs[-1].end_date == date(2026, 3, 31)


def test_request_keys_are_unique_and_sequenced(builder):
    reqs = _build(builder, date(2026, 1, 1), date(2026, 3, 31), batch_size_days=30)
    keys = [r.request_key for r in reqs]
    assert len(set(keys)) == len(keys)
    assert keys[0].endswith("_AAPL_001")
    assert keys[2].endswith("_AAPL_003")


def test_single_day_range(builder):
    reqs = _build(builder, date(2026, 7, 4), date(2026, 7, 4))
    assert len(reqs) == 1
    assert reqs[0].start_date == reqs[0].end_date == date(2026, 7, 4)


def test_end_before_start_raises(builder):
    with pytest.raises(ValueError):
        _build(builder, date(2026, 7, 4), date(2026, 7, 1))


def test_invalid_batch_size_raises(builder):
    with pytest.raises(ValueError):
        _build(builder, date(2026, 7, 1), date(2026, 7, 4), batch_size_days=0)


def test_land_to_is_vendor_first_dataset_path(builder):
    req = _build(builder, date(2026, 7, 1), date(2026, 7, 2))[0]
    assert req.land_to.startswith("s3://test-raw-bucket/ibkr/ohlcv_daily/ingest_date=")


def test_manifest_dict_is_json_safe(builder):
    req = _build(builder, date(2026, 7, 1), date(2026, 7, 2))[0]
    m = req.manifest_dict()
    assert m["start_date"] == "2026-07-01"
    assert m["end_date"] == "2026-07-02"
    assert m["task_type"] == "FETCH_OHLCV"
    assert m["instrument_id"] == 5
    import json
    json.dumps(m)  # must not raise
