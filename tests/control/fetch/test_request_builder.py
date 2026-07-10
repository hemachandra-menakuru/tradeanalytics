"""Unit tests — FetchRequestBuilder chunking + Contract v2 manifest shape."""

from datetime import date

import pytest

from src.control.fetch.request_builder import FetchRequestBuilder
from src.control.fetch.models import CONTRACT_VERSION


@pytest.fixture
def builder():
    return FetchRequestBuilder(
        raw_bucket="test-raw-bucket",
        backfill_chunk_days=365,
        pipeline_version="testsha",
    )


def _build(builder, start, end, batch_size_days=30, symbol="AAPL",
           load_type="INCREMENTAL", **kw):
    return builder.build(
        batch_id="batch_daily_20260705_190000",
        stream="daily",
        load_type=load_type,
        bar_interval="1d",
        start_date=start,
        end_date=end,
        batch_size_days=batch_size_days,
        instrument_id=5,
        symbol=symbol,
        vendor="ibkr",
        vendor_instrument_id="265598",
        exchange_mic="XNAS",
        **kw,
    )


# ── Chunking ──────────────────────────────────────────────────────────────────

def test_single_chunk_when_range_fits(builder):
    reqs = _build(builder, date(2026, 7, 1), date(2026, 7, 3))
    assert len(reqs) == 1
    assert reqs[0].start_date == date(2026, 7, 1)
    assert reqs[0].end_date == date(2026, 7, 3)


def test_incremental_uses_batch_size_days(builder):
    # 90 days at 30/chunk = 3 chunks, contiguous, no overlap, no gap
    reqs = _build(builder, date(2026, 1, 1), date(2026, 3, 31), batch_size_days=30)
    assert len(reqs) == 3
    for prev, nxt in zip(reqs, reqs[1:]):
        assert (nxt.start_date - prev.end_date).days == 1
    assert reqs[-1].end_date == date(2026, 3, 31)


def test_initial_load_uses_backfill_chunk_days(builder):
    # 10 years at 365/chunk ≈ 11 chunks — NOT ~122 chunks of 30 days
    reqs = _build(builder, date(2016, 1, 1), date(2026, 7, 3),
                  batch_size_days=30, load_type="INITIAL_LOAD")
    assert 10 <= len(reqs) <= 12
    for prev, nxt in zip(reqs, reqs[1:]):
        assert (nxt.start_date - prev.end_date).days == 1
    assert reqs[0].start_date == date(2016, 1, 1)
    assert reqs[-1].end_date == date(2026, 7, 3)


def test_history_extension_also_uses_backfill_chunks(builder):
    reqs = _build(builder, date(2020, 1, 1), date(2021, 12, 31),
                  load_type="HISTORY_EXTENSION")
    assert len(reqs) == 3  # 731 days (2020 is a leap year) = 365 + 365 + 1


def test_gap_fill_stays_small_chunks(builder):
    reqs = _build(builder, date(2026, 1, 1), date(2026, 3, 31),
                  batch_size_days=30, load_type="GAP_FILL")
    assert len(reqs) == 3


# ── Keys and validation ───────────────────────────────────────────────────────

def test_request_keys_unique_sequenced_with_vendor(builder):
    reqs = _build(builder, date(2026, 1, 1), date(2026, 3, 31), batch_size_days=30)
    keys = [r.request_key for r in reqs]
    assert len(set(keys)) == len(keys)
    assert keys[0].endswith("_ibkr_AAPL_001")
    assert keys[2].endswith("_ibkr_AAPL_003")


def test_end_before_start_raises(builder):
    with pytest.raises(ValueError):
        _build(builder, date(2026, 7, 4), date(2026, 7, 1))


def test_invalid_batch_size_raises(builder):
    with pytest.raises(ValueError):
        _build(builder, date(2026, 7, 1), date(2026, 7, 4), batch_size_days=0)


# ── Contract v2 manifest ──────────────────────────────────────────────────────

def test_manifest_is_contract_v2(builder):
    req = _build(builder, date(2026, 7, 1), date(2026, 7, 2))[0]
    m = req.manifest_dict()

    assert m["contract_version"] == CONTRACT_VERSION == "2"
    assert m["task_type"] == "FETCH_OHLCV"

    inst = m["instrument"]
    assert inst["instrument_id"] == 5
    assert inst["vendor_instrument_id"] == "265598"   # conId → agent needs no qualification
    assert inst["security_type"] == "STK"
    assert inst["exchange"] == "SMART"
    assert inst["currency"] == "USD"
    assert inst["exchange_mic"] == "XNAS"

    fetch = m["fetch"]
    assert fetch["start_date"] == "2026-07-01"
    assert fetch["what_to_show"] == "TRADES"
    assert fetch["use_rth"] is True

    assert m["landing"]["payload_format"] == "ohlcv_json_v1"
    assert m["landing"]["land_to"].startswith("s3://test-raw-bucket/ibkr/ohlcv_daily/ingest_date=")

    lin = m["lineage"]
    assert lin["requested_by"] == "fetch_planner_job"
    assert lin["pipeline_version"] == "testsha"

    import json
    json.dumps(m)  # must be JSON-safe


def test_manifest_without_vendor_id_is_allowed(builder):
    # unmapped instrument → agent falls back to symbol qualification
    reqs = builder.build(
        batch_id="b", stream="daily", load_type="INCREMENTAL", bar_interval="1d",
        start_date=date(2026, 7, 1), end_date=date(2026, 7, 2), batch_size_days=30,
        instrument_id=9, symbol="MSFT", vendor="ibkr",
    )
    m = reqs[0].manifest_dict()
    assert m["instrument"]["vendor_instrument_id"] is None
