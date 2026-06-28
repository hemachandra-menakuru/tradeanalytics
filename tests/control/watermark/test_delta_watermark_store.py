"""
Tests for DeltaWatermarkStore (local mode — no Spark required).

Scenarios:
  1. get_watermark returns None on first run (INITIAL_LOAD signal)
  2. update_watermark stores and get_watermark retrieves it
  3. Dates only widen: earliest never moves forward, latest never backward
  4. consecutive_failures increments on failure, resets to 0 on success
  5. list_watermarks returns all stored records
  6. list_watermarks filtered by stream returns only matching records
  7. Two instruments with same stream are stored independently
  8. Two streams for same instrument are stored independently
  9. spark mode raises ValueError when spark=None
 10. DeltaWatermarkStore implements WatermarkStore ABC
"""

import pytest
from datetime import date

from src.control.watermark.delta_watermark_store import DeltaWatermarkStore
from src.shared.base.watermark_store import WatermarkStore


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def store():
    return DeltaWatermarkStore(mode="local")


def _update(store, instrument_id=505, stream="daily",
            earliest=date(2026, 1, 2), latest=date(2026, 6, 24),
            record_count=100, status="success"):
    store.update_watermark(
        instrument_id=instrument_id,
        stream=stream,
        earliest_date=earliest,
        latest_date=latest,
        record_count=record_count,
        batch_id="batch_001",
        mode="INITIAL_LOAD",
        status=status,
    )


# ---------------------------------------------------------------------------
# get_watermark
# ---------------------------------------------------------------------------

def test_get_watermark_returns_none_on_first_run(store):
    assert store.get_watermark(instrument_id=505, stream="daily") is None


def test_get_watermark_returns_record_after_update(store):
    _update(store)
    wm = store.get_watermark(instrument_id=505, stream="daily")
    assert wm is not None
    assert wm.instrument_id == 505
    assert wm.stream == "daily"
    assert wm.earliest_date == date(2026, 1, 2)
    assert wm.latest_date == date(2026, 6, 24)
    assert wm.record_count == 100
    assert wm.status == "success"


# ---------------------------------------------------------------------------
# Date widening
# ---------------------------------------------------------------------------

def test_earliest_date_never_moves_forward(store):
    _update(store, earliest=date(2026, 3, 1), latest=date(2026, 6, 24))
    # Second update with a later earliest — should be ignored
    _update(store, earliest=date(2026, 6, 1), latest=date(2026, 6, 25))
    wm = store.get_watermark(505, "daily")
    assert wm.earliest_date == date(2026, 3, 1)  # preserved


def test_latest_date_never_moves_backward(store):
    _update(store, earliest=date(2026, 1, 2), latest=date(2026, 6, 24))
    # Second update with an earlier latest — should be ignored
    _update(store, earliest=date(2026, 1, 2), latest=date(2026, 5, 1))
    wm = store.get_watermark(505, "daily")
    assert wm.latest_date == date(2026, 6, 24)  # preserved


def test_earliest_moves_backward_on_history_extension(store):
    _update(store, earliest=date(2026, 3, 1), latest=date(2026, 6, 24))
    _update(store, earliest=date(2020, 1, 2), latest=date(2026, 6, 24))
    wm = store.get_watermark(505, "daily")
    assert wm.earliest_date == date(2020, 1, 2)  # extended backward


def test_latest_moves_forward_on_incremental(store):
    _update(store, earliest=date(2026, 1, 2), latest=date(2026, 6, 24))
    _update(store, earliest=date(2026, 1, 2), latest=date(2026, 6, 27))
    wm = store.get_watermark(505, "daily")
    assert wm.latest_date == date(2026, 6, 27)  # extended forward


# ---------------------------------------------------------------------------
# consecutive_failures
# ---------------------------------------------------------------------------

def test_consecutive_failures_increments_on_failure(store):
    _update(store, status="success")
    _update(store, status="failed")
    _update(store, status="failed")
    wm = store.get_watermark(505, "daily")
    assert wm.consecutive_failures == 2


def test_consecutive_failures_resets_on_success(store):
    _update(store, status="failed")
    _update(store, status="failed")
    _update(store, status="success")
    wm = store.get_watermark(505, "daily")
    assert wm.consecutive_failures == 0


def test_first_run_failure_sets_consecutive_failures_to_1(store):
    _update(store, status="failed")
    wm = store.get_watermark(505, "daily")
    assert wm.consecutive_failures == 1


# ---------------------------------------------------------------------------
# list_watermarks
# ---------------------------------------------------------------------------

def test_list_watermarks_returns_all(store):
    _update(store, instrument_id=505, stream="daily")
    _update(store, instrument_id=506, stream="daily")
    assert len(store.list_watermarks()) == 2


def test_list_watermarks_filtered_by_stream(store):
    _update(store, instrument_id=505, stream="daily")
    _update(store, instrument_id=505, stream="intraday")
    _update(store, instrument_id=506, stream="daily")
    daily = store.list_watermarks(stream="daily")
    assert len(daily) == 2
    assert all(r.stream == "daily" for r in daily)


def test_list_watermarks_empty_on_fresh_store(store):
    assert store.list_watermarks() == []


# ---------------------------------------------------------------------------
# Isolation — different instruments / streams
# ---------------------------------------------------------------------------

def test_two_instruments_same_stream_stored_independently(store):
    _update(store, instrument_id=505, stream="daily", record_count=100)
    _update(store, instrument_id=506, stream="daily", record_count=200)
    assert store.get_watermark(505, "daily").record_count == 100
    assert store.get_watermark(506, "daily").record_count == 200


def test_two_streams_same_instrument_stored_independently(store):
    _update(store, instrument_id=505, stream="daily",    record_count=100)
    _update(store, instrument_id=505, stream="intraday", record_count=50)
    assert store.get_watermark(505, "daily").record_count    == 100
    assert store.get_watermark(505, "intraday").record_count == 50


# ---------------------------------------------------------------------------
# Spark mode guard
# ---------------------------------------------------------------------------

def test_spark_mode_requires_spark_session():
    with pytest.raises(ValueError, match="SparkSession required"):
        DeltaWatermarkStore(mode="spark", spark=None)


# ---------------------------------------------------------------------------
# ABC compliance
# ---------------------------------------------------------------------------

def test_implements_watermark_store_abc():
    store = DeltaWatermarkStore(mode="local")
    assert isinstance(store, WatermarkStore)
