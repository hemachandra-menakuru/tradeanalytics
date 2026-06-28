"""Tests for WatermarkManager — local mode only."""
import pytest
from datetime import date
from src.bronze.writers.watermark_manager import WatermarkManager
from src.bronze.models.ingestion_mode import IngestionWatermark


@pytest.fixture
def manager():
    return WatermarkManager(mode="local")


def test_get_watermark_returns_none_for_unknown(manager):
    wm = manager.get_watermark("AAPL", "1d")
    assert wm is None


def test_update_and_get_watermark(manager):
    manager.update_watermark(
        symbol="AAPL", interval="1d",
        earliest_date=date(2016, 1, 4),
        latest_date=date(2026, 6, 22),
        record_count=2626,
        batch_id="batch_001",
        mode="initial_load",
    )
    wm = manager.get_watermark("AAPL", "1d")
    assert wm is not None
    assert wm.symbol == "AAPL"
    assert wm.latest_date == date(2026, 6, 22)
    assert wm.earliest_date == date(2016, 1, 4)


def test_watermark_update_preserves_earliest_date(manager):
    """earliest_date should only ever move backwards, not forwards."""
    manager.update_watermark(
        symbol="AAPL", interval="1d",
        earliest_date=date(2016, 1, 4),
        latest_date=date(2026, 6, 22),
        record_count=2626, batch_id="batch_001", mode="initial_load",
    )
    # Try to update with a later earliest_date
    manager.update_watermark(
        symbol="AAPL", interval="1d",
        earliest_date=date(2020, 1, 1),  # later — should be ignored
        latest_date=date(2026, 6, 23),
        record_count=2627, batch_id="batch_002", mode="incremental",
    )
    wm = manager.get_watermark("AAPL", "1d")
    assert wm.earliest_date == date(2016, 1, 4)  # preserved original


def test_watermark_update_preserves_latest_date(manager):
    """latest_date should only ever move forwards, not backwards."""
    manager.update_watermark(
        symbol="AAPL", interval="1d",
        earliest_date=date(2016, 1, 4),
        latest_date=date(2026, 6, 22),
        record_count=2626, batch_id="batch_001", mode="incremental",
    )
    # Try to update with an earlier latest_date
    manager.update_watermark(
        symbol="AAPL", interval="1d",
        earliest_date=date(2016, 1, 4),
        latest_date=date(2026, 6, 20),  # earlier — should be ignored
        record_count=2625, batch_id="batch_002", mode="backfill",
    )
    wm = manager.get_watermark("AAPL", "1d")
    assert wm.latest_date == date(2026, 6, 22)  # preserved original


def test_multiple_symbols_tracked_independently(manager):
    manager.update_watermark(
        symbol="AAPL", interval="1d",
        earliest_date=date(2016, 1, 4),
        latest_date=date(2026, 6, 22),
        record_count=2626, batch_id="batch_001", mode="incremental",
    )
    manager.update_watermark(
        symbol="MSFT", interval="1d",
        earliest_date=date(2016, 1, 4),
        latest_date=date(2026, 6, 20),  # different latest date
        record_count=2624, batch_id="batch_001", mode="incremental",
    )
    aapl_wm = manager.get_watermark("AAPL", "1d")
    msft_wm = manager.get_watermark("MSFT", "1d")
    assert aapl_wm.latest_date != msft_wm.latest_date


def test_get_all_watermarks(manager):
    manager.update_watermark(
        "AAPL", "1d", date(2016, 1, 4), date(2026, 6, 22),
        2626, "batch_001", "incremental",
    )
    manager.update_watermark(
        "MSFT", "1d", date(2016, 1, 4), date(2026, 6, 22),
        2626, "batch_001", "incremental",
    )
    all_wm = manager.get_all_watermarks()
    assert len(all_wm) == 2


def test_watermark_status_recorded(manager):
    manager.update_watermark(
        "AAPL", "1d", date(2016, 1, 4), date(2026, 6, 22),
        2626, "batch_001", "incremental",
        status="success",
    )
    wm = manager.get_watermark("AAPL", "1d")
    assert wm.last_run_status == "success"
    assert wm.last_mode == "incremental"
    assert wm.last_batch_id == "batch_001"
