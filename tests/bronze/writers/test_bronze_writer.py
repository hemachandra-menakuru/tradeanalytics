"""Tests for BronzeWriter — local mode only."""
import pytest
from src.bronze.writers.bronze_writer import BronzeWriter, BronzeWriteResult
from unittest.mock import MagicMock

MAIN_TABLE     = "market_data_daily"
REJECTED_TABLE = "market_data_rejected"


@pytest.fixture
def writer():
    return BronzeWriter(mode="local")


@pytest.fixture
def clean_record():
    return {
        "symbol": "AAPL", "bar_date": "2026-06-22", "interval": "1d",
        "source": "yahoo",
        "open": 150.0, "high": 152.5, "low": 149.75, "close": 151.25,
        "volume": 1_000_000,
        "data_quality_flag": False,
        "data_quality_reasons": "[]",
        "record_version": 1,
    }


@pytest.fixture
def rejected_record():
    return {
        "symbol": "AAPL", "bar_date": "2026-06-22", "interval": "1d",
        "source": "yahoo",
        "rejected_rule": "high_gte_low",
        "rejection_reason": "High < Low",
        "raw_record": "{}",
        "rejected_at": "2026-06-22T19:00:00Z",
        "reprocessed": False,
    }


# ── Write tests ───────────────────────────────────────────────────────────────

def test_write_clean_record(writer, clean_record):
    result = writer.write_batch(
        symbol="AAPL", interval="1d", batch_id="batch_001",
        clean_records=[clean_record], rejected_records=[],
        main_table=MAIN_TABLE, rejected_table=REJECTED_TABLE,
    )
    assert result.records_written == 1
    assert result.records_skipped == 0
    assert result.records_amended == 0


def test_write_rejected_record(writer, rejected_record):
    result = writer.write_batch(
        symbol="AAPL", interval="1d", batch_id="batch_001",
        clean_records=[], rejected_records=[rejected_record],
        main_table=MAIN_TABLE, rejected_table=REJECTED_TABLE,
    )
    assert result.rejected_written == 1
    rejected = writer.get_local_records(REJECTED_TABLE)
    assert len(rejected) == 1


def test_write_empty_batch(writer):
    result = writer.write_batch(
        symbol="AAPL", interval="1d", batch_id="batch_001",
        clean_records=[], rejected_records=[],
        main_table=MAIN_TABLE, rejected_table=REJECTED_TABLE,
    )
    assert result.records_written == 0
    assert result.rejected_written == 0


def test_duplicate_record_is_skipped(writer, clean_record):
    # Write once
    writer.write_batch(
        symbol="AAPL", interval="1d", batch_id="batch_001",
        clean_records=[clean_record], rejected_records=[],
        main_table=MAIN_TABLE, rejected_table=REJECTED_TABLE,
    )
    # Write same record again
    result = writer.write_batch(
        symbol="AAPL", interval="1d", batch_id="batch_002",
        clean_records=[clean_record], rejected_records=[],
        main_table=MAIN_TABLE, rejected_table=REJECTED_TABLE,
    )
    assert result.records_skipped == 1
    assert result.records_written == 0


def test_amended_record_is_written(writer, clean_record):
    # Write original
    writer.write_batch(
        symbol="AAPL", interval="1d", batch_id="batch_001",
        clean_records=[clean_record], rejected_records=[],
        main_table=MAIN_TABLE, rejected_table=REJECTED_TABLE,
    )
    # Write amendment with different close price
    amended = clean_record.copy()
    amended["close"] = 152.00
    amended["record_version"] = 2

    result = writer.write_batch(
        symbol="AAPL", interval="1d", batch_id="batch_002",
        clean_records=[amended], rejected_records=[],
        main_table=MAIN_TABLE, rejected_table=REJECTED_TABLE,
    )
    assert result.records_amended == 1
    assert result.records_skipped == 0


def test_records_stored_in_local_store(writer, clean_record):
    writer.write_batch(
        symbol="AAPL", interval="1d", batch_id="batch_001",
        clean_records=[clean_record], rejected_records=[],
        main_table=MAIN_TABLE, rejected_table=REJECTED_TABLE,
    )
    records = writer.get_local_records(MAIN_TABLE)
    assert len(records) == 1


def test_clear_local_store(writer, clean_record):
    writer.write_batch(
        symbol="AAPL", interval="1d", batch_id="batch_001",
        clean_records=[clean_record], rejected_records=[],
        main_table=MAIN_TABLE, rejected_table=REJECTED_TABLE,
    )
    writer.clear_local_store()
    assert writer.get_local_record_count(MAIN_TABLE) == 0


def test_write_result_repr(writer, clean_record):
    result = writer.write_batch(
        symbol="AAPL", interval="1d", batch_id="batch_001",
        clean_records=[clean_record], rejected_records=[],
        main_table=MAIN_TABLE, rejected_table=REJECTED_TABLE,
    )
    r = repr(result)
    assert "AAPL" in r
    assert "written=1" in r


def test_total_processed(writer, clean_record):
    result = writer.write_batch(
        symbol="AAPL", interval="1d", batch_id="batch_001",
        clean_records=[clean_record], rejected_records=[],
        main_table=MAIN_TABLE, rejected_table=REJECTED_TABLE,
    )
    assert result.total_processed == 1
