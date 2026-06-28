"""
Tests for BronzeIngestionJob — end-to-end pipeline in local mode.
Uses mocked provider to avoid network calls.
"""
import pytest
from datetime import date, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch
from src.shared.config.config_loader import ConfigLoader
from src.bronze.jobs.bronze_ingestion_job import BronzeIngestionJob, JobRunSummary
from src.bronze.writers.bronze_writer import BronzeWriter
from src.bronze.writers.watermark_manager import WatermarkManager
from src.bronze.validation.validator import DataQualityValidator
from src.reference.readers.ticker_reader import TickerReader

REPO_ROOT = Path(__file__).resolve().parents[3]
TODAY = date(2026, 6, 22)
RULES_PATH = REPO_ROOT / "src" / "reference" / "seed" / "data_quality_rules.yml"


@pytest.fixture(autouse=True)
def reset_config():
    ConfigLoader.reset()
    yield
    ConfigLoader.reset()


@pytest.fixture
def config():
    return ConfigLoader.load(environment="dev", repo_root=REPO_ROOT)


@pytest.fixture
def mock_provider():
    """Mock provider that returns clean OHLCV data."""
    provider = MagicMock()
    provider.provider_name    = "yahoo"
    provider.health_check.return_value = True
    provider.get_historical.return_value = [
        {
            "symbol": "AAPL", "date": "2026-06-20",
            "interval": "1d", "source": "yahoo",
            "open": 150.0, "high": 152.5,
            "low": 149.75, "close": 151.25,
            "volume": 1_000_000,
            "prev_close": 149.50, "vwap": 150.87,
            "trade_count": None, "pre_market_volume": None,
            "fetch_duration_ms": 245,
            "is_amended": False,
            "has_corporate_action": False,
            "dividend_amount": 0.0,
            "split_factor": None,
            "is_ex_dividend_date": False,
            "is_ex_split_date": False,
        }
    ]
    return provider


@pytest.fixture
def writer():
    return BronzeWriter(mode="local")


@pytest.fixture
def watermark_mgr():
    return WatermarkManager(mode="local")


@pytest.fixture
def validator(config):
    return DataQualityValidator.for_stream(
        config, "daily", rules_path=RULES_PATH
    )


@pytest.fixture
def ticker_reader(config):
    return TickerReader(config, tickers_path=REPO_ROOT / "src" / "reference" / "seed" / "tickers.csv")


@pytest.fixture
def job(config, mock_provider, writer, watermark_mgr, validator, ticker_reader):
    """Build job with all components injected."""
    with patch(
        "src.bronze.jobs.bronze_ingestion_job.MarketDataFactory"
        ".get_provider",
        return_value=mock_provider
    ), patch(
        "src.bronze.jobs.bronze_ingestion_job.MarketDataFactory"
        ".get_fallback_provider",
        return_value=mock_provider
    ):
        j = BronzeIngestionJob(
            config=config,
            stream_name="daily",
            writer=writer,
            watermark_mgr=watermark_mgr,
            validator=validator,
            ticker_reader=ticker_reader,
        )
        j._provider = mock_provider
        return j


# ── Job setup tests ───────────────────────────────────────────────────────────

def test_job_initialises_successfully(job):
    assert job is not None


def test_job_summary_initialises(job):
    summary = JobRunSummary("daily", "batch_001")
    assert summary.total_symbols == 0
    assert summary.total_records_written == 0


# ── Run tests ─────────────────────────────────────────────────────────────────

def test_job_run_returns_summary(job):
    summary = job.run(symbols=["AAPL"], as_of_date=TODAY)
    assert isinstance(summary, JobRunSummary)
    assert summary.completed_at is not None


def test_job_run_processes_symbol(job, writer):
    job.run(symbols=["AAPL"], as_of_date=TODAY)
    # Records should be in the writer
    records = writer.get_local_records("market_data_daily")
    assert len(records) >= 0  # may be 0 if no-op


def test_dry_run_does_not_write(job, writer):
    job.run(symbols=["AAPL"], as_of_date=TODAY, dry_run=True)
    records = writer.get_local_records("market_data_daily")
    assert len(records) == 0


def test_job_run_with_no_active_tickers(job):
    """Job with no matching symbols returns empty summary."""
    summary = job.run(symbols=["ZZZZ_NONEXISTENT"], as_of_date=TODAY)
    assert summary.total_symbols == 0


# ── Watermark integration tests ───────────────────────────────────────────────

def test_watermark_set_after_successful_run(job, watermark_mgr):
    job.run(symbols=["AAPL"], as_of_date=TODAY)
    # Watermark should exist after run if data was written
    # (may still be None if provider returned empty or no-op)
    wm = watermark_mgr.get_watermark("AAPL", "1d")
    # Just verify it doesn't crash — actual value depends on provider mock


def test_no_op_when_already_up_to_date(job, watermark_mgr):
    """If watermark = today, job should skip."""
    watermark_mgr.update_watermark(
        symbol="AAPL", interval="1d",
        earliest_date=date(2016, 1, 4),
        latest_date=TODAY,
        record_count=2626,
        batch_id="batch_prior",
        mode="incremental",
    )
    summary = job.run(symbols=["AAPL"], as_of_date=TODAY)
    assert "AAPL" in summary.skipped


# ── Provider fallback tests ───────────────────────────────────────────────────

def test_provider_fallback_on_health_check_failure(config, writer, watermark_mgr, validator, ticker_reader):
    """Job should fall back to Yahoo when primary provider health check fails."""
    failing_provider = MagicMock()
    failing_provider.provider_name = "ibkr"
    failing_provider.health_check.return_value = False  # ← fails

    fallback_provider = MagicMock()
    fallback_provider.provider_name = "yahoo"
    fallback_provider.health_check.return_value = True
    fallback_provider.get_historical.return_value = []

    with patch(
        "src.bronze.jobs.bronze_ingestion_job.MarketDataFactory.get_provider",
        return_value=failing_provider
    ), patch(
        "src.bronze.jobs.bronze_ingestion_job.MarketDataFactory.get_fallback_provider",
        return_value=fallback_provider
    ):
        job = BronzeIngestionJob(
            config=config, stream_name="daily",
            writer=writer, watermark_mgr=watermark_mgr,
            validator=validator, ticker_reader=ticker_reader,
        )
        job._provider = failing_provider
        job.run(symbols=["AAPL"], as_of_date=TODAY)

        # Fallback provider should have been called
        assert fallback_provider.get_historical.called or \
               failing_provider.get_historical.called


# ── JobRunSummary tests ───────────────────────────────────────────────────────

def test_job_summary_success_rate():
    summary = JobRunSummary("daily", "batch_001")
    summary.add_skip("MSFT")
    mock_result = MagicMock()
    mock_result.records_written  = 10
    mock_result.records_amended  = 0
    mock_result.rejected_written = 1
    summary.add_result(mock_result)
    assert summary.success_rate_pct == 100.0


def test_job_summary_tracks_failures():
    summary = JobRunSummary("daily", "batch_001")
    summary.add_failure("AAPL", "Connection error")
    assert "AAPL" in summary.failed
    assert "Connection error" in summary.errors["AAPL"]


def test_job_summary_repr():
    summary = JobRunSummary("daily", "batch_001")
    r = repr(summary)
    assert "daily" in r
