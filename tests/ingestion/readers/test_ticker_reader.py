"""Tests for TickerReader."""
import pytest
from pathlib import Path
from src.config.config_loader import ConfigLoader
from src.ingestion.readers.ticker_reader import TickerReader, TickerInfo

REPO_ROOT = Path(__file__).resolve().parents[3]


@pytest.fixture(autouse=True)
def reset_config():
    ConfigLoader.reset()
    yield
    ConfigLoader.reset()


@pytest.fixture
def config():
    return ConfigLoader.load(environment="dev", repo_root=REPO_ROOT)


@pytest.fixture
def reader(config):
    return TickerReader(config)


def test_reader_loads_tickers(reader):
    tickers = reader.get_all_tickers()
    assert len(tickers) > 0


def test_reader_returns_ticker_info_objects(reader):
    tickers = reader.get_all_tickers()
    assert all(isinstance(t, TickerInfo) for t in tickers)


def test_active_tickers_all_active(reader):
    tickers = reader.get_active_tickers()
    assert all(t.active for t in tickers)


def test_get_symbols_returns_strings(reader):
    symbols = reader.get_symbols()
    assert all(isinstance(s, str) for s in symbols)
    assert "AAPL" in symbols


def test_get_ticker_by_symbol(reader):
    ticker = reader.get_ticker("AAPL")
    assert ticker is not None
    assert ticker.symbol == "AAPL"
    assert ticker.sector == "Technology"


def test_get_ticker_unknown_symbol_returns_none(reader):
    ticker = reader.get_ticker("ZZZZ")
    assert ticker is None


def test_filter_by_symbols(reader):
    tickers = reader.get_active_tickers(symbols=["AAPL", "MSFT"])
    symbols = [t.symbol for t in tickers]
    assert "AAPL" in symbols
    assert "MSFT" in symbols
    assert "NVDA" not in symbols


def test_filter_by_asset_class(reader):
    equities = reader.get_active_tickers(asset_classes=["equity"])
    assert all(t.asset_class == "equity" for t in equities)


def test_filter_by_sector(reader):
    tech = reader.get_active_tickers(sectors=["Technology"])
    assert all(t.sector == "Technology" for t in tech)


def test_ticker_has_history_start(reader):
    ticker = reader.get_ticker("AAPL")
    assert ticker.history_start is not None


def test_effective_history_start_uses_history_start(reader):
    ticker = reader.get_ticker("AAPL")
    assert ticker.effective_history_start == ticker.history_start


def test_active_count(reader):
    assert reader.active_count > 0
    assert reader.active_count <= reader.ticker_count


def test_ticker_reader_fails_on_missing_file(config):
    with pytest.raises(FileNotFoundError):
        TickerReader(config, tickers_path=Path("/nonexistent/tickers.csv"))
