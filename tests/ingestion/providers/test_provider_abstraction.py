"""
Tests for provider abstraction layer.
Uses YahooProvider as the concrete implementation under test
since it requires no credentials or running services.
Tests verify the ABC contract, factory pattern, and Yahoo mapping.
"""
import pytest
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, patch
from src.config.config_loader import ConfigLoader
from src.ingestion.factory.provider_factory import MarketDataFactory
from src.ingestion.providers.yahoo_provider import YahooProvider
from src.ingestion.providers.ibkr_provider import IBKRProvider
from src.ingestion.base.market_data_provider import (
    MarketDataProvider, ProviderNotSupportedError
)

REPO_ROOT = Path(__file__).resolve().parents[3]

# ---------------------------------------------------------------------------
# Live gateway guard — skip tests that require a running, authenticated session
# ---------------------------------------------------------------------------
def _gateway_is_authenticated() -> bool:
    """Return True only if IBKR gateway is reachable AND authenticated."""
    try:
        import ssl, urllib.request, json
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        req = urllib.request.Request(
            "https://localhost:5055/v1/api/iserver/auth/status"
        )
        req.add_header("User-Agent", "TradeAnalytics/test")
        with urllib.request.urlopen(req, context=ctx, timeout=3) as resp:
            data = json.loads(resp.read().decode())
            return bool(data.get("authenticated", False))
    except Exception:
        return False

requires_live_gateway = pytest.mark.skipif(
    not _gateway_is_authenticated(),
    reason="IBKR gateway not running or not authenticated — skipping live test",
)


@pytest.fixture(autouse=True)
def reset_config():
    ConfigLoader.reset()
    yield
    ConfigLoader.reset()


@pytest.fixture
def config():
    return ConfigLoader.load(environment="dev", repo_root=REPO_ROOT)


@pytest.fixture
def yahoo_provider(config):
    return YahooProvider(config)


@pytest.fixture
def ibkr_provider(config):
    return IBKRProvider(config)


# ── Abstract base class contract tests ────────────────────────────────────────

def test_market_data_provider_is_abstract():
    """Cannot instantiate the ABC directly."""
    with pytest.raises(TypeError):
        MarketDataProvider(MagicMock())


def test_yahoo_is_subclass_of_provider(yahoo_provider):
    assert isinstance(yahoo_provider, MarketDataProvider)


def test_ibkr_is_subclass_of_provider(ibkr_provider):
    assert isinstance(ibkr_provider, MarketDataProvider)


# ── Yahoo provider identity tests ─────────────────────────────────────────────

def test_yahoo_provider_name(yahoo_provider):
    assert yahoo_provider.provider_name == "yahoo"


def test_yahoo_does_not_support_options(yahoo_provider):
    assert yahoo_provider.supports_options == False


def test_yahoo_does_not_support_realtime(yahoo_provider):
    assert yahoo_provider.supports_realtime == False


def test_yahoo_supported_intervals(yahoo_provider):
    assert "1d" in yahoo_provider.supported_intervals
    assert "1h" in yahoo_provider.supported_intervals


def test_yahoo_supports_interval_1d(yahoo_provider):
    assert yahoo_provider.supports_interval("1d") == True


def test_yahoo_does_not_support_4h(yahoo_provider):
    assert yahoo_provider.supports_interval("4h") == False


def test_yahoo_get_provider_info(yahoo_provider):
    info = yahoo_provider.get_provider_info()
    assert info["provider_name"] == "yahoo"
    assert "supported_intervals" in info
    assert "supports_options" in info


# ── IBKR provider identity tests ──────────────────────────────────────────────

def test_ibkr_provider_name(ibkr_provider):
    assert ibkr_provider.provider_name == "ibkr"


def test_ibkr_supports_options(ibkr_provider):
    assert ibkr_provider.supports_options == True


def test_ibkr_supports_realtime(ibkr_provider):
    assert ibkr_provider.supports_realtime == True


def test_ibkr_supports_4h_interval(ibkr_provider):
    assert ibkr_provider.supports_interval("4h") == True


@requires_live_gateway
def test_ibkr_health_check_returns_bool(ibkr_provider):
    """IBKR health_check returns bool — True if gateway running, False if not."""
    result = ibkr_provider.health_check()
    assert isinstance(result, bool)


@requires_live_gateway
def test_ibkr_get_historical_returns_list(ibkr_provider):
    """IBKR get_historical returns a list (may be empty if gateway not running)."""
    result = ibkr_provider.get_historical(
        "AAPL", date(2026, 1, 1), date(2026, 6, 19)
    )
    assert isinstance(result, list)


@requires_live_gateway
def test_ibkr_get_latest_quote_returns_dict_or_none(ibkr_provider):
    """IBKR get_latest_quote returns dict if gateway running, None if not."""
    result = ibkr_provider.get_latest_quote("AAPL")
    assert result is None or isinstance(result, dict)


def test_ibkr_get_options_chain_returns_empty_when_stub(ibkr_provider):
    assert ibkr_provider.get_options_chain("AAPL", date(2026, 7, 18)) == []


# ── Yahoo options not supported ───────────────────────────────────────────────

def test_yahoo_get_options_chain_raises(yahoo_provider):
    with pytest.raises(ProviderNotSupportedError):
        yahoo_provider.get_options_chain("AAPL", date(2026, 7, 18))


def test_yahoo_unsupported_interval_raises(yahoo_provider):
    with pytest.raises(ProviderNotSupportedError):
        yahoo_provider.get_historical(
            "AAPL", date(2026, 1, 1), date(2026, 6, 19),
            interval="4h"  # not supported by Yahoo
        )


# ── Factory tests ─────────────────────────────────────────────────────────────

def test_factory_returns_ibkr_for_ibkr_config(config):
    provider = MarketDataFactory.get_provider(config)
    assert provider.provider_name == "ibkr"


def test_factory_returns_yahoo_for_fallback(config):
    provider = MarketDataFactory.get_fallback_provider(config)
    assert provider.provider_name == "yahoo"  # config.sources.fallback = yahoo


def test_factory_get_by_name_yahoo(config):
    provider = MarketDataFactory.get_provider_by_name("yahoo", config)
    assert isinstance(provider, YahooProvider)


def test_factory_get_by_name_ibkr(config):
    provider = MarketDataFactory.get_provider_by_name("ibkr", config)
    assert isinstance(provider, IBKRProvider)


def test_factory_raises_for_unknown_provider(config):
    with pytest.raises(ValueError, match="Unknown data source"):
        MarketDataFactory.get_provider_by_name("nonexistent", config)


def test_factory_registered_providers_includes_ibkr_and_yahoo(config):
    providers = MarketDataFactory.registered_providers()
    assert "ibkr" in providers
    assert "yahoo" in providers


# ── Yahoo historical data mapping tests (mocked) ──────────────────────────────

def test_yahoo_get_historical_returns_standard_format(yahoo_provider):
    """
    Mock yfinance to test the mapping logic without network calls.
    Verifies the standard OHLCV dict format is returned.
    """
    import pandas as pd
    from datetime import timezone

    # Create a mock DataFrame row matching yfinance output
    idx = pd.Timestamp("2026-06-19", tz="America/New_York")
    mock_df = pd.DataFrame({
        "Open":         [150.00],
        "High":         [152.50],
        "Low":          [149.75],
        "Close":        [151.25],
        "Volume":       [1_000_000],
        "Dividends":    [0.0],
        "Stock Splits": [0.0],
    }, index=[idx])

    mock_ticker = MagicMock()
    mock_ticker.history.return_value = mock_df
    yahoo_provider._yf.Ticker = MagicMock(return_value=mock_ticker)

    records = yahoo_provider.get_historical(
        "AAPL",
        date(2026, 6, 19),
        date(2026, 6, 19),
        interval="1d",
    )

    assert len(records) == 1
    record = records[0]

    # Verify standard format fields present
    assert record["symbol"]   == "AAPL"
    assert record["source"]   == "yahoo"
    assert record["interval"] == "1d"
    assert record["open"]     == 150.00
    assert record["high"]     == 152.50
    assert record["low"]      == 149.75
    assert record["close"]    == 151.25
    assert record["volume"]   == 1_000_000

    # Fields Yahoo cannot supply must be None — not fabricated
    assert record["vwap"]        is None
    assert record["trade_count"] is None
    assert record["prev_close"]  is None


def test_yahoo_maps_dividend_correctly(yahoo_provider):
    """Dividend records must set is_ex_dividend_date=True and dividend_amount."""
    import pandas as pd

    idx = pd.Timestamp("2026-06-19", tz="America/New_York")
    mock_df = pd.DataFrame({
        "Open": [150.0], "High": [152.0], "Low": [149.0],
        "Close": [151.0], "Volume": [1_000_000],
        "Dividends": [0.75],  # $0.75 dividend
        "Stock Splits": [0.0],
    }, index=[idx])

    mock_ticker = MagicMock()
    mock_ticker.history.return_value = mock_df
    yahoo_provider._yf.Ticker = MagicMock(return_value=mock_ticker)

    records = yahoo_provider.get_historical(
        "AAPL", date(2026, 6, 19), date(2026, 6, 19)
    )
    record = records[0]

    assert record["dividend_amount"]    == 0.75
    assert record["is_ex_dividend_date"] == True
    assert record["has_corporate_action"] == True


def test_yahoo_maps_split_correctly(yahoo_provider):
    """Split records must set split_factor and is_ex_split_date=True."""
    import pandas as pd

    idx = pd.Timestamp("2026-06-19", tz="America/New_York")
    mock_df = pd.DataFrame({
        "Open": [600.0], "High": [605.0], "Low": [598.0],
        "Close": [601.0], "Volume": [500_000],
        "Dividends": [0.0],
        "Stock Splits": [4.0],  # 4:1 split
    }, index=[idx])

    mock_ticker = MagicMock()
    mock_ticker.history.return_value = mock_df
    yahoo_provider._yf.Ticker = MagicMock(return_value=mock_ticker)

    records = yahoo_provider.get_historical(
        "AAPL", date(2026, 6, 19), date(2026, 6, 19)
    )
    record = records[0]

    assert record["is_ex_split_date"]     == True
    assert record["split_factor"]          == pytest.approx(0.25)  # 1/4
    assert record["has_corporate_action"]  == True


def test_yahoo_returns_empty_for_empty_dataframe(yahoo_provider):
    """Empty DataFrame from yfinance → empty list, no error."""
    import pandas as pd

    mock_ticker = MagicMock()
    mock_ticker.history.return_value = pd.DataFrame()
    yahoo_provider._yf.Ticker = MagicMock(return_value=mock_ticker)

    records = yahoo_provider.get_historical(
        "AAPL", date(2026, 6, 19), date(2026, 6, 19)
    )
    assert records == []


def test_yahoo_is_market_open_false_on_weekend(yahoo_provider):
    """Saturday/Sunday must return False."""
    with patch("src.ingestion.providers.yahoo_provider.datetime") as mock_dt:
        # Saturday 2026-06-20
        mock_sat = MagicMock()
        mock_sat.weekday.return_value = 5
        mock_dt.now.return_value = mock_sat
        assert yahoo_provider.is_market_open() == False


def test_yahoo_repr(yahoo_provider):
    r = repr(yahoo_provider)
    assert "yahoo" in r
    assert "YahooProvider" in r
