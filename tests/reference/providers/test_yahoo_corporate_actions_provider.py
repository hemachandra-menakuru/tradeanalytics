"""
Unit tests for YahooCorporateActionsProvider.

Tests cover:
  - Split parsing: forward splits (2:1, 4:1), reverse splits (1:10)
  - Dividend parsing: regular and special dividends
  - Date range filtering: events outside from_date/to_date excluded
  - Missing symbol handling: unknown instrument_ids skipped gracefully
  - API failure: yfinance exception handled without crash
  - vendor_name and health_check behaviour
  - event_factor correctness for each event type
"""

import pytest
from unittest.mock import MagicMock, patch
from datetime import date

import pandas as pd

from src.reference.providers.yahoo_corporate_actions_provider import YahooCorporateActionsProvider


def _make_provider(symbol_map=None):
    return YahooCorporateActionsProvider(
        symbol_map=symbol_map or {1001: "SPLIT2", 1002: "DIVD", 1003: "RSPLIT"}
    )


def _make_splits_series(data: dict):
    """Build a pandas Series of splits as yfinance returns them."""
    idx = pd.DatetimeIndex([pd.Timestamp(d) for d in data.keys()])
    return pd.Series(list(data.values()), index=idx)


def _make_dividends_series(data: dict):
    """Build a pandas Series of dividends as yfinance returns them."""
    idx = pd.DatetimeIndex([pd.Timestamp(d) for d in data.keys()])
    return pd.Series(list(data.values()), index=idx, dtype=float)


# ---------------------------------------------------------------------------
# vendor_name
# ---------------------------------------------------------------------------

def test_vendor_name():
    provider = _make_provider()
    assert provider.vendor_name == "yahoo"


# ---------------------------------------------------------------------------
# Split parsing
# ---------------------------------------------------------------------------

class TestSplitParsing:

    def _mock_ticker(self, splits=None, dividends=None):
        ticker = MagicMock()
        ticker.splits = splits if splits is not None else pd.Series(dtype=float)
        ticker.dividends = dividends if dividends is not None else pd.Series(dtype=float)
        return ticker

    def test_2_for_1_split_parsed_correctly(self):
        """yfinance returns ratio=2.0 for a 2-for-1 split."""
        provider = _make_provider({1001: "SPLIT2"})
        splits = _make_splits_series({"2024-01-15": 2.0})

        with patch("yfinance.Ticker", return_value=self._mock_ticker(splits=splits)):
            events = provider.get_corporate_actions(
                [1001], date(2024, 1, 1), date(2024, 1, 31)
            )

        assert len(events) == 1
        e = events[0]
        assert e["event_type"] == "SPLIT"
        assert e["split_ratio_from"] == 2
        assert e["split_ratio_to"] == 1
        assert abs(e["event_factor"] - 0.5) < 0.001
        assert e["source_vendor"] == "yahoo"
        assert e["instrument_id"] == 1001

    def test_4_for_1_split_parsed_correctly(self):
        """yfinance returns ratio=4.0 for a 4-for-1 split (Apple 2020 style)."""
        provider = _make_provider({1001: "SPLIT4"})
        splits = _make_splits_series({"2024-03-01": 4.0})

        with patch("yfinance.Ticker", return_value=self._mock_ticker(splits=splits)):
            events = provider.get_corporate_actions(
                [1001], date(2024, 1, 1), date(2024, 12, 31)
            )

        assert len(events) == 1
        e = events[0]
        assert e["event_type"] == "SPLIT"
        assert e["split_ratio_from"] == 4
        assert e["split_ratio_to"] == 1
        assert abs(e["event_factor"] - 0.25) < 0.001

    def test_1_for_10_reverse_split_parsed_correctly(self):
        """yfinance returns ratio=0.1 for a 1-for-10 reverse split."""
        provider = _make_provider({1003: "RSPLIT"})
        splits = _make_splits_series({"2024-06-01": 0.1})

        with patch("yfinance.Ticker", return_value=self._mock_ticker(splits=splits)):
            events = provider.get_corporate_actions(
                [1003], date(2024, 1, 1), date(2024, 12, 31)
            )

        assert len(events) == 1
        e = events[0]
        assert e["event_type"] == "REVERSE_SPLIT"
        assert e["split_ratio_from"] == 10
        assert e["split_ratio_to"] == 1
        assert abs(e["event_factor"] - 10.0) < 0.1

    def test_multiple_splits_same_instrument(self):
        """Two splits for same instrument in range — both returned."""
        provider = _make_provider({1001: "MULTI"})
        splits = _make_splits_series({"2024-01-15": 2.0, "2024-06-15": 3.0})

        with patch("yfinance.Ticker", return_value=self._mock_ticker(splits=splits)):
            events = provider.get_corporate_actions(
                [1001], date(2024, 1, 1), date(2024, 12, 31)
            )

        assert len(events) == 2
        types = {e["event_type"] for e in events}
        assert types == {"SPLIT"}


# ---------------------------------------------------------------------------
# Dividend parsing
# ---------------------------------------------------------------------------

class TestDividendParsing:

    def _mock_ticker(self, splits=None, dividends=None):
        ticker = MagicMock()
        ticker.splits = splits if splits is not None else pd.Series(dtype=float)
        ticker.dividends = dividends if dividends is not None else pd.Series(dtype=float)
        return ticker

    def test_regular_dividend_parsed_correctly(self):
        provider = _make_provider({1002: "DIVD"})
        dividends = _make_dividends_series({"2024-03-15": 0.88})

        with patch("yfinance.Ticker", return_value=self._mock_ticker(dividends=dividends)):
            events = provider.get_corporate_actions(
                [1002], date(2024, 1, 1), date(2024, 12, 31)
            )

        assert len(events) == 1
        e = events[0]
        assert e["event_type"] == "DIVIDEND"
        assert abs(e["dividend_amount"] - 0.88) < 0.001
        assert e["dividend_currency"] == "USD"
        assert e["event_factor"] == 1.0    # dividends don't change the split factor
        assert e["split_ratio_from"] == 1
        assert e["split_ratio_to"] == 1

    def test_special_dividend_large_amount(self):
        """Special dividends can be much larger than regular ones."""
        provider = _make_provider({1002: "DIVD"})
        dividends = _make_dividends_series({"2024-12-15": 5.00})

        with patch("yfinance.Ticker", return_value=self._mock_ticker(dividends=dividends)):
            events = provider.get_corporate_actions(
                [1002], date(2024, 1, 1), date(2024, 12, 31)
            )

        assert len(events) == 1
        assert events[0]["event_type"] == "DIVIDEND"
        assert events[0]["dividend_amount"] == 5.00


# ---------------------------------------------------------------------------
# Date range filtering
# ---------------------------------------------------------------------------

class TestDateRangeFiltering:

    def _mock_ticker(self, splits):
        ticker = MagicMock()
        ticker.splits = splits
        ticker.dividends = pd.Series(dtype=float)
        return ticker

    def test_events_outside_range_excluded(self):
        """Split on 2023-12-15 is before from_date=2024-01-01 — must be excluded."""
        provider = _make_provider({1001: "SPLIT2"})
        splits = _make_splits_series({"2023-12-15": 2.0, "2024-03-01": 4.0})

        with patch("yfinance.Ticker", return_value=self._mock_ticker(splits)):
            events = provider.get_corporate_actions(
                [1001], date(2024, 1, 1), date(2024, 12, 31)
            )

        assert len(events) == 1
        assert events[0]["split_ratio_from"] == 4  # only the 4:1 in range

    def test_events_on_boundary_dates_included(self):
        """Events on exactly from_date and to_date must be included (inclusive)."""
        provider = _make_provider({1001: "SPLIT2"})
        splits = _make_splits_series({"2024-01-01": 2.0, "2024-12-31": 3.0})

        with patch("yfinance.Ticker", return_value=self._mock_ticker(splits)):
            events = provider.get_corporate_actions(
                [1001], date(2024, 1, 1), date(2024, 12, 31)
            )

        assert len(events) == 2

    def test_no_events_in_range_returns_empty(self):
        """No events in specified range — returns empty list, not error."""
        provider = _make_provider({1001: "SPLIT2"})
        splits = _make_splits_series({"2023-01-01": 2.0})

        with patch("yfinance.Ticker", return_value=self._mock_ticker(splits)):
            events = provider.get_corporate_actions(
                [1001], date(2024, 1, 1), date(2024, 12, 31)
            )

        assert events == []


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------

class TestErrorHandling:

    def test_unknown_instrument_id_skipped(self):
        """instrument_id=9999 not in symbol_map — skipped with warning, no crash."""
        provider = _make_provider({1001: "KNOWN"})
        events = provider.get_corporate_actions(
            [9999], date(2024, 1, 1), date(2024, 12, 31)
        )
        assert events == []

    def test_yfinance_exception_returns_empty(self):
        """yfinance API failure for one instrument — returns empty list, doesn't propagate."""
        provider = _make_provider({1001: "BROKEN"})

        with patch("yfinance.Ticker", side_effect=Exception("Connection refused")):
            events = provider.get_corporate_actions(
                [1001], date(2024, 1, 1), date(2024, 12, 31)
            )

        assert events == []

    def test_empty_symbol_map_returns_empty(self):
        """No instruments in symbol_map — nothing to fetch."""
        provider = YahooCorporateActionsProvider(symbol_map={})
        events = provider.get_bulk_corporate_actions(date(2024, 1, 1), date(2024, 12, 31))
        assert events == []

    def test_multiple_instruments_partial_failure(self):
        """First instrument fails, second succeeds — second events still returned."""
        provider = _make_provider({1001: "BROKEN", 1002: "GOOD"})

        call_count = [0]
        def ticker_side_effect(symbol):
            call_count[0] += 1
            if symbol == "BROKEN":
                raise RuntimeError("API error")
            ticker = MagicMock()
            ticker.splits = _make_splits_series({"2024-06-01": 2.0})
            ticker.dividends = pd.Series(dtype=float)
            return ticker

        with patch("yfinance.Ticker", side_effect=ticker_side_effect):
            events = provider.get_corporate_actions(
                [1001, 1002], date(2024, 1, 1), date(2024, 12, 31)
            )

        # instrument 1002 events returned despite 1001 failing
        assert len(events) == 1
        assert events[0]["instrument_id"] == 1002


# ---------------------------------------------------------------------------
# from_spark constructor
# ---------------------------------------------------------------------------

class TestFromSpark:

    def test_from_spark_builds_correct_symbol_map(self):
        """from_spark() must load symbol_map from instrument_listing correctly."""
        spark = MagicMock()
        df = MagicMock()
        rows = [
            MagicMock(**{"__getitem__": lambda self, k: {"instrument_id": 505, "symbol": "SPY"}[k]}),
            MagicMock(**{"__getitem__": lambda self, k: {"instrument_id": 506, "symbol": "QQQ"}[k]}),
        ]
        for r, data in zip(rows, [{"instrument_id": 505, "symbol": "SPY"}, {"instrument_id": 506, "symbol": "QQQ"}]):
            r.__getitem__ = lambda self, k, d=data: d[k]
        df.collect.return_value = rows
        spark.sql.return_value = df

        provider = YahooCorporateActionsProvider.from_spark(spark, catalog="tradeanalytics")
        assert provider._symbol_map.get(505) == "SPY"
        assert provider._symbol_map.get(506) == "QQQ"
