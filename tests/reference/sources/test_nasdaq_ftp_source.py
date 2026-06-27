"""
Tests for NasdaqFtpSource parsing logic.

These tests exercise the pure Python parsing functions (_parse_nasdaq_listed,
_parse_other_listed) and the exclusion filter (_is_excluded) with no network
calls and no Spark. HTTP is mocked via unittest.mock.patch.

Test scenarios:
  1. Normal equity parsed correctly from nasdaqlisted.txt
  2. ETF parsed correctly (ETF column = 'Y')
  3. Test issue excluded (Test Issue column = 'Y')
  4. Excluded suffix instruments skipped (warrants, units, preferred, etc.)
  5. Symbol ending in '$' excluded
  6. NASDAQ and otherlisted symbols deduplicated (NASDAQ takes precedence)
  7. Exchange code mapped to correct MIC in otherlisted.txt
  8. Unknown exchange code results in None MIC (not a crash)
  9. HTTP error raises an exception (not swallowed)
 10. source_name property returns a non-empty string
"""

import pytest
from unittest.mock import patch, MagicMock

from src.reference.sources.nasdaq_ftp_source import (
    NasdaqFtpSource,
    _parse_nasdaq_listed,
    _parse_other_listed,
    _is_excluded,
)


# ---------------------------------------------------------------------------
# Fixtures — minimal pipe-delimited content matching each file's header
# ---------------------------------------------------------------------------

NASDAQ_LISTED_HEADER = (
    "Symbol|Security Name|Market Category|Test Issue|"
    "Financial Status|Round Lot Size|ETF|NextShares\n"
)
OTHER_LISTED_HEADER = (
    "ACT Symbol|Security Name|Exchange|CQS Symbol|"
    "ETF|Round Lot Size|Test Issue|NASDAQ Symbol\n"
)
FILE_CREATION_TRAILER = "File Creation Time: 20260628\n"


def _nasdaq_row(symbol, name, test_issue="N", etf="N"):
    return f"{symbol}|{name}|Q|{test_issue}|N|100|{etf}|N\n"


def _other_row(symbol, name, exchange="N", test_issue="N", etf="N"):
    return f"{symbol}|{name}|{exchange}|{symbol}|{etf}|100|{test_issue}|{symbol}\n"


# ---------------------------------------------------------------------------
# _is_excluded
# ---------------------------------------------------------------------------

def test_is_excluded_normal_equity_not_excluded():
    assert _is_excluded("Apple Inc.", "AAPL") is False


def test_is_excluded_warrant_excluded():
    assert _is_excluded("XYZ Corp Warrant", "XYZW") is True


def test_is_excluded_preferred_excluded():
    assert _is_excluded("Bank of X Preferred", "BXP") is True


def test_is_excluded_dollar_suffix_excluded():
    assert _is_excluded("Test Corp", "TEST$") is True


def test_is_excluded_unit_excluded():
    assert _is_excluded("SPAC Units", "XYZUU") is True


def test_is_excluded_etf_name_not_excluded():
    # ETF names don't contain excluded suffixes
    assert _is_excluded("SPDR S&P 500 ETF Trust", "SPY") is False


# ---------------------------------------------------------------------------
# _parse_nasdaq_listed
# ---------------------------------------------------------------------------

def test_parse_nasdaq_equity():
    text = NASDAQ_LISTED_HEADER + _nasdaq_row("AAPL", "Apple Inc.") + FILE_CREATION_TRAILER
    result = _parse_nasdaq_listed(text)
    assert len(result) == 1
    assert result[0].symbol == "AAPL"
    assert result[0].asset_class == "equity"
    assert result[0].exchange_mic == "XNAS"


def test_parse_nasdaq_etf():
    text = NASDAQ_LISTED_HEADER + _nasdaq_row("SPY", "SPDR S&P 500 ETF", etf="Y") + FILE_CREATION_TRAILER
    result = _parse_nasdaq_listed(text)
    assert len(result) == 1
    assert result[0].asset_class == "etf"


def test_parse_nasdaq_test_issue_excluded():
    text = NASDAQ_LISTED_HEADER + _nasdaq_row("ZTEST", "Test Corp", test_issue="Y") + FILE_CREATION_TRAILER
    result = _parse_nasdaq_listed(text)
    assert len(result) == 0


def test_parse_nasdaq_warrant_excluded():
    text = NASDAQ_LISTED_HEADER + _nasdaq_row("XYZW", "XYZ Corp Warrant") + FILE_CREATION_TRAILER
    result = _parse_nasdaq_listed(text)
    assert len(result) == 0


def test_parse_nasdaq_skips_trailer_line():
    text = NASDAQ_LISTED_HEADER + _nasdaq_row("AAPL", "Apple Inc.") + FILE_CREATION_TRAILER
    result = _parse_nasdaq_listed(text)
    # Trailer must not be parsed as an instrument
    assert all(r.symbol != "File Creation Time: 20260628" for r in result)


def test_parse_nasdaq_multiple_rows():
    text = (
        NASDAQ_LISTED_HEADER
        + _nasdaq_row("AAPL", "Apple Inc.")
        + _nasdaq_row("MSFT", "Microsoft Corp")
        + _nasdaq_row("SPY",  "SPDR S&P 500 ETF", etf="Y")
        + FILE_CREATION_TRAILER
    )
    result = _parse_nasdaq_listed(text)
    assert len(result) == 3
    symbols = {r.symbol for r in result}
    assert symbols == {"AAPL", "MSFT", "SPY"}


# ---------------------------------------------------------------------------
# _parse_other_listed
# ---------------------------------------------------------------------------

def test_parse_other_nyse_equity():
    text = OTHER_LISTED_HEADER + _other_row("JPM", "JPMorgan Chase", exchange="N") + FILE_CREATION_TRAILER
    result = _parse_other_listed(text)
    assert len(result) == 1
    assert result[0].symbol == "JPM"
    assert result[0].exchange_mic == "XNYS"
    assert result[0].asset_class == "equity"


def test_parse_other_arcx_etf():
    text = OTHER_LISTED_HEADER + _other_row("SPY", "SPDR S&P 500 ETF", exchange="P", etf="Y") + FILE_CREATION_TRAILER
    result = _parse_other_listed(text)
    assert result[0].exchange_mic == "ARCX"
    assert result[0].asset_class == "etf"


def test_parse_other_unknown_exchange_mic_is_none():
    text = OTHER_LISTED_HEADER + _other_row("XYZ", "XYZ Corp", exchange="X") + FILE_CREATION_TRAILER
    result = _parse_other_listed(text)
    assert len(result) == 1
    assert result[0].exchange_mic is None  # unknown exchange code → None, not a crash


def test_parse_other_test_issue_excluded():
    text = OTHER_LISTED_HEADER + _other_row("ZTEST", "Test", test_issue="Y") + FILE_CREATION_TRAILER
    result = _parse_other_listed(text)
    assert len(result) == 0


# ---------------------------------------------------------------------------
# NasdaqFtpSource.fetch() — deduplication + HTTP mocking
# ---------------------------------------------------------------------------

def _make_mock_response(text: str) -> MagicMock:
    mock = MagicMock()
    mock.text = text
    mock.raise_for_status = MagicMock()
    return mock


def test_fetch_deduplicates_nasdaq_takes_precedence():
    # AAPL appears in both files — NASDAQ listing must win
    nasdaq_text = NASDAQ_LISTED_HEADER + _nasdaq_row("AAPL", "Apple Inc. NASDAQ") + FILE_CREATION_TRAILER
    other_text  = OTHER_LISTED_HEADER  + _other_row("AAPL", "Apple Inc. NYSE", exchange="N") + FILE_CREATION_TRAILER

    with patch("src.reference.sources.nasdaq_ftp_source.requests.get") as mock_get:
        mock_get.side_effect = [
            _make_mock_response(nasdaq_text),
            _make_mock_response(other_text),
        ]
        source = NasdaqFtpSource()
        result = source.fetch()

    aapl_rows = [r for r in result if r.symbol == "AAPL"]
    assert len(aapl_rows) == 1
    assert aapl_rows[0].company_name == "Apple Inc. NASDAQ"


def test_fetch_includes_other_only_symbols():
    nasdaq_text = NASDAQ_LISTED_HEADER + FILE_CREATION_TRAILER
    other_text  = OTHER_LISTED_HEADER + _other_row("JPM", "JPMorgan", exchange="N") + FILE_CREATION_TRAILER

    with patch("src.reference.sources.nasdaq_ftp_source.requests.get") as mock_get:
        mock_get.side_effect = [
            _make_mock_response(nasdaq_text),
            _make_mock_response(other_text),
        ]
        result = NasdaqFtpSource().fetch()

    assert any(r.symbol == "JPM" for r in result)


def test_fetch_raises_on_http_error():
    import requests as req
    with patch("src.reference.sources.nasdaq_ftp_source.requests.get") as mock_get:
        mock_get.return_value.raise_for_status.side_effect = req.HTTPError("404")
        with pytest.raises(req.HTTPError):
            NasdaqFtpSource().fetch()


def test_source_name_non_empty():
    assert NasdaqFtpSource().source_name != ""
