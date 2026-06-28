"""
Tests for OpenFigiClient enrichment logic.

Scenarios:
  1. Batch size = 10 when no API key (free tier)
  2. Batch size = 100 when API key provided
  3. Successful enrichment fills figi, isin, exchange_mic on instrument
  4. Instrument not found in OpenFIGI → unchanged (no figi/isin), no crash
  5. Error response for one item in batch → that item unchanged, others enriched
  6. Rate limit (429) triggers sleep + retry
  7. Network error retries up to _RETRY_ATTEMPTS then returns None for all in batch
  8. Empty instrument list → no HTTP calls made
  9. Auth header set when API key provided, absent when not
"""

import pytest
from unittest.mock import patch, MagicMock, call

from src.reference.sources.openfigi_client import (
    OpenFigiClient,
    _BATCH_SIZE_NO_KEY,
    _BATCH_SIZE_WITH_KEY,
)
from src.reference.sources.universe_source import RawInstrument


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _instrument(symbol: str) -> RawInstrument:
    return RawInstrument(
        symbol=symbol, company_name=f"{symbol} Corp",
        asset_class="equity", exchange="NASDAQ", exchange_mic=None,
    )


def _ok_response(results: list) -> MagicMock:
    """Simulate a successful OpenFIGI API response."""
    mock = MagicMock()
    mock.status_code = 200
    mock.json.return_value = results
    mock.raise_for_status = MagicMock()
    return mock


def _rate_limit_response(retry_after: int = 5) -> MagicMock:
    mock = MagicMock()
    mock.status_code = 429
    mock.headers = {"Retry-After": str(retry_after)}
    return mock


def _error_response(status: int = 500) -> MagicMock:
    import requests
    mock = MagicMock()
    mock.status_code = status
    mock.raise_for_status.side_effect = requests.HTTPError(f"HTTP {status}")
    return mock


# ---------------------------------------------------------------------------
# Batch size selection
# ---------------------------------------------------------------------------

def test_batch_size_no_key():
    assert _BATCH_SIZE_NO_KEY == 10


def test_batch_size_with_key():
    assert _BATCH_SIZE_WITH_KEY == 100


def test_enrich_uses_10_per_batch_without_key():
    instruments = [_instrument(f"SYM{i}") for i in range(25)]
    figi_results = [[{"data": []}]] * 10  # each item → not found

    with patch.object(OpenFigiClient, "_query_batch", return_value={}) as mock_qb:
        OpenFigiClient().enrich(instruments)
        # 25 symbols / 10 per batch = 3 calls
        assert mock_qb.call_count == 3
        batch_sizes = [len(args[0]) for args, _ in mock_qb.call_args_list]
        assert batch_sizes == [10, 10, 5]


def test_enrich_uses_100_per_batch_with_key():
    instruments = [_instrument(f"SYM{i}") for i in range(150)]

    with patch.object(OpenFigiClient, "_query_batch", return_value={}) as mock_qb:
        OpenFigiClient(api_key="test-key").enrich(instruments)
        # 150 / 100 = 2 calls
        assert mock_qb.call_count == 2
        batch_sizes = [len(args[0]) for args, _ in mock_qb.call_args_list]
        assert batch_sizes == [100, 50]


# ---------------------------------------------------------------------------
# Successful enrichment
# ---------------------------------------------------------------------------

def test_enrich_fills_figi_isin_exchange_mic():
    inst = _instrument("AAPL")
    query_result = {
        "AAPL": {"figi": "BBG000B9XRY4", "isin": "US0378331005", "exchCode": "US"}
    }

    with patch.object(OpenFigiClient, "_query_batch", return_value=query_result):
        OpenFigiClient().enrich([inst])

    assert inst.figi == "BBG000B9XRY4"
    assert inst.isin == "US0378331005"
    assert inst.exchange_mic == "US"


def test_enrich_not_found_leaves_instrument_unchanged():
    inst = _instrument("UNKN")
    with patch.object(OpenFigiClient, "_query_batch", return_value={"UNKN": None}):
        OpenFigiClient().enrich([inst])

    assert inst.figi is None
    assert inst.isin is None


def test_enrich_partial_batch_some_found_some_not():
    aapl = _instrument("AAPL")
    unkn = _instrument("UNKN")
    query_result = {
        "AAPL": {"figi": "BBG000B9XRY4", "isin": "US0378331005", "exchCode": "US"},
        "UNKN": None,
    }

    with patch.object(OpenFigiClient, "_query_batch", return_value=query_result):
        OpenFigiClient().enrich([aapl, unkn])

    assert aapl.figi == "BBG000B9XRY4"
    assert unkn.figi is None


def test_enrich_empty_list_makes_no_calls():
    with patch.object(OpenFigiClient, "_query_batch") as mock_qb:
        OpenFigiClient().enrich([])
        mock_qb.assert_not_called()


# ---------------------------------------------------------------------------
# Auth header
# ---------------------------------------------------------------------------

def test_api_key_sets_auth_header():
    client = OpenFigiClient(api_key="my-secret-key")
    assert client._session.headers.get("X-OPENFIGI-APIKEY") == "my-secret-key"


def test_no_api_key_no_auth_header():
    client = OpenFigiClient()
    assert "X-OPENFIGI-APIKEY" not in client._session.headers


# ---------------------------------------------------------------------------
# _query_batch — rate limit + retry
# ---------------------------------------------------------------------------

def test_query_batch_retries_on_rate_limit():
    success_data = [{"data": [{"figi": "BBG000B9XRY4", "isin": "US0378331005", "exchCode": "US"}]}]

    client = OpenFigiClient()
    client._session = MagicMock()
    client._session.post.side_effect = [
        _rate_limit_response(retry_after=2),
        _ok_response(success_data),
    ]

    with patch("src.reference.sources.openfigi_client.time.sleep") as mock_sleep:
        result = client._query_batch(["AAPL"])

    assert "AAPL" in result
    assert result["AAPL"]["figi"] == "BBG000B9XRY4"
    mock_sleep.assert_called_once_with(2)


def test_query_batch_returns_none_after_all_retries_exhausted():
    import requests

    client = OpenFigiClient()
    client._session = MagicMock()
    client._session.post.side_effect = requests.RequestException("timeout")

    with patch("src.reference.sources.openfigi_client.time.sleep"):
        result = client._query_batch(["AAPL"])

    assert result == {"AAPL": None}


def test_query_batch_error_item_returns_none():
    data = [{"error": "No identifier found."}]

    client = OpenFigiClient()
    client._session = MagicMock()
    client._session.post.return_value = _ok_response(data)

    result = client._query_batch(["UNKN"])

    assert result["UNKN"] is None
