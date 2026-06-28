"""
Unit tests for IBKRCorporateActionsProvider.

Tests cover:
  - vendor_name
  - health_check: authenticated, unauthenticated, connection failure
  - get_corporate_actions: per-instrument call with unknown/known conids
  - get_bulk_corporate_actions: warning logged when called with large map
  - from_spark: conid map loaded correctly from instrument_vendor_id
  - API failure handling: exception caught, empty list returned
"""

import pytest
from unittest.mock import MagicMock, patch
from datetime import date

from src.reference.providers.ibkr_corporate_actions_provider import IBKRCorporateActionsProvider


def _make_provider(conid_map=None):
    return IBKRCorporateActionsProvider(
        base_url="https://localhost:5055/v1/api",
        conid_map=conid_map or {505: "265598", 506: "320227"},
        verify_ssl=False,
    )


# ---------------------------------------------------------------------------
# vendor_name
# ---------------------------------------------------------------------------

def test_vendor_name():
    assert _make_provider().vendor_name == "ibkr"


# ---------------------------------------------------------------------------
# health_check
# ---------------------------------------------------------------------------

class TestHealthCheck:

    def test_health_check_authenticated_returns_true(self):
        provider = _make_provider()
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"authenticated": True}

        with patch("requests.get", return_value=mock_resp):
            assert provider.health_check() is True

    def test_health_check_not_authenticated_returns_false(self):
        provider = _make_provider()
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"authenticated": False}

        with patch("requests.get", return_value=mock_resp):
            assert provider.health_check() is False

    def test_health_check_connection_error_returns_false(self):
        """Gateway not reachable — must return False, not raise."""
        provider = _make_provider()
        with patch("requests.get", side_effect=ConnectionRefusedError("gateway down")):
            assert provider.health_check() is False

    def test_health_check_timeout_returns_false(self):
        import requests as req
        provider = _make_provider()
        with patch("requests.get", side_effect=req.exceptions.Timeout("timeout")):
            assert provider.health_check() is False

    def test_health_check_malformed_response_returns_false(self):
        """JSON response missing 'authenticated' key — defaults to False."""
        provider = _make_provider()
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"status": "ok"}  # no 'authenticated' key

        with patch("requests.get", return_value=mock_resp):
            assert provider.health_check() is False


# ---------------------------------------------------------------------------
# get_corporate_actions — conid resolution
# ---------------------------------------------------------------------------

class TestGetCorporateActions:

    def test_unknown_instrument_id_skipped(self):
        """instrument_id not in conid_map — skipped with warning, no crash."""
        provider = _make_provider({505: "265598"})
        events = provider.get_corporate_actions(
            [9999], date(2024, 1, 1), date(2024, 12, 31)
        )
        assert events == []

    def test_known_instrument_calls_fetch(self):
        """Known instrument_id — _fetch_events_for_conid is called once."""
        provider = _make_provider({505: "265598"})

        with patch.object(provider, "_fetch_events_for_conid", return_value=[]) as mock_fetch:
            provider.get_corporate_actions([505], date(2024, 1, 1), date(2024, 12, 31))

        mock_fetch.assert_called_once_with(505, "265598", date(2024, 1, 1), date(2024, 12, 31))

    def test_multiple_instruments_each_called_once(self):
        """Two instruments — _fetch_events_for_conid called twice."""
        provider = _make_provider({505: "265598", 506: "320227"})

        with patch.object(provider, "_fetch_events_for_conid", return_value=[]) as mock_fetch:
            provider.get_corporate_actions(
                [505, 506], date(2024, 1, 1), date(2024, 12, 31)
            )

        assert mock_fetch.call_count == 2

    def test_partial_conid_map_processes_known_only(self):
        """3 requested, only 2 in conid_map — only 2 API calls made."""
        provider = _make_provider({505: "265598", 506: "320227"})

        with patch.object(provider, "_fetch_events_for_conid", return_value=[]) as mock_fetch:
            provider.get_corporate_actions(
                [505, 506, 9999], date(2024, 1, 1), date(2024, 12, 31)
            )

        assert mock_fetch.call_count == 2

    def test_fetch_exception_returns_empty_not_raise(self):
        """_fetch_events_for_conid raises exception — caught, empty list returned."""
        provider = _make_provider({505: "265598"})

        with patch.object(provider, "_fetch_events_for_conid",
                          side_effect=RuntimeError("IBKR API error")):
            events = provider.get_corporate_actions(
                [505], date(2024, 1, 1), date(2024, 12, 31)
            )

        assert events == []

    def test_empty_instrument_list_returns_empty(self):
        provider = _make_provider()
        events = provider.get_corporate_actions([], date(2024, 1, 1), date(2024, 12, 31))
        assert events == []


# ---------------------------------------------------------------------------
# get_bulk_corporate_actions — warns when called with large map
# ---------------------------------------------------------------------------

class TestGetBulkCorporateActions:

    def test_small_map_no_warning(self, caplog):
        """10 instruments — no warning logged."""
        import logging
        conid_map = {i: str(i) for i in range(10)}
        provider = _make_provider(conid_map)

        with patch.object(provider, "_fetch_events_for_conid", return_value=[]):
            with caplog.at_level(logging.WARNING):
                provider.get_bulk_corporate_actions(date(2024, 1, 1), date(2024, 12, 31))

        assert "pre-filter" not in caplog.text.lower()

    def test_large_map_logs_warning(self, caplog):
        """51 instruments — warning must be logged about scale risk."""
        import logging
        conid_map = {i: str(i) for i in range(51)}
        provider = _make_provider(conid_map)

        with patch.object(provider, "_fetch_events_for_conid", return_value=[]):
            with caplog.at_level(logging.WARNING):
                provider.get_bulk_corporate_actions(date(2024, 1, 1), date(2024, 12, 31))

        assert "pre-filter" in caplog.text.lower() or "51" in caplog.text


# ---------------------------------------------------------------------------
# from_spark constructor
# ---------------------------------------------------------------------------

class TestFromSpark:

    def test_from_spark_builds_conid_map(self):
        """from_spark loads conid_map from instrument_vendor_id table."""
        spark = MagicMock()
        df = MagicMock()

        row_spy = MagicMock()
        row_spy.__getitem__ = lambda self, k: {"instrument_id": 505, "vendor_instrument_id": "265598"}[k]
        row_qqq = MagicMock()
        row_qqq.__getitem__ = lambda self, k: {"instrument_id": 506, "vendor_instrument_id": "320227"}[k]

        df.collect.return_value = [row_spy, row_qqq]
        spark.sql.return_value = df

        provider = IBKRCorporateActionsProvider.from_spark(spark, catalog="tradeanalytics")
        assert provider._conid_map.get(505) == "265598"
        assert provider._conid_map.get(506) == "320227"

    def test_from_spark_empty_table_gives_empty_map(self):
        """Empty instrument_vendor_id table — conid_map is empty dict."""
        spark = MagicMock()
        df = MagicMock()
        df.collect.return_value = []
        spark.sql.return_value = df

        provider = IBKRCorporateActionsProvider.from_spark(spark, catalog="tradeanalytics")
        assert provider._conid_map == {}
