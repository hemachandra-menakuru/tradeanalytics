"""
TradeAnalytics IBKR Client Portal REST API Provider
=====================================================
Primary data provider for all phases. Uses IBKR Client Portal REST API —
no IB Gateway or TWS required for Phases 1-4.

IBKR Client Portal REST API:
  - Base URL: https://localhost:5000/v1/api (local gateway process)
  - Authentication: session-based (login once, session maintained)
  - Rate limits: ~10 requests/second
  - Historical data: up to 10 years for daily, varies for intraday

Capabilities:
  ✅ get_historical()    — full OHLCV with VWAP, pre/post market
  ✅ get_latest_quote()  — real-time bid/ask/last
  ✅ get_options_chain() — full chain with Greeks
  ✅ is_market_open()    — IBKR market hours API
  ✅ health_check()      — IBKR /tickle endpoint

Phase 1-4: Uses Client Portal REST API (this class)
Phase 5:   Switches to IB Gateway + ib_insync (separate provider)

Current status: STUB — full implementation in Phase 2 sprint 2
The stub returns empty data gracefully so the pipeline can be
developed and tested end-to-end using YahooProvider as fallback.
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timezone
from typing import List, Optional
from zoneinfo import ZoneInfo

from src.config.config_loader import ConfigNode
from src.ingestion.base.market_data_provider import (
    MarketDataProvider,
    ProviderConnectionError,
    ProviderAuthError,
    ProviderNotSupportedError,
)

logger = logging.getLogger(__name__)

_EASTERN = ZoneInfo("America/New_York")


class IBKRProvider(MarketDataProvider):
    """
    IBKR Client Portal REST API provider.
    Primary data source for all TradeAnalytics phases.

    Current status: STUB
    Full implementation: Phase 2 sprint 2 (after IBKR account setup verified)
    """

    def __init__(self, config: ConfigNode):
        super().__init__(config)
        self._base_url    = config.ibkr.base_url
        self._verify_ssl  = config.ibkr.verify_ssl
        self._timeout     = config.ibkr.timeout_seconds
        self._max_retries = config.ibkr.max_retries
        self._account_id  = config.ibkr.account_id
        self._session     = None  # requests.Session — initialised on first use

        logger.info(
            f"IBKRProvider initialised — "
            f"base_url={self._base_url}, "
            f"account_id={'set' if self._account_id else 'NOT SET'}"
        )

    # ── Provider identity ──────────────────────────────────────────────────────

    @property
    def provider_name(self) -> str:
        return "ibkr"

    @property
    def supports_options(self) -> bool:
        return True

    @property
    def supports_realtime(self) -> bool:
        return True

    @property
    def supported_intervals(self) -> List[str]:
        return ["1d", "1h", "4h", "5m", "1m"]

    # ── Core methods (STUB — returns empty, logs clearly) ──────────────────────

    def get_historical(
        self,
        symbol: str,
        start_date: date,
        end_date: date,
        interval: str = "1d",
    ) -> List[dict]:
        """
        STUB: Full implementation coming in Phase 2 sprint 2.

        Will call:
          GET /v1/api/iserver/marketdata/history
          params: conid, period, bar, outsideRth

        Returns empty list until implemented.
        """
        logger.warning(
            f"IBKRProvider.get_historical() is a STUB — "
            f"returning empty list for {symbol} {interval}. "
            f"Full implementation coming in Phase 2 sprint 2."
        )
        return []

    def get_latest_quote(self, symbol: str) -> Optional[dict]:
        """
        STUB: Full implementation coming in Phase 2 sprint 2.

        Will call:
          GET /v1/api/iserver/marketdata/snapshot
          params: conids, fields
        """
        logger.warning(
            f"IBKRProvider.get_latest_quote() is a STUB — "
            f"returning None for {symbol}."
        )
        return None

    def get_options_chain(self, symbol: str, expiry: date) -> List[dict]:
        """
        STUB: Full implementation coming in Phase 2 sprint 2.

        Will call:
          GET /v1/api/iserver/secdef/search (get conid)
          GET /v1/api/iserver/secdef/strikes (get strikes)
          GET /v1/api/trsrv/secdef (get full chain)
        """
        logger.warning(
            f"IBKRProvider.get_options_chain() is a STUB — "
            f"returning empty list for {symbol}."
        )
        return []

    def is_market_open(self) -> bool:
        """
        STUB: Will call GET /v1/api/iserver/marketdata/trading-schedule
        For now, computes from US Eastern time.
        """
        now_eastern = datetime.now(_EASTERN)
        if now_eastern.weekday() >= 5:
            return False
        open_time  = now_eastern.replace(hour=9,  minute=30, second=0, microsecond=0)
        close_time = now_eastern.replace(hour=16, minute=0,  second=0, microsecond=0)
        return open_time <= now_eastern < close_time

    def health_check(self) -> bool:
        """
        STUB: Will call GET /v1/api/tickle to keep session alive and verify auth.
        Returns False until IBKR Client Portal is configured.
        """
        logger.warning(
            "IBKRProvider.health_check() is a STUB — returning False. "
            "Configure IBKR Client Portal and implement fully in Phase 2 sprint 2."
        )
        return False

# Self-register with factory
from src.ingestion.factory.provider_factory import MarketDataFactory
MarketDataFactory.register('ibkr', IBKRProvider)
