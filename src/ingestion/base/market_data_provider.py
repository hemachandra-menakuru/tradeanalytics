"""
TradeAnalytics Market Data Provider — Abstract Base Class
=========================================================
The contract every data provider MUST implement.
The ingestion layer ONLY ever calls these methods — never provider-specific code.

Design principles:
  - All providers return the same standard OHLCV dict format
  - Provider-specific API details are hidden inside each implementation
  - Switching provider = one line change in config/dev.yml (data.source)
  - If a provider cannot supply a field → return None, never fabricate
  - health_check() must be called before any data fetch in production

Standard OHLCV record format (all providers must return this):
  {
    # Identity
    "symbol":       str,       # e.g. "AAPL"
    "date":         str,       # "YYYY-MM-DD" always
    "interval":     str,       # "1d" | "1h" | "4h" | "5m" | "1m"
    "source":       str,       # provider name e.g. "ibkr"

    # Raw price (required — never None)
    "open":         float,
    "high":         float,
    "low":          float,
    "close":        float,
    "volume":       int,

    # Adjusted price (None if provider cannot supply)
    "adj_close":    float | None,
    "adj_factor":   float | None,

    # Session context (None if provider cannot supply)
    "prev_close":   float | None,
    "vwap":         float | None,
    "trade_count":  int | None,
    "exchange":     str | None,
    "currency":     str,          # default "USD"

    # Pre/post market (None if provider cannot supply)
    "pre_market_high":    float | None,
    "pre_market_low":     float | None,
    "pre_market_close":   float | None,
    "pre_market_volume":  int | None,
    "post_market_high":   float | None,
    "post_market_low":    float | None,
    "post_market_close":  float | None,
    "post_market_volume": int | None,

    # Corporate actions (None/0 if none on this date)
    "split_factor":       float | None,
    "dividend_amount":    float,       # 0.0 if no dividend
    "is_ex_dividend_date": bool,
    "is_ex_split_date":   bool,

    # Market conditions
    "trading_halt":  bool,    # default False
    "is_trading_day": bool,   # default True

    # Intraday only (None for daily)
    "bar_time_utc":     str | None,   # ISO UTC "2026-06-19T13:30:00Z"
    "bar_time_eastern": str | None,   # Eastern "2026-06-19T09:30:00"
    "bar_end_time_utc": str | None,
    "timezone_source":  str | None,

    # Pipeline audit (set by provider)
    "fetch_duration_ms": int | None,
    "data_as_of":        str | None,
    "source_version":    str | None,
  }
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import date
from typing import List, Optional

from src.config.config_loader import ConfigNode


class MarketDataProvider(ABC):
    """
    Abstract base class for all market data providers.

    Every provider (IBKR, Yahoo, Polygon, etc.) must:
      1. Inherit from this class
      2. Implement all abstract methods
      3. Register with MarketDataFactory

    The ingestion layer calls ONLY these methods — never
    provider-specific code directly.
    """

    def __init__(self, config: ConfigNode):
        self._config = config

    # ── Required abstract methods ──────────────────────────────────────────────

    @abstractmethod
    def get_historical(
        self,
        symbol: str,
        start_date: date,
        end_date: date,
        interval: str = "1d",
    ) -> List[dict]:
        """
        Fetch historical OHLCV data for a symbol.

        Args:
            symbol:     Ticker symbol e.g. "AAPL"
            start_date: Start date (inclusive)
            end_date:   End date (inclusive)
            interval:   Data interval — "1d" | "1h" | "4h" | "5m" | "1m"

        Returns:
            List of standard OHLCV dicts (see module docstring for format).
            Empty list if no data available.
            Never raises on empty data — raises only on connection/auth errors.

        Raises:
            ProviderConnectionError: if provider is unreachable
            ProviderAuthError:       if credentials are invalid
            ProviderDataError:       if provider returns malformed data
        """

    @abstractmethod
    def get_latest_quote(self, symbol: str) -> Optional[dict]:
        """
        Fetch the latest available quote for a symbol.

        Returns:
            Dict with at minimum: symbol, bid, ask, last, volume, timestamp
            None if quote unavailable (e.g. market closed, symbol not found)
        """

    @abstractmethod
    def get_options_chain(
        self,
        symbol: str,
        expiry: date,
    ) -> List[dict]:
        """
        Fetch options chain for a symbol and expiry date.

        Returns:
            List of option contract dicts.
            Empty list if provider doesn't support options or no data.
        """

    @abstractmethod
    def is_market_open(self) -> bool:
        """
        Returns True if the US stock market is currently open.
        Should use provider's market hours API if available,
        otherwise compute from US Eastern time (09:30-16:00 Mon-Fri).
        """

    @abstractmethod
    def health_check(self) -> bool:
        """
        Verify provider is reachable and credentials are valid.

        Returns:
            True if provider is healthy and ready to serve data.
            False if provider is unreachable or auth fails.
            Should never raise — catch all exceptions and return False.

        Usage:
            if not provider.health_check():
                logger.error("Provider unhealthy — switch to fallback")
                provider = factory.get_fallback_provider(config)
        """

    # ── Required properties ────────────────────────────────────────────────────

    @property
    @abstractmethod
    def provider_name(self) -> str:
        """
        Unique string identifier for this provider.
        Must match the value in config data.source.
        e.g. "ibkr" | "yahoo" | "polygon"
        """

    @property
    @abstractmethod
    def supports_options(self) -> bool:
        """True if this provider supports options chain data."""

    @property
    @abstractmethod
    def supports_realtime(self) -> bool:
        """True if this provider supports real-time quotes."""

    @property
    @abstractmethod
    def supported_intervals(self) -> List[str]:
        """
        List of interval strings this provider supports.
        e.g. ["1d", "1h", "5m"]
        Ingestion job uses this to validate interval before fetching.
        """

    # ── Concrete helper methods (shared by all providers) ─────────────────────

    def supports_interval(self, interval: str) -> bool:
        """Returns True if this provider supports the given interval."""
        return interval in self.supported_intervals

    def get_provider_info(self) -> dict:
        """Returns a summary dict of provider capabilities."""
        return {
            "provider_name":        self.provider_name,
            "supports_options":     self.supports_options,
            "supports_realtime":    self.supports_realtime,
            "supported_intervals":  self.supported_intervals,
        }

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}("
            f"provider={self.provider_name}, "
            f"options={self.supports_options}, "
            f"realtime={self.supports_realtime}"
            f")"
        )


# ── Provider exceptions ────────────────────────────────────────────────────────

class ProviderConnectionError(Exception):
    """Provider is unreachable — network issue or service down."""
    pass


class ProviderAuthError(Exception):
    """Provider credentials are invalid or expired."""
    pass


class ProviderDataError(Exception):
    """Provider returned malformed or unexpected data."""
    pass


class ProviderNotSupportedError(Exception):
    """Requested operation not supported by this provider."""
    pass
