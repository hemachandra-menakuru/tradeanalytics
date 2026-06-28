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
    "bar_date":     str,       # "YYYY-MM-DD" always
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

from abc import abstractmethod
from datetime import date
from typing import List, Optional

from src.shared.config.config_loader import ConfigNode
from src.shared.base.data_provider import (
    HistoricalDataProvider,
    RealtimeProvider,
    OptionsProvider,
)


class MarketDataProvider(HistoricalDataProvider, RealtimeProvider, OptionsProvider):
    """
    Composite ABC combining all three provider interfaces.

    Maintained for backward compatibility — existing providers (IBKR, Yahoo) inherit
    this class and continue to work unchanged. New providers that support only a subset
    of capabilities should inherit directly from HistoricalDataProvider, RealtimeProvider,
    or OptionsProvider in src/shared/base/data_provider.py.

    The factory accepts any HistoricalDataProvider subclass.
    """

    def __init__(self, config: ConfigNode):
        self._config = config

    # ── Abstract methods — declared here for documentation clarity ─────────────
    # (Already declared abstract in the constituent ABCs; repeated here for IDE support.)

    @abstractmethod
    def get_historical(
        self,
        symbol: str,
        start_date: date,
        end_date: date,
        interval: str = "1d",
    ) -> List[dict]: ...

    @abstractmethod
    def get_latest_quote(self, symbol: str) -> Optional[dict]: ...

    @abstractmethod
    def get_options_chain(self, symbol: str, expiry: date) -> List[dict]: ...

    @abstractmethod
    def is_market_open(self) -> bool: ...

    @abstractmethod
    def health_check(self) -> bool: ...

    @property
    @abstractmethod
    def provider_name(self) -> str: ...

    @property
    @abstractmethod
    def supports_options(self) -> bool: ...

    @property
    @abstractmethod
    def supports_realtime(self) -> bool: ...

    @property
    @abstractmethod
    def supported_intervals(self) -> List[str]: ...


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
