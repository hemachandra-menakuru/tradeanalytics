"""
Segregated data provider ABCs.

Three focused interfaces, each covering a distinct capability:
  - HistoricalDataProvider: OHLCV history (used by all Bronze ingestion)
  - RealtimeProvider:        live quotes and market status (Phase 5)
  - OptionsProvider:         options chains (Phase 4)

MarketDataProvider (in src/bronze/base/) is the composite that combines all three
for backward compatibility. New providers should inherit only the interface(s) they
actually support.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import date
from typing import List, Optional

from src.shared.config.config_loader import ConfigNode


class HistoricalDataProvider(ABC):
    """Contract for providers that supply OHLCV history."""

    def __init__(self, config: ConfigNode):
        self._config = config

    @abstractmethod
    def get_historical(
        self,
        symbol: str,
        start_date: date,
        end_date: date,
        interval: str = "1d",
    ) -> List[dict]:
        """Fetch historical OHLCV data. Returns [] on empty, raises on hard errors."""

    @abstractmethod
    def health_check(self) -> bool:
        """Return True if provider is reachable and authenticated."""

    @property
    @abstractmethod
    def provider_name(self) -> str:
        """Unique identifier matching config data.source."""

    @property
    @abstractmethod
    def supported_intervals(self) -> List[str]:
        """Intervals this provider supports, e.g. ['1d', '1h']."""

    @property
    @abstractmethod
    def supports_options(self) -> bool:
        """True if this provider also implements OptionsProvider."""

    @property
    @abstractmethod
    def supports_realtime(self) -> bool:
        """True if this provider also implements RealtimeProvider."""

    def supports_interval(self, interval: str) -> bool:
        return interval in self.supported_intervals

    def get_provider_info(self) -> dict:
        return {
            "provider_name":       self.provider_name,
            "supports_options":    self.supports_options,
            "supports_realtime":   self.supports_realtime,
            "supported_intervals": self.supported_intervals,
        }

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}("
            f"provider={self.provider_name}, "
            f"options={self.supports_options}, "
            f"realtime={self.supports_realtime})"
        )


class RealtimeProvider(ABC):
    """Contract for providers that supply live quotes and market status."""

    @abstractmethod
    def get_latest_quote(self, symbol: str) -> Optional[dict]:
        """Return latest quote dict or None if unavailable."""

    @abstractmethod
    def is_market_open(self) -> bool:
        """Return True if the US stock market is currently open."""


class OptionsProvider(ABC):
    """Contract for providers that supply options chains."""

    @abstractmethod
    def get_options_chain(self, symbol: str, expiry: date) -> List[dict]:
        """Return options contracts for symbol and expiry. [] if unsupported."""
