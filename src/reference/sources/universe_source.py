"""
TradeAnalytics Universe Source — Abstract Base Class
=====================================================
Contract for any external source that provides instrument universe data.

Implementations:
  NasdaqFtpSource    → All US-listed securities from NASDAQ Trader FTP
  EtfHoldingsSource  → Index constituents from ETF holdings files (SPY/QQQ/IWM)

Both return RawInstrument objects. The UniverseSyncJob reconciles these
against the reference.instrument and reference.instrument_feed_config tables.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import List, Optional


@dataclass
class RawInstrument:
    """
    Instrument record as returned by an external universe source.
    Not yet enriched with FIGI/ISIN — that happens in the enrichment step.
    """
    symbol:         str
    company_name:   str
    asset_class:    str             # equity | etf
    exchange:       str             # NASDAQ | NYSE | NYSE ARCA | etc.
    exchange_mic:   Optional[str]   # ISO 10383 — filled by OpenFIGI
    currency:       str = "USD"
    isin:           Optional[str] = None    # filled by OpenFIGI
    figi:           Optional[str] = None    # filled by OpenFIGI
    ibkr_con_id:    Optional[int] = None    # filled by IBKR validation
    universe_codes: List[str] = field(default_factory=list)  # SP500, NDX100, etc.
    index_weight:   Optional[float] = None  # weight in source ETF (0.0–1.0)
    source_etf:     Optional[str] = None    # SPY | QQQ | IWM

    def __repr__(self) -> str:
        return (
            f"RawInstrument(symbol={self.symbol}, "
            f"asset_class={self.asset_class}, "
            f"exchange={self.exchange})"
        )


class UniverseSource(ABC):
    """
    Abstract base class for external universe data sources.

    Each implementation fetches from one specific source and returns
    a list of RawInstrument objects. No database writes — pure data fetch.
    """

    @abstractmethod
    def fetch(self) -> List[RawInstrument]:
        """
        Fetch the full instrument list from this source.

        Returns:
            List of RawInstrument objects. Not yet FIGI/ISIN enriched.

        Raises:
            ConnectionError: If the source is unreachable.
            ValueError: If the source returns unexpected data format.
        """

    @property
    @abstractmethod
    def source_name(self) -> str:
        """Human-readable name for logging (e.g. 'NASDAQ FTP', 'SPY Holdings')."""
