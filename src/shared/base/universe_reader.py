"""
TradeAnalytics Universe Reader — Abstract Base Class
=====================================================
Contract for any instrument universe source.

Why this exists:
  Today universe data lives in src/reference/seed/tickers.csv.
  In Phase 2.5 it will move to the Delta table
  `tradeanalytics.reference.ticker_feed_config`.

  Without this ABC, BronzeIngestionJob imports TickerReader
  (the concrete CSV reader) directly. Swapping in the Delta-backed
  reader in Phase 2.5 would require changing the job's import.

  With this ABC, BronzeIngestionJob only knows about UniverseReader.
  Swapping implementations is a one-line change at the call site
  (or injected via DI) — the job itself never changes.

Implementations:
  CsvUniverseReader         → src/reference/managers/ticker_reader.py
                              (TickerReader is-a UniverseReader — backward compatible)
  DeltaUniverseReader       → src/reference/managers/delta_universe_reader.py  (Phase 2.5)

Usage:
    from src.shared.base.universe_reader import UniverseReader, InstrumentInfo

    def build_job(reader: UniverseReader, config):
        instruments = reader.get_active_instruments()
        for inst in instruments:
            print(inst.symbol, inst.history_start)
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import date
from typing import List, Optional


@dataclass
class InstrumentInfo:
    """
    Minimal instrument descriptor returned by any UniverseReader.

    Fields are the intersection of what the CSV seed has today and
    what ticker_feed_config will have in Phase 2.5.

    instrument_id is Optional[int]:
      - None when reading from CSV (Phase 2 / Phase 2.5 seed)
      - Populated integer when reading from Delta (Phase 2.5+)

    The IngestionPlanner only needs symbol, asset_class, and history_start.
    Additional fields are carried through for logging and DQ purposes.
    """
    symbol:          str
    name:            str
    asset_class:     str            # equity | etf | option | crypto | fx
    active:          bool
    history_start:   Optional[date] # per-instrument override for initial load
    instrument_id:   Optional[int] = None   # None until Phase 2.5 Delta tables
    exchange_mic:    Optional[str] = None   # ISO 10383 (XNAS, XNYS, etc.)
    currency:        Optional[str] = None   # ISO 4217
    sector:          Optional[str] = None
    market_cap_tier: Optional[str] = None   # large | mid | small
    ipo_date:        Optional[date] = None

    @property
    def effective_history_start(self) -> Optional[date]:
        """History start for initial load. Falls back to ipo_date, then None."""
        return self.history_start or self.ipo_date

    def __repr__(self) -> str:
        return (
            f"InstrumentInfo(symbol={self.symbol}, "
            f"asset_class={self.asset_class}, "
            f"active={self.active}, "
            f"instrument_id={self.instrument_id})"
        )


class UniverseReader(ABC):
    """
    Abstract base class for instrument universe sources.

    BronzeIngestionJob depends on this ABC only.
    All universe-reading implementations (CSV, Delta, API) implement this.
    """

    @abstractmethod
    def get_active_instruments(
        self,
        symbols: Optional[List[str]] = None,
        asset_classes: Optional[List[str]] = None,
    ) -> List[InstrumentInfo]:
        """
        Return active instruments, with optional filters.

        Args:
            symbols:       Restrict to specific symbols e.g. ["AAPL", "MSFT"]
            asset_classes: Restrict by asset class e.g. ["equity"]

        Returns:
            List of active InstrumentInfo objects matching all filters.
        """

    @abstractmethod
    def get_instrument(self, symbol: str) -> Optional[InstrumentInfo]:
        """
        Return info for a single symbol, or None if not found / inactive.

        Args:
            symbol: Ticker symbol e.g. "AAPL"

        Returns:
            InstrumentInfo if the symbol exists (active or not), else None.
        """

    @abstractmethod
    def get_symbols(self, active_only: bool = True) -> List[str]:
        """
        Return symbol strings only — lightweight version of get_active_instruments.

        Args:
            active_only: If True, return only active symbols.

        Returns:
            List of symbol strings.
        """
