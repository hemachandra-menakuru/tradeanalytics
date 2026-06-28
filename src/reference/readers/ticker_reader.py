"""
TradeAnalytics Ticker Reader
=============================
CSV-backed implementation of UniverseReader.
Reads active tickers from src/reference/seed/tickers.csv.

Phase 2.5 migration path:
  This class will be replaced by DeltaUniverseReader, which reads from
  tradeanalytics.reference.ticker_feed_config.
  BronzeIngestionJob depends on the UniverseReader ABC, so the swap
  requires zero changes to the job — only the construction call site changes.

CSV columns:
  symbol, name, sector, asset_class, active, added_date,
  min_price, market_cap_tier, history_start, ipo_date, notes

To add a new symbol: add a row to tickers.csv, set active=true.
To stop ingesting a symbol: set active=false (historical data preserved).
"""

from __future__ import annotations

import csv
import logging
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import List, Optional

from src.shared.config.config_loader import ConfigNode, _find_repo_root
from src.shared.base.universe_reader import UniverseReader, InstrumentInfo

logger = logging.getLogger(__name__)


@dataclass
class TickerInfo:
    """
    Internal CSV row model. Carries CSV-specific fields (min_price, notes, etc.)
    that are not part of InstrumentInfo.

    TickerReader.to_instrument_info() converts TickerInfo → InstrumentInfo
    for the UniverseReader interface.
    """
    symbol:           str
    name:             str
    sector:           str
    asset_class:      str           # equity | etf | option
    active:           bool
    added_date:       Optional[date]
    min_price:        float
    market_cap_tier:  str           # large | mid | small
    history_start:    Optional[date] # per-ticker override for initial load
    ipo_date:         Optional[date]
    notes:            str = ""

    @property
    def effective_history_start(self) -> Optional[date]:
        """
        The date to use as history_start for initial load.
        Uses history_start if set, falls back to ipo_date, then None (use global).
        """
        return self.history_start or self.ipo_date

    def to_instrument_info(self) -> InstrumentInfo:
        """Convert to the shared InstrumentInfo model."""
        return InstrumentInfo(
            symbol          = self.symbol,
            name            = self.name,
            asset_class     = self.asset_class,
            active          = self.active,
            history_start   = self.history_start,
            instrument_id   = None,        # not available from CSV
            exchange_mic    = None,        # not available from CSV
            currency        = None,        # not available from CSV
            sector          = self.sector,
            market_cap_tier = self.market_cap_tier,
            ipo_date        = self.ipo_date,
        )

    def __repr__(self) -> str:
        return (
            f"TickerInfo(symbol={self.symbol}, "
            f"sector={self.sector}, "
            f"active={self.active}, "
            f"history_start={self.history_start})"
        )


class TickerReader(UniverseReader):
    """
    CSV-backed UniverseReader. Reads from src/reference/seed/tickers.csv.

    Usage:
        reader = TickerReader(config)

        # UniverseReader interface (used by BronzeIngestionJob):
        instruments = reader.get_active_instruments()

        # Legacy TickerInfo interface (used by tests and internal code):
        tickers = reader.get_active_tickers()
    """

    def __init__(
        self,
        config: ConfigNode,
        tickers_path: Optional[Path] = None,
    ):
        if tickers_path is None:
            repo_root    = _find_repo_root()
            tickers_path = repo_root / "src" / "reference" / "tickers.csv"

        if not tickers_path.exists():
            raise FileNotFoundError(
                f"Tickers file not found: {tickers_path}. "
                f"Expected at src/reference/tickers.csv"
            )

        self._path    = tickers_path
        self._config  = config
        self._tickers: Optional[List[TickerInfo]] = None

    def _load(self) -> List[TickerInfo]:
        """Load and parse tickers.csv. Cached after first call."""
        if self._tickers is not None:
            return self._tickers

        tickers = []
        with open(self._path, newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                try:
                    ticker = self._parse_row(row)
                    tickers.append(ticker)
                except Exception as e:
                    logger.warning(
                        f"Skipping malformed ticker row "
                        f"symbol={row.get('symbol', '?')}: {e}"
                    )

        self._tickers = tickers
        logger.info(
            f"TickerReader: loaded {len(tickers)} tickers "
            f"({sum(1 for t in tickers if t.active)} active) "
            f"from {self._path.name}"
        )
        return tickers

    def _parse_row(self, row: dict) -> TickerInfo:
        """Parse one CSV row into a TickerInfo."""
        return TickerInfo(
            symbol          = row["symbol"].strip().upper(),
            name            = row["name"].strip(),
            sector          = row["sector"].strip(),
            asset_class     = row["asset_class"].strip(),
            active          = row["active"].strip().lower() == "true",
            added_date      = self._parse_date(row.get("added_date")),
            min_price       = float(row.get("min_price", 5.0)),
            market_cap_tier = row.get("market_cap_tier", "large").strip(),
            history_start   = self._parse_date(row.get("history_start")),
            ipo_date        = self._parse_date(row.get("ipo_date")),
            notes           = row.get("notes", "").strip(),
        )

    def _parse_date(self, value: Optional[str]) -> Optional[date]:
        """Parse a date string — returns None if empty or invalid."""
        if not value or not value.strip():
            return None
        try:
            return date.fromisoformat(value.strip())
        except ValueError:
            return None

    def get_all_tickers(self) -> List[TickerInfo]:
        """Return all tickers including inactive ones."""
        return self._load()

    def get_active_tickers(
        self,
        symbols: Optional[List[str]] = None,
        asset_classes: Optional[List[str]] = None,
        sectors: Optional[List[str]] = None,
    ) -> List[TickerInfo]:
        """
        Return active tickers with optional filters.

        Args:
            symbols:       Filter to specific symbols e.g. ["AAPL", "MSFT"]
            asset_classes: Filter by asset class e.g. ["equity"]
            sectors:       Filter by sector e.g. ["Technology"]

        Returns:
            List of active TickerInfo matching all filters
        """
        tickers = [t for t in self._load() if t.active]

        if symbols:
            symbols_upper = [s.upper() for s in symbols]
            tickers = [t for t in tickers if t.symbol in symbols_upper]

        if asset_classes:
            tickers = [t for t in tickers if t.asset_class in asset_classes]

        if sectors:
            tickers = [t for t in tickers if t.sector in sectors]

        logger.info(
            f"TickerReader: {len(tickers)} active tickers "
            f"(filters: symbols={symbols}, "
            f"asset_classes={asset_classes}, sectors={sectors})"
        )
        return tickers

    def get_ticker(self, symbol: str) -> Optional[TickerInfo]:
        """Return info for a specific symbol, or None if not found."""
        symbol = symbol.upper()
        for ticker in self._load():
            if ticker.symbol == symbol:
                return ticker
        return None

    def get_symbols(self, active_only: bool = True) -> List[str]:
        """Return just the symbol strings."""
        tickers = self._load()
        if active_only:
            tickers = [t for t in tickers if t.active]
        return [t.symbol for t in tickers]

    @property
    def ticker_count(self) -> int:
        return len(self._load())

    @property
    def active_count(self) -> int:
        return sum(1 for t in self._load() if t.active)

    # ── UniverseReader interface ───────────────────────────────────────────────

    def get_active_instruments(
        self,
        symbols: Optional[List[str]] = None,
        asset_classes: Optional[List[str]] = None,
    ) -> List[InstrumentInfo]:
        """
        UniverseReader implementation — returns InstrumentInfo objects.
        Delegates to get_active_tickers() then converts to InstrumentInfo.
        """
        tickers = self.get_active_tickers(
            symbols=symbols,
            asset_classes=asset_classes,
        )
        return [t.to_instrument_info() for t in tickers]

    def get_instrument(self, symbol: str) -> Optional[InstrumentInfo]:
        """UniverseReader implementation."""
        ticker = self.get_ticker(symbol)
        return ticker.to_instrument_info() if ticker is not None else None

    def get_symbols(self, active_only: bool = True) -> List[str]:
        """UniverseReader implementation."""
        tickers = self._load()
        if active_only:
            tickers = [t for t in tickers if t.active]
        return [t.symbol for t in tickers]
