"""
TradeAnalytics Ticker Reader
=============================
Reads active tickers from src/reference/tickers.csv.
Provides filtered views — active only, by sector, by asset class.

The ticker list drives which symbols get ingested on every job run.
To add a new symbol: add a row to tickers.csv, set active=true.
To stop ingesting a symbol: set active=false (historical data preserved).

CSV columns:
  symbol, name, sector, asset_class, active, added_date,
  min_price, market_cap_tier, history_start, ipo_date, notes
"""

from __future__ import annotations

import csv
import logging
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import List, Optional

from src.shared.config.config_loader import ConfigNode, _find_repo_root

logger = logging.getLogger(__name__)


@dataclass
class TickerInfo:
    """
    Metadata for one ticker from ref_tickers.csv.
    Used by IngestionPlanner to determine per-ticker fetch range.
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

    def __repr__(self) -> str:
        return (
            f"TickerInfo(symbol={self.symbol}, "
            f"sector={self.sector}, "
            f"active={self.active}, "
            f"history_start={self.history_start})"
        )


class TickerReader:
    """
    Reads and filters tickers from src/reference/tickers.csv.

    Usage:
        reader = TickerReader(config)
        tickers = reader.get_active_tickers()
        for ticker in tickers:
            print(ticker.symbol, ticker.effective_history_start)
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
