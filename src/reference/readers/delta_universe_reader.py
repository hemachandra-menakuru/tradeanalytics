"""
DeltaUniverseReader — UniverseReader backed by reference Delta tables.

Replaces TickerReader (CSV) in Phase 2.5.
BronzeIngestionJob depends on UniverseReader ABC, so no job code changes.

Reads from three tables joined together:
  reference.ticker_feed_config  — desired state (is_active, target dates, batch_group)
  reference.instrument_listing  — symbol, exchange, currency (is_current=true only)
  reference.instrument          — asset_class

Returns InstrumentInfo with instrument_id populated — enables instrument_id-keyed
watermarks and removes all symbol-as-key fragility.

Modes:
  local — in-memory list injected at construction (unit tests, no Spark needed)
  spark — live Delta query via Databricks Connect or cluster
"""

from __future__ import annotations

import logging
from typing import Dict, List, Optional

from src.shared.base.universe_reader import InstrumentInfo, UniverseReader

logger = logging.getLogger(__name__)

_QUERY = """
    SELECT
        fc.instrument_id,
        fc.target_start_date  AS history_start,
        fc.batch_group,
        fc.priority,
        fc.max_lookback_days,
        il.symbol,
        il.company_name       AS name,
        il.exchange_mic,
        il.currency,
        i.asset_class
    FROM {catalog}.reference.ticker_feed_config fc
    JOIN {catalog}.reference.instrument_listing il
      ON fc.instrument_id = il.instrument_id
     AND il.is_current = true
    JOIN {catalog}.reference.instrument i
      ON fc.instrument_id = i.instrument_id
    WHERE fc.is_active = true
    {symbol_filter}
    ORDER BY fc.priority, il.symbol
"""


class DeltaUniverseReader(UniverseReader):
    """
    Delta-backed UniverseReader. Reads from reference.ticker_feed_config.

    Usage (local / tests):
        instruments = [InstrumentInfo(symbol="SPY", ...)]
        reader = DeltaUniverseReader(mode="local", instruments=instruments)

    Usage (Spark / Databricks):
        reader = DeltaUniverseReader(mode="spark", spark=spark, catalog="tradeanalytics")
        active = reader.get_active_instruments()
    """

    def __init__(
        self,
        mode: str = "local",
        spark=None,
        catalog: str = "tradeanalytics",
        instruments: Optional[List[InstrumentInfo]] = None,
    ):
        if mode == "spark" and spark is None:
            raise ValueError("SparkSession required for spark mode")
        if mode == "local" and instruments is None:
            instruments = []

        self._mode    = mode
        self._spark   = spark
        self._catalog = catalog

        # Local mode: index by symbol for O(1) lookup
        self._local: Dict[str, InstrumentInfo] = {
            inst.symbol.upper(): inst for inst in (instruments or [])
        }

        logger.info(f"DeltaUniverseReader initialised — mode={mode}, catalog={catalog}")

    # ------------------------------------------------------------------
    # UniverseReader ABC
    # ------------------------------------------------------------------

    def get_active_instruments(
        self,
        symbols: Optional[List[str]] = None,
        asset_classes: Optional[List[str]] = None,
    ) -> List[InstrumentInfo]:
        """Return all active instruments, optionally filtered by symbol or asset class."""
        if self._mode == "local":
            instruments = list(self._local.values())
            if symbols:
                upper = {s.upper() for s in symbols}
                instruments = [i for i in instruments if i.symbol.upper() in upper]
            if asset_classes:
                instruments = [i for i in instruments if i.asset_class in asset_classes]
            logger.info(f"DeltaUniverseReader (local): {len(instruments)} instruments")
            return instruments

        return self._spark_get_active(symbols=symbols, asset_classes=asset_classes)

    def get_instrument(self, symbol: str) -> Optional[InstrumentInfo]:
        """Return a single instrument by symbol, or None if not found."""
        symbol = symbol.upper()
        if self._mode == "local":
            return self._local.get(symbol)
        results = self._spark_get_active(symbols=[symbol])
        return results[0] if results else None

    def get_symbols(self, active_only: bool = True) -> List[str]:
        """Return symbol strings only."""
        instruments = self.get_active_instruments() if active_only else list(self._local.values())
        return [i.symbol for i in instruments]

    # ------------------------------------------------------------------
    # Spark internals
    # ------------------------------------------------------------------

    def _spark_get_active(
        self,
        symbols: Optional[List[str]] = None,
        asset_classes: Optional[List[str]] = None,
    ) -> List[InstrumentInfo]:
        """Execute the three-table JOIN and return InstrumentInfo list."""
        symbol_filter = ""
        if symbols:
            quoted = ", ".join(f"'{s.upper()}'" for s in symbols)
            symbol_filter = f"AND il.symbol IN ({quoted})"

        sql = _QUERY.format(catalog=self._catalog, symbol_filter=symbol_filter)

        try:
            rows = self._spark.sql(sql).collect()
        except Exception as e:
            logger.error(f"DeltaUniverseReader: query failed — {e}")
            raise

        instruments = [self._row_to_instrument(r.asDict()) for r in rows]

        if asset_classes:
            instruments = [i for i in instruments if i.asset_class in asset_classes]

        logger.info(
            f"DeltaUniverseReader: {len(instruments)} active instruments "
            f"(filters: symbols={symbols}, asset_classes={asset_classes})"
        )
        return instruments

    @staticmethod
    def _row_to_instrument(row: dict) -> InstrumentInfo:
        """Convert a Delta row dict to InstrumentInfo."""
        from datetime import date as _date

        history_start = row.get("history_start")
        if history_start is not None and not isinstance(history_start, _date):
            history_start = _date.fromisoformat(str(history_start)[:10])

        return InstrumentInfo(
            instrument_id   = row["instrument_id"],
            symbol          = row["symbol"],
            name            = row.get("name") or "",
            asset_class     = row.get("asset_class") or "equity",
            active          = True,        # query already filters is_active=true
            history_start   = history_start,
            exchange_mic    = row.get("exchange_mic"),
            currency        = row.get("currency"),
        )
