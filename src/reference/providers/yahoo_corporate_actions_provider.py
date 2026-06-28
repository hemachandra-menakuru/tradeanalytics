"""
YahooCorporateActionsProvider — CorporateActionsProvider backed by Yahoo Finance.

Role: seed and verify — NOT production ingestion.

Why Yahoo for corporate actions?
    Yahoo Finance exposes full corporate actions history (splits, dividends) via
    yfinance for free, with no API key, and supports bulk fetching via a single
    Ticker.history() call per symbol. This makes it ideal for:
    1. Initial seeding of reference.corporate_actions for all 8–2000 instruments.
    2. Cross-verification of IBKR-detected events (two-source confirmation).
    3. Unit tests (no gateway required).

Why NOT Yahoo for production ingestion?
    Yahoo is the unit test fallback only (CLAUDE.md rule, non-negotiable). Do not
    route daily incremental ingestion through Yahoo. Use IBKRCorporateActionsProvider
    for production; Yahoo only for seed/verify.

get_bulk_corporate_actions(): fetches splits + dividends for all symbols sequentially
    using yfinance Ticker.splits and Ticker.dividends. Not a single API call —
    yfinance makes one call per ticker. This is acceptable for one-time seed operations
    but not for daily 2000-instrument runs (would take 30+ minutes).
"""

from __future__ import annotations

import logging
from datetime import date, datetime
from typing import Dict, List, Optional

from src.shared.base.corporate_actions_provider import CorporateActionsProvider

logger = logging.getLogger(__name__)


class YahooCorporateActionsProvider(CorporateActionsProvider):
    """
    Yahoo Finance backed CorporateActionsProvider.

    Use for: seeding reference.corporate_actions, verifying IBKR detections.
    Do NOT use for: production daily ingestion.

    Args:
        symbol_map: {instrument_id: ticker_symbol} — e.g. {505: "SPY", 506: "QQQ"}.
                    Pass this in so no Spark dependency at construction time.
    """

    def __init__(self, symbol_map: Optional[Dict[int, str]] = None):
        self._symbol_map = symbol_map or {}

    @property
    def vendor_name(self) -> str:
        return "yahoo"

    def health_check(self) -> bool:
        """Attempt a minimal yfinance fetch to verify connectivity."""
        try:
            import yfinance as yf
            ticker = yf.Ticker("SPY")
            _ = ticker.splits
            return True
        except Exception as e:
            logger.error(f"YahooCorporateActionsProvider: health check failed — {e}")
            return False

    def get_corporate_actions(
        self,
        instrument_ids: List[int],
        from_date: date,
        to_date: date,
    ) -> List[Dict]:
        """Fetch splits and dividends for a specific list of instruments."""
        results = []
        for instrument_id in instrument_ids:
            symbol = self._symbol_map.get(instrument_id)
            if not symbol:
                logger.warning(
                    f"YahooCorporateActionsProvider: no symbol for instrument_id={instrument_id}"
                )
                continue
            events = self._fetch_for_symbol(instrument_id, symbol, from_date, to_date)
            results.extend(events)
        return results

    def get_bulk_corporate_actions(
        self,
        from_date: date,
        to_date: date,
    ) -> List[Dict]:
        """
        Fetch corporate actions for ALL known symbols.

        One yfinance call per symbol — acceptable for one-time seed, not for
        daily 2000-instrument production runs.
        """
        return self.get_corporate_actions(
            list(self._symbol_map.keys()), from_date, to_date
        )

    def _fetch_for_symbol(
        self,
        instrument_id: int,
        symbol: str,
        from_date: date,
        to_date: date,
    ) -> List[Dict]:
        """Fetch splits and dividends for a single symbol from Yahoo Finance."""
        try:
            import yfinance as yf
        except ImportError:
            logger.error("YahooCorporateActionsProvider: yfinance not installed")
            return []

        try:
            ticker = yf.Ticker(symbol)
            events = []

            # --- Splits ---
            splits = ticker.splits
            if splits is not None and not splits.empty:
                for idx, ratio in splits.items():
                    event_date = idx.date() if hasattr(idx, "date") else idx
                    if not (from_date <= event_date <= to_date):
                        continue
                    if ratio < 1.0:
                        # Reverse split: ratio is < 1 (e.g. 0.1 for 1-for-10)
                        ratio_from = round(1.0 / ratio)
                        ratio_to = 1
                        event_type = "REVERSE_SPLIT"
                        event_factor = float(ratio_from)  # multiply historical by ratio_from
                    else:
                        # Forward split: ratio is > 1 (e.g. 4.0 for 4-for-1)
                        ratio_from = round(ratio)
                        ratio_to = 1
                        event_type = "SPLIT"
                        event_factor = 1.0 / ratio_from
                    events.append({
                        "instrument_id":    instrument_id,
                        "effective_date":   event_date,
                        "event_type":       event_type,
                        "split_ratio_from": ratio_from,
                        "split_ratio_to":   ratio_to,
                        "event_factor":     event_factor,
                        "dividend_amount":  None,
                        "dividend_currency": None,
                        "event_detail":     f"{ratio_from}-for-{ratio_to} {event_type.lower().replace('_', ' ')}",
                        "source_vendor":    "yahoo",
                    })

            # --- Dividends ---
            dividends = ticker.dividends
            if dividends is not None and not dividends.empty:
                for idx, amount in dividends.items():
                    event_date = idx.date() if hasattr(idx, "date") else idx
                    if not (from_date <= event_date <= to_date):
                        continue
                    events.append({
                        "instrument_id":    instrument_id,
                        "effective_date":   event_date,
                        "event_type":       "DIVIDEND",
                        "split_ratio_from": 1,
                        "split_ratio_to":   1,
                        "event_factor":     1.0,
                        "dividend_amount":  float(amount),
                        "dividend_currency": "USD",
                        "event_detail":     f"${float(amount):.4f} per share",
                        "source_vendor":    "yahoo",
                    })

            logger.info(
                f"YahooCorporateActionsProvider: {symbol} ({instrument_id}): "
                f"{len(events)} events in {from_date}→{to_date}"
            )
            return events

        except Exception as e:
            logger.error(
                f"YahooCorporateActionsProvider: fetch failed for {symbol} "
                f"instrument_id={instrument_id} — {e}"
            )
            return []

    @classmethod
    def from_spark(
        cls,
        spark,
        catalog: str = "tradeanalytics",
    ) -> "YahooCorporateActionsProvider":
        """
        Build provider by loading symbol map from reference tables.

        Joins instrument_listing (symbol) with instrument (instrument_id).
        """
        rows = spark.sql(f"""
            SELECT il.instrument_id, il.symbol
            FROM {catalog}.reference.instrument_listing il
            WHERE il.is_current = true
        """).collect()

        symbol_map = {r["instrument_id"]: r["symbol"] for r in rows}
        logger.info(f"YahooCorporateActionsProvider: loaded {len(symbol_map)} symbols from Delta")
        return cls(symbol_map=symbol_map)
