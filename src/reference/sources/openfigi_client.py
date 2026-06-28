"""
TradeAnalytics OpenFIGI Enrichment Client
==========================================
Maps ticker symbols to FIGI + ISIN using the Bloomberg OpenFIGI API.

OpenFIGI is Bloomberg's free, public instrument identification API.
No API key required for up to 250 requests/minute.
With an API key (free registration): 25,000 requests/day.

Why FIGI + ISIN?
  Symbol alone is fragile — FB became META, GOOGL has two share classes.
  FIGI (Financial Instrument Global Identifier) is a stable, permanent,
  globally unique identifier assigned per instrument per exchange.
  ISIN (ISO 6166) is the international cross-source join key.

API docs: https://www.openfigi.com/api

Usage:
    client = OpenFigiClient()
    enriched = client.enrich(instruments)
    # instruments with figi, isin, exchange_mic populated where found
"""

from __future__ import annotations

import logging
import time
from typing import Dict, List, Optional

import requests

from src.reference.sources.universe_source import RawInstrument

logger = logging.getLogger(__name__)

_OPENFIGI_URL   = "https://api.openfigi.com/v3/mapping"
_BATCH_SIZE_NO_KEY  = 10    # OpenFIGI limit without API key
_BATCH_SIZE_WITH_KEY = 100  # OpenFIGI limit with API key
_RATE_LIMIT_RPS = 4         # 250/min without key → ~4/sec conservatively
_RETRY_ATTEMPTS = 3
_RETRY_DELAY    = 5         # seconds


class OpenFigiClient:
    """
    Enriches RawInstrument objects with FIGI + ISIN from OpenFIGI API.

    Processes in batches of 10 (no API key) or 100 (with API key).
    Rate-limited to stay within free tier (250 req/min).
    Instruments not found in OpenFIGI are returned unchanged (no FIGI/ISIN).
    """

    def __init__(self, api_key: Optional[str] = None):
        """
        Args:
            api_key: Optional OpenFIGI API key. Without key: 250 req/min.
                     With key (free registration): 25,000 req/day.
                     Set via env var OPENFIGI_API_KEY.
        """
        self._api_key = api_key
        self._session = requests.Session()
        if api_key:
            self._session.headers["X-OPENFIGI-APIKEY"] = api_key

    def enrich(self, instruments: List[RawInstrument]) -> List[RawInstrument]:
        """
        Enrich a list of instruments with FIGI + ISIN.

        Modifies instruments in-place (figi, isin, exchange_mic fields).
        Returns the same list for chaining.

        Args:
            instruments: List of RawInstrument objects to enrich.

        Returns:
            Same list with figi/isin/exchange_mic populated where found.
        """
        batch_size = _BATCH_SIZE_WITH_KEY if self._api_key else _BATCH_SIZE_NO_KEY
        logger.info(
            f"OpenFigiClient: enriching {len(instruments)} instruments "
            f"(batches of {batch_size}, {'with' if self._api_key else 'no'} API key)"
        )

        # Build lookup: symbol → instrument (for updating in-place)
        symbol_map: Dict[str, RawInstrument] = {i.symbol: i for i in instruments}

        # Process in batches
        symbols   = list(symbol_map.keys())
        total     = len(symbols)
        enriched  = 0
        not_found = 0

        for batch_start in range(0, total, batch_size):
            batch_symbols = symbols[batch_start:batch_start + batch_size]
            results       = self._query_batch(batch_symbols)

            for symbol, result in results.items():
                if symbol in symbol_map and result:
                    symbol_map[symbol].figi         = result.get("figi")
                    symbol_map[symbol].isin         = result.get("isin")
                    symbol_map[symbol].exchange_mic = result.get("exchCode")
                    enriched += 1
                else:
                    not_found += 1

            # Rate limiting — stay within free tier
            if batch_start + batch_size < total:
                time.sleep(1.0 / _RATE_LIMIT_RPS)

        logger.info(
            f"OpenFigiClient: enriched {enriched}/{total} instruments "
            f"({not_found} not found in OpenFIGI)"
        )
        return instruments

    def _query_batch(self, symbols: List[str]) -> Dict[str, Optional[dict]]:
        """
        Query OpenFIGI for a batch of symbols.

        Returns:
            Dict mapping symbol → first matching result dict (or None if not found).
        """
        payload = [
            {"idType": "TICKER", "idValue": s, "exchCode": "US"}
            for s in symbols
        ]

        for attempt in range(_RETRY_ATTEMPTS):
            try:
                response = self._session.post(
                    _OPENFIGI_URL,
                    json=payload,
                    timeout=30,
                )
                if response.status_code == 429:
                    wait = int(response.headers.get("Retry-After", 60))
                    logger.warning(
                        f"OpenFigiClient: rate limited — waiting {wait}s"
                    )
                    time.sleep(wait)
                    continue

                response.raise_for_status()
                data = response.json()
                break

            except requests.RequestException as e:
                if attempt < _RETRY_ATTEMPTS - 1:
                    logger.warning(
                        f"OpenFigiClient: attempt {attempt + 1} failed: {e} — retrying"
                    )
                    time.sleep(_RETRY_DELAY)
                else:
                    logger.error(f"OpenFigiClient: all retries exhausted: {e}")
                    return {s: None for s in symbols}

        results = {}
        for symbol, item in zip(symbols, data):
            if "error" in item or "data" not in item or not item["data"]:
                results[symbol] = None
            else:
                # Take first result — most relevant match for US equities
                match         = item["data"][0]
                results[symbol] = {
                    "figi":     match.get("figi"),
                    "isin":     match.get("isin"),
                    "exchCode": match.get("exchCode"),
                }

        return results
