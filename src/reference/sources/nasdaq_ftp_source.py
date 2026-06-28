"""
TradeAnalytics NASDAQ FTP Universe Source
==========================================
Downloads ALL US-listed securities from the NASDAQ Trader FTP.
Populates reference.instrument as a comprehensive master registry.

Source files (updated nightly by NASDAQ):
  ftp://ftp.nasdaqtrader.com/SymbolDirectory/nasdaqlisted.txt  — NASDAQ-listed
  ftp://ftp.nasdaqtrader.com/SymbolDirectory/otherlisted.txt   — NYSE, ARCA, BATS etc.

These are official exchange publications. Free, no license, no API key.
We access them over HTTPS (NASDAQ provides an HTTP mirror).

Scope:
  - asset_class = equity OR etf (filtered from raw file)
  - Excludes: warrants, rights, units, preferred shares, test symbols
  - Excluded: Forex, Crypto (per project scope decision 2026-06-26)

Usage:
    source = NasdaqFtpSource()
    instruments = source.fetch()   # ~8,000–10,000 instruments
"""

from __future__ import annotations

import logging
from io import StringIO
from typing import List

import requests

from src.reference.sources.universe_source import RawInstrument, UniverseSource

logger = logging.getLogger(__name__)

# NASDAQ provides HTTPS mirrors of the FTP files — no FTP client needed
_NASDAQ_LISTED_URL = (
    "http://ftp.nasdaqtrader.com/dynamic/SymbolDirectory/nasdaqlisted.txt"
)
_OTHER_LISTED_URL = (
    "http://ftp.nasdaqtrader.com/dynamic/SymbolDirectory/otherlisted.txt"
)

# Exchange code → ISO 10383 MIC mapping (from otherlisted.txt Exchange column)
_EXCHANGE_TO_MIC = {
    "A": "XASE",   # NYSE American (AMEX)
    "N": "XNYS",   # NYSE
    "P": "ARCX",   # NYSE Arca
    "Z": "BATS",   # CBOE BZX (formerly BATS)
    "V": "IEXG",   # IEX
    "Q": "XNAS",   # NASDAQ (nasdaqlisted.txt)
}

# Suffixes that indicate non-equity instruments we exclude
_EXCLUDED_SUFFIXES = (
    " warrant", " warrants", " right", " rights", " unit", " units",
    " preferred", " pfd", " note", " notes", " bond", " debenture",
    "test", " acquisition", " spac",
)


def _is_excluded(name: str, symbol: str) -> bool:
    """Return True if this row should be excluded from the universe."""
    name_lower = name.lower()
    symbol_upper = symbol.upper()

    # NASDAQ marks test symbols with a $ suffix
    if symbol_upper.endswith("$"):
        return True
    # Skip ETNs, structured products, and other non-equity/non-ETF instruments
    for suffix in _EXCLUDED_SUFFIXES:
        if suffix in name_lower:
            return True
    return False


def _parse_nasdaq_listed(text: str) -> List[RawInstrument]:
    """Parse nasdaqlisted.txt — pipe-delimited, NASDAQ-listed securities."""
    instruments = []
    reader = StringIO(text)
    lines = reader.readlines()

    # Header: Symbol|Security Name|Market Category|Test Issue|Financial Status|Round Lot Size|ETF|NextShares
    for line in lines[1:]:           # skip header
        line = line.strip()
        if not line or line.startswith("File Creation Time"):
            continue
        parts = line.split("|")
        if len(parts) < 7:
            continue

        symbol      = parts[0].strip()
        name        = parts[1].strip()
        is_etf      = parts[6].strip().upper() == "Y"
        test_issue  = parts[3].strip().upper() == "Y"

        if test_issue:
            continue
        if _is_excluded(name, symbol):
            continue

        asset_class = "etf" if is_etf else "equity"
        instruments.append(RawInstrument(
            symbol       = symbol,
            company_name = name,
            asset_class  = asset_class,
            exchange     = "NASDAQ",
            exchange_mic = "XNAS",
            currency     = "USD",
        ))

    logger.info(f"NasdaqFtpSource: parsed {len(instruments)} from nasdaqlisted.txt")
    return instruments


def _parse_other_listed(text: str) -> List[RawInstrument]:
    """Parse otherlisted.txt — pipe-delimited, NYSE/Arca/BATS/AMEX securities."""
    instruments = []
    reader = StringIO(text)
    lines = reader.readlines()

    # Header: ACT Symbol|Security Name|Exchange|CQS Symbol|ETF|Round Lot Size|Test Issue|NASDAQ Symbol
    for line in lines[1:]:
        line = line.strip()
        if not line or line.startswith("File Creation Time"):
            continue
        parts = line.split("|")
        if len(parts) < 7:
            continue

        symbol      = parts[0].strip()
        name        = parts[1].strip()
        exchange_cd = parts[2].strip()
        is_etf      = parts[4].strip().upper() == "Y"
        test_issue  = parts[6].strip().upper() == "Y"

        if test_issue:
            continue
        if _is_excluded(name, symbol):
            continue

        exchange     = exchange_cd
        exchange_mic = _EXCHANGE_TO_MIC.get(exchange_cd)
        asset_class  = "etf" if is_etf else "equity"

        instruments.append(RawInstrument(
            symbol       = symbol,
            company_name = name,
            asset_class  = asset_class,
            exchange     = exchange,
            exchange_mic = exchange_mic,
            currency     = "USD",
        ))

    logger.info(f"NasdaqFtpSource: parsed {len(instruments)} from otherlisted.txt")
    return instruments


class NasdaqFtpSource(UniverseSource):
    """
    Fetches all US-listed equities and ETFs from the NASDAQ Trader FTP.

    This is the comprehensive instrument registry source — it returns
    everything, not just what we actively trade.
    All returned instruments go into reference.instrument with is_active=false
    by default. The feed config job activates the subset we want to ingest.
    """

    def __init__(self, timeout_seconds: int = 30):
        self._timeout = timeout_seconds

    @property
    def source_name(self) -> str:
        return "NASDAQ Trader FTP"

    def fetch(self) -> List[RawInstrument]:
        """
        Download and parse both NASDAQ and other-listed files.
        Returns the union — deduplicated by symbol.
        """
        logger.info("NasdaqFtpSource: fetching universe from NASDAQ Trader FTP")

        nasdaq    = self._download(_NASDAQ_LISTED_URL)
        other     = self._download(_OTHER_LISTED_URL)
        nasdaq_instruments = _parse_nasdaq_listed(nasdaq)
        other_instruments  = _parse_other_listed(other)

        # Deduplicate by symbol — NASDAQ listing takes precedence
        seen = {}
        for inst in nasdaq_instruments:
            seen[inst.symbol] = inst
        for inst in other_instruments:
            if inst.symbol not in seen:
                seen[inst.symbol] = inst

        instruments = list(seen.values())
        logger.info(
            f"NasdaqFtpSource: {len(instruments)} unique instruments "
            f"({sum(1 for i in instruments if i.asset_class == 'equity')} equity, "
            f"{sum(1 for i in instruments if i.asset_class == 'etf')} etf)"
        )
        return instruments

    def _download(self, url: str) -> str:
        """Download a file and return its text content."""
        logger.debug(f"NasdaqFtpSource: downloading {url}")
        response = requests.get(url, timeout=self._timeout)
        response.raise_for_status()
        return response.text
