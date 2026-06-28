"""
TradeAnalytics ETF Holdings Universe Source
============================================
Downloads ETF constituent files from asset manager websites.
Used to determine which instruments belong to which index universe
and should have is_active=true in reference.instrument_feed_config.

Sources (all public, intended for investor consumption, no license required):
  SPY  → S&P 500       (SPDR / State Street)
  QQQ  → NASDAQ 100    (Invesco)
  IWM  → Russell 2000  (iShares / BlackRock)

is_active default by universe:
  SP500    (SPY) → True  — large cap, liquid, always ingest
  NDX100   (QQQ) → True  — tech/growth, liquid, always ingest
  RUSSELL2000 (IWM) → False — small cap, low liquidity, selective activation

Usage:
    source = EtfHoldingsSource()
    instruments = source.fetch()
    # Returns instruments with universe_codes populated
    # e.g. AAPL → universe_codes=["SP500", "NDX100"]
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from io import BytesIO, StringIO
from typing import Dict, List, Optional

import requests

from src.reference.sources.universe_source import RawInstrument, UniverseSource

logger = logging.getLogger(__name__)


@dataclass
class EtfConfig:
    """Configuration for one ETF holdings source."""
    etf_symbol:     str     # e.g. "SPY"
    universe_code:  str     # e.g. "SP500"
    url:            str     # holdings file URL
    is_active:      bool    # default is_active for instruments in this ETF
    file_format:    str     # "csv" or "xlsx"
    symbol_col:     str     # column name containing the ticker symbol
    weight_col:     Optional[str] = None    # column name for index weight
    name_col:       Optional[str] = None    # column name for company name
    skip_rows:      int = 0                 # rows to skip before header


# ETF sources — public holdings disclosures
_ETF_CONFIGS = [
    EtfConfig(
        etf_symbol   = "SPY",
        universe_code= "SP500",
        url          = "https://www.ssga.com/us/en/institutional/etfs/library-content/products/fund-data/etfs/us/holdings-daily-us-en-spy.xlsx",
        is_active    = True,
        file_format  = "xlsx",
        symbol_col   = "Ticker",
        weight_col   = "Weight",
        name_col     = "Name",
        skip_rows    = 4,
    ),
    EtfConfig(
        etf_symbol   = "QQQ",
        universe_code= "NDX100",
        url          = "https://www.invesco.com/us/financial-products/etfs/holdings/main/holdings/0?audienceType=Investor&ticker=QQQ",
        is_active    = True,
        file_format  = "csv",
        symbol_col   = "Holding Ticker",
        weight_col   = "Weight",
        name_col     = "Name",
        skip_rows    = 0,
    ),
    EtfConfig(
        etf_symbol   = "IWM",
        universe_code= "RUSSELL2000",
        url          = "https://www.ishares.com/us/products/239710/ishares-russell-2000-etf/1467271812596.ajax?fileType=csv&fileName=IWM_holdings&dataType=fund",
        is_active    = False,   # switched OFF by default — selective activation
        file_format  = "csv",
        symbol_col   = "Ticker",
        weight_col   = "Weight (%)",
        name_col     = "Name",
        skip_rows    = 9,       # iShares CSV has metadata rows at top
    ),
]


class EtfHoldingsSource(UniverseSource):
    """
    Downloads ETF constituent files to determine active universe membership.

    Returns instruments with universe_codes and index_weight populated.
    The same instrument can appear in multiple ETFs (e.g. AAPL in both
    SPY and QQQ) — universe_codes will contain both.

    is_active in the returned RawInstrument reflects the ETF's default:
      SPY/QQQ constituents → is_active intent = True
      IWM constituents     → is_active intent = False (selective)

    Note: When an instrument appears in both SPY (active=True) and IWM
    (active=False), the True takes precedence in the sync job.
    """

    def __init__(
        self,
        etf_configs: Optional[List[EtfConfig]] = None,
        timeout_seconds: int = 30,
    ):
        self._configs = etf_configs or _ETF_CONFIGS
        self._timeout = timeout_seconds

    @property
    def source_name(self) -> str:
        etfs = ", ".join(c.etf_symbol for c in self._configs)
        return f"ETF Holdings ({etfs})"

    def fetch(self) -> List[RawInstrument]:
        """
        Download all configured ETF holdings files and merge into one list.
        Deduplicates by symbol — combines universe_codes across ETFs.
        """
        logger.info(f"EtfHoldingsSource: fetching from {self.source_name}")

        # symbol → RawInstrument (merged across ETFs)
        merged: Dict[str, RawInstrument] = {}

        for cfg in self._configs:
            try:
                rows = self._fetch_etf(cfg)
                logger.info(
                    f"EtfHoldingsSource: {cfg.etf_symbol} → {len(rows)} constituents"
                )
                for inst in rows:
                    if inst.symbol in merged:
                        # Already seen from another ETF — just add universe_code
                        existing = merged[inst.symbol]
                        if cfg.universe_code not in existing.universe_codes:
                            existing.universe_codes.append(cfg.universe_code)
                        # If ANY ETF has it active, mark active
                        if cfg.is_active:
                            pass  # sync job handles this logic
                    else:
                        merged[inst.symbol] = inst
            except Exception as e:
                logger.error(
                    f"EtfHoldingsSource: failed to fetch {cfg.etf_symbol}: {e}",
                    exc_info=True,
                )
                # Continue — partial results are better than no results

        instruments = list(merged.values())
        logger.info(
            f"EtfHoldingsSource: {len(instruments)} unique instruments "
            f"across {len(self._configs)} ETFs"
        )
        return instruments

    def _fetch_etf(self, cfg: EtfConfig) -> List[RawInstrument]:
        """Fetch and parse one ETF's holdings file."""
        logger.debug(f"EtfHoldingsSource: downloading {cfg.etf_symbol} from {cfg.url}")
        response = requests.get(
            cfg.url,
            timeout=self._timeout,
            headers={"User-Agent": "TradeAnalytics/1.0 (institutional data use)"},
        )
        response.raise_for_status()

        if cfg.file_format == "xlsx":
            return self._parse_xlsx(response.content, cfg)
        else:
            return self._parse_csv(response.text, cfg)

    def _parse_xlsx(self, content: bytes, cfg: EtfConfig) -> List[RawInstrument]:
        """Parse an Excel holdings file."""
        try:
            import openpyxl
        except ImportError:
            raise ImportError("openpyxl required for xlsx parsing: pip install openpyxl")

        import openpyxl
        wb = openpyxl.load_workbook(BytesIO(content), data_only=True)
        ws = wb.active

        rows = list(ws.iter_rows(values_only=True))
        if not rows:
            return []

        # Skip metadata rows, then find header
        data_rows = rows[cfg.skip_rows:]
        if not data_rows:
            return []

        header = [str(c).strip() if c else "" for c in data_rows[0]]
        return self._rows_to_instruments(data_rows[1:], header, cfg)

    def _parse_csv(self, text: str, cfg: EtfConfig) -> List[RawInstrument]:
        """Parse a CSV holdings file."""
        import csv
        lines = text.splitlines()
        data_lines = lines[cfg.skip_rows:]
        if not data_lines:
            return []

        reader = csv.DictReader(StringIO("\n".join(data_lines)))
        rows = [(tuple(row.values()),) for row in reader]

        # Re-read with DictReader for clean column access
        reader2 = csv.DictReader(StringIO("\n".join(data_lines)))
        instruments = []
        for row in reader2:
            inst = self._row_dict_to_instrument(row, cfg)
            if inst:
                instruments.append(inst)
        return instruments

    def _rows_to_instruments(
        self, rows, header: List[str], cfg: EtfConfig
    ) -> List[RawInstrument]:
        """Convert xlsx rows to RawInstrument list."""
        instruments = []
        try:
            sym_idx    = header.index(cfg.symbol_col)
            name_idx   = header.index(cfg.name_col) if cfg.name_col else None
            weight_idx = header.index(cfg.weight_col) if cfg.weight_col else None
        except ValueError as e:
            logger.error(
                f"EtfHoldingsSource: column not found in {cfg.etf_symbol}: {e}. "
                f"Available: {header}"
            )
            return []

        for row in rows:
            if not row or row[0] is None:
                continue
            symbol = str(row[sym_idx]).strip().upper() if row[sym_idx] else ""
            if not symbol or symbol in ("-", "N/A", "CASH", ""):
                continue

            name   = str(row[name_idx]).strip() if name_idx is not None and row[name_idx] else ""
            weight = None
            if weight_idx is not None and row[weight_idx] is not None:
                try:
                    weight = float(str(row[weight_idx]).replace("%", "")) / 100
                except (ValueError, TypeError):
                    weight = None

            instruments.append(RawInstrument(
                symbol        = symbol,
                company_name  = name,
                asset_class   = "equity",   # ETF holdings are equities
                exchange      = "US",
                exchange_mic  = None,       # filled by OpenFIGI
                currency      = "USD",
                universe_codes= [cfg.universe_code],
                index_weight  = weight,
                source_etf    = cfg.etf_symbol,
            ))

        return instruments

    def _row_dict_to_instrument(
        self, row: dict, cfg: EtfConfig
    ) -> Optional[RawInstrument]:
        """Convert one CSV dict row to RawInstrument."""
        symbol = row.get(cfg.symbol_col, "").strip().upper()
        if not symbol or symbol in ("-", "N/A", "CASH", ""):
            return None

        name   = row.get(cfg.name_col, "").strip() if cfg.name_col else ""
        weight = None
        if cfg.weight_col and cfg.weight_col in row:
            try:
                weight = float(str(row[cfg.weight_col]).replace("%", "")) / 100
            except (ValueError, TypeError):
                weight = None

        return RawInstrument(
            symbol        = symbol,
            company_name  = name,
            asset_class   = "equity",
            exchange      = "US",
            exchange_mic  = None,
            currency      = "USD",
            universe_codes= [cfg.universe_code],
            index_weight  = weight,
            source_etf    = cfg.etf_symbol,
        )
