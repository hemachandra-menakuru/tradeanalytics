"""
TradeAnalytics Yahoo Finance Provider
======================================
Lightweight fallback provider using the yfinance library.
Used for unit tests and local development only — NOT for production.

Capabilities:
  ✅ get_historical()    — daily and intraday OHLCV
  ⚠️  get_latest_quote() — delayed 15 minutes
  ❌ get_options_chain() — raises ProviderNotSupportedError
  ✅ is_market_open()    — computed from US Eastern time
  ✅ health_check()      — simple fetch test

Limitations vs IBKR:
  - No real-time data
  - No options chains
  - No VWAP
  - No trade count
  - No pre/post market data (via standard API)
  - Unofficial API — may break without notice
  - Not suitable for production trading decisions

Configuration:
  data:
    source: yahoo        # in config/dev.yml to use this provider
"""

from __future__ import annotations

import logging
import time
from datetime import date, datetime, timezone
from typing import List, Optional
from zoneinfo import ZoneInfo

from src.shared.config.config_loader import ConfigNode
from src.bronze.base.market_data_provider import (
    MarketDataProvider,
    ProviderConnectionError,
    ProviderNotSupportedError,
    ProviderDataError,
)

logger = logging.getLogger(__name__)

_EASTERN = ZoneInfo("America/New_York")
_MARKET_OPEN_HOUR   = 9
_MARKET_OPEN_MINUTE = 30
_MARKET_CLOSE_HOUR  = 16


class YahooProvider(MarketDataProvider):
    """
    Yahoo Finance data provider via yfinance library.
    For unit tests and local dev only — not for production.
    """

    def __init__(self, config: ConfigNode):
        super().__init__(config)
        try:
            import yfinance as yf
            self._yf = yf
        except ImportError:
            raise ImportError(
                "yfinance is not installed. "
                "Run: pip install yfinance"
            )

    # ── Provider identity ──────────────────────────────────────────────────────

    @property
    def provider_name(self) -> str:
        return "yahoo"

    @property
    def supports_options(self) -> bool:
        return False

    @property
    def supports_realtime(self) -> bool:
        return False

    @property
    def supported_intervals(self) -> List[str]:
        return ["1d", "1h", "5m", "1m"]

    # ── Core methods ───────────────────────────────────────────────────────────

    def get_historical(
        self,
        symbol: str,
        start_date: date,
        end_date: date,
        interval: str = "1d",
    ) -> List[dict]:
        """
        Fetch historical OHLCV from Yahoo Finance.
        Maps Yahoo's response to standard OHLCV dict format.
        """
        if not self.supports_interval(interval):
            raise ProviderNotSupportedError(
                f"Yahoo Finance does not support interval '{interval}'. "
                f"Supported: {self.supported_intervals}"
            )

        logger.info(
            f"Yahoo: fetching {symbol} {interval} "
            f"{start_date} → {end_date}"
        )

        fetch_start = time.time()

        try:
            ticker = self._yf.Ticker(symbol)
            # yfinance end date is exclusive — add 1 day
            from datetime import timedelta
            yf_end = end_date + timedelta(days=1)

            # auto_adjust=True gives split/dividend adjusted prices
            df = ticker.history(
                start=start_date.isoformat(),
                end=yf_end.isoformat(),
                interval=interval,
                auto_adjust=True,
                actions=True,       # includes dividends and splits
            )
        except Exception as e:
            raise ProviderConnectionError(
                f"Yahoo Finance fetch failed for {symbol}: {e}"
            ) from e

        fetch_duration_ms = int((time.time() - fetch_start) * 1000)

        if df is None or df.empty:
            logger.warning(f"Yahoo: no data returned for {symbol} {interval}")
            return []

        records = []
        try:
            for idx, row in df.iterrows():
                record = self._map_row_to_record(
                    symbol=symbol,
                    idx=idx,
                    row=row,
                    interval=interval,
                    fetch_duration_ms=fetch_duration_ms,
                )
                if record is not None:
                    records.append(record)
        except Exception as e:
            raise ProviderDataError(
                f"Failed to parse Yahoo Finance response for {symbol}: {e}"
            ) from e

        logger.info(
            f"Yahoo: fetched {len(records)} records for "
            f"{symbol} {interval} in {fetch_duration_ms}ms"
        )
        return records

    def get_latest_quote(self, symbol: str) -> Optional[dict]:
        """Returns latest quote — note: delayed 15 minutes on Yahoo."""
        try:
            ticker = self._yf.Ticker(symbol)
            info = ticker.fast_info
            return {
                "symbol":       symbol,
                "last":         getattr(info, "last_price", None),
                "bid":          None,  # Yahoo doesn't provide bid
                "ask":          None,  # Yahoo doesn't provide ask
                "volume":       getattr(info, "last_volume", None),
                "timestamp":    datetime.now(timezone.utc).isoformat(),
                "source":       self.provider_name,
                "is_delayed":   True,  # Yahoo data is delayed
            }
        except Exception as e:
            logger.warning(f"Yahoo: get_latest_quote failed for {symbol}: {e}")
            return None

    def get_options_chain(self, symbol: str, expiry: date) -> List[dict]:
        """Not supported by Yahoo provider."""
        raise ProviderNotSupportedError(
            f"Yahoo Finance provider does not support options chain data. "
            f"Use IBKRProvider for options data."
        )

    def is_market_open(self) -> bool:
        """
        Compute market status from US Eastern time.
        Market is open Mon-Fri 09:30-16:00 Eastern.
        """
        now_eastern = datetime.now(_EASTERN)
        if now_eastern.weekday() >= 5:  # Saturday=5, Sunday=6
            return False
        open_time  = now_eastern.replace(
            hour=_MARKET_OPEN_HOUR, minute=_MARKET_OPEN_MINUTE,
            second=0, microsecond=0
        )
        close_time = now_eastern.replace(
            hour=_MARKET_CLOSE_HOUR, minute=0,
            second=0, microsecond=0
        )
        return open_time <= now_eastern < close_time

    def health_check(self) -> bool:
        """Verify Yahoo Finance is reachable by fetching one day of SPY data."""
        try:
            ticker = self._yf.Ticker("SPY")
            df = ticker.history(period="1d")
            healthy = df is not None and not df.empty
            if healthy:
                logger.debug("Yahoo Finance health check passed")
            else:
                logger.warning("Yahoo Finance health check failed — empty response")
            return healthy
        except Exception as e:
            logger.error(f"Yahoo Finance health check failed: {e}")
            return False

    # ── Private mapping helpers ────────────────────────────────────────────────

    def _map_row_to_record(
        self,
        symbol: str,
        idx,
        row,
        interval: str,
        fetch_duration_ms: int,
    ) -> Optional[dict]:
        """
        Map a single yfinance DataFrame row to standard OHLCV dict.
        Returns None if row cannot be mapped (e.g. all nulls).
        """
        try:
            # Extract date and bar time
            if hasattr(idx, 'date'):
                record_date = idx.date().isoformat()
            else:
                record_date = str(idx)[:10]

            # Bar times for intraday
            bar_time_utc    = None
            bar_time_eastern = None
            bar_end_time_utc = None
            timezone_source  = None

            if interval != "1d" and hasattr(idx, 'tzinfo') and idx.tzinfo:
                utc_time = idx.astimezone(timezone.utc)
                bar_time_utc = utc_time.strftime("%Y-%m-%dT%H:%M:%SZ")
                eastern_time = idx.astimezone(_EASTERN)
                bar_time_eastern = eastern_time.strftime("%Y-%m-%dT%H:%M:%S")
                timezone_source = "America/New_York"

            # Corporate actions
            split_factor   = None
            dividend_amount = 0.0
            is_ex_div      = False
            is_ex_split    = False

            if "Stock Splits" in row and row["Stock Splits"] != 0:
                raw_split = row["Stock Splits"]
                # yfinance gives splits as ratio e.g. 4.0 for 4:1
                # Convert to adjustment factor e.g. 0.25
                split_factor = 1.0 / raw_split if raw_split != 0 else None
                is_ex_split  = True

            if "Dividends" in row and row["Dividends"] != 0:
                dividend_amount = float(row["Dividends"])
                is_ex_div = True

            return {
                # Identity
                "symbol":       symbol,
                "bar_date":     record_date,
                "interval":     interval,
                "source":       self.provider_name,

                # Raw price
                "open":         float(row["Open"])   if row["Open"]   is not None else None,
                "high":         float(row["High"])   if row["High"]   is not None else None,
                "low":          float(row["Low"])    if row["Low"]    is not None else None,
                "close":        float(row["Close"])  if row["Close"]  is not None else None,
                "volume":       int(row["Volume"])   if row["Volume"] is not None else None,

                # Adjusted price
                # With auto_adjust=True, Close IS the adjusted close
                "adj_close":    float(row["Close"])  if row["Close"]  is not None else None,
                "adj_factor":   None,   # Yahoo doesn't expose the factor separately
                "unadj_close":  float(row["Close"])  if row["Close"]  is not None else None,

                # Session context — Yahoo doesn't provide these
                "exchange":         None,
                "currency":         "USD",
                "market_session":   "regular",
                "trade_count":      None,   # not available
                "vwap":             None,   # not available
                "prev_close":       None,   # not available directly
                "session_open":     None,
                "session_close":    None,

                # Pre/post market — not available via standard yfinance
                "pre_market_high":    None,
                "pre_market_low":     None,
                "pre_market_close":   None,
                "pre_market_volume":  None,
                "post_market_high":   None,
                "post_market_low":    None,
                "post_market_close":  None,
                "post_market_volume": None,

                # Intraday bar times
                "bar_time_utc":     bar_time_utc,
                "bar_time_eastern": bar_time_eastern,
                "bar_end_time_utc": bar_end_time_utc,
                "timezone_source":  timezone_source,

                # Corporate actions
                "has_corporate_action": is_ex_div or is_ex_split,
                "split_factor":         split_factor,
                "dividend_amount":      dividend_amount,
                "dividend_type":        "cash" if is_ex_div else None,
                "is_ex_dividend_date":  is_ex_div,
                "is_ex_split_date":     is_ex_split,

                # Market conditions
                "trading_halt":     False,
                "halt_reason":      None,
                "is_trading_day":   True,
                "shares_outstanding": None,
                "market_cap_at_date": None,

                # Pipeline audit
                "fetch_duration_ms": fetch_duration_ms,
                "data_as_of":       datetime.now(timezone.utc).isoformat(),
                "source_version":   "yfinance",
            }
        except Exception as e:
            logger.warning(
                f"Yahoo: failed to map row for {symbol} at {idx}: {e}"
            )
            return None

# Self-register with factory
from src.bronze.factory.provider_factory import MarketDataFactory
MarketDataFactory.register('yahoo', YahooProvider)
