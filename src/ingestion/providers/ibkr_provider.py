"""
TradeAnalytics IBKR Client Portal REST API Provider
=====================================================
Primary data provider for all phases. Uses IBKR Client Portal REST API.

Gateway location: ~/dev/tools/ibkr/clientportal.gw
Start command:    bin/run.sh root/conf.yaml
Login at:         https://localhost:5055
Port:             5055 (5000 blocked by macOS Control Centre)

Verified working:
  - Account: U5498892 (NZD)
  - SPY conid: 756733
  - Historical OHLCV: 4 bars returned, correctly mapped
  - Session keepalive: /tickle endpoint working

API endpoints used:
  POST /iserver/secdef/search          → get conid for symbol
  GET  /iserver/marketdata/history     → historical OHLCV
  GET  /iserver/marketdata/snapshot    → real-time quote
  GET  /iserver/auth/status            → auth check
  POST /tickle                         → session keepalive
  GET  /portfolio/accounts             → account info

Phase 1-4: Client Portal REST API (this class)
Phase 5:   IB Gateway + ib_insync (separate provider)
"""

from __future__ import annotations

import json
import logging
import ssl
import time
import urllib.error
import urllib.request
from datetime import date, datetime, timezone, timedelta
from typing import Dict, List, Optional
from zoneinfo import ZoneInfo

from src.config.config_loader import ConfigNode
from src.ingestion.base.market_data_provider import (
    MarketDataProvider,
    ProviderAuthError,
    ProviderConnectionError,
    ProviderDataError,
    ProviderNotSupportedError,
)

logger = logging.getLogger(__name__)

_EASTERN = ZoneInfo("America/New_York")

# IBKR bar size mapping: our interval → IBKR bar parameter
_INTERVAL_TO_BAR = {
    "1d":  "1d",
    "1h":  "1h",
    "4h":  "4h",
    "5m":  "5mins",
    "1m":  "1min",
}

# IBKR period mapping: approximate days → IBKR period string
# IBKR accepts: {N}d, {N}w, {N}m, {N}y
def _days_to_period(days: int) -> str:
    """Convert number of days to IBKR period string."""
    if days <= 30:
        return f"{days}d"
    elif days <= 365:
        weeks = max(1, days // 7)
        return f"{weeks}w"
    else:
        months = max(1, days // 30)
        return f"{months}m"


class IBKRProvider(MarketDataProvider):
    """
    IBKR Client Portal REST API provider.
    Primary data source for all TradeAnalytics phases.
    """

    def __init__(self, config: ConfigNode):
        super().__init__(config)
        self._base_url    = config.sources.ibkr.base_url
        self._verify_ssl  = config.sources.ibkr.verify_ssl
        self._timeout     = config.sources.ibkr.timeout_seconds
        self._max_retries = config.sources.ibkr.max_retries
        self._account_id  = config.sources.ibkr.account_id

        # SSL context — Client Portal uses self-signed cert
        self._ssl_ctx = ssl.create_default_context()
        self._ssl_ctx.check_hostname = False
        self._ssl_ctx.verify_mode    = ssl.CERT_NONE

        # conid cache — avoid repeated contract searches
        self._conid_cache: Dict[str, str] = {}

        logger.info(
            f"IBKRProvider initialised — "
            f"base_url={self._base_url}, "
            f"account={'set' if self._account_id else 'NOT SET'}"
        )

    # ── Provider identity ──────────────────────────────────────────────────────

    @property
    def provider_name(self) -> str:
        return "ibkr"

    @property
    def supports_options(self) -> bool:
        return True

    @property
    def supports_realtime(self) -> bool:
        return True

    @property
    def supported_intervals(self) -> List[str]:
        return list(_INTERVAL_TO_BAR.keys())

    # ── HTTP helpers ───────────────────────────────────────────────────────────

    def _get(self, endpoint: str) -> dict:
        """Make GET request to Client Portal API with retry logic."""
        url = f"{self._base_url}{endpoint}"
        return self._request(url, method="GET")

    def _post(self, endpoint: str, data: dict = None) -> dict:
        """Make POST request to Client Portal API with retry logic."""
        url = f"{self._base_url}{endpoint}"
        return self._request(url, method="POST", data=data)

    def _request(
        self,
        url: str,
        method: str = "GET",
        data: dict = None,
        attempt: int = 1,
    ) -> dict:
        """Make HTTP request with retry on failure."""
        payload = json.dumps(data or {}).encode("utf-8") if data else None
        req     = urllib.request.Request(url, data=payload, method=method)
        req.add_header("User-Agent", "TradeAnalytics/1.0")
        if payload:
            req.add_header("Content-Type", "application/json")

        try:
            with urllib.request.urlopen(
                req, context=self._ssl_ctx, timeout=self._timeout
            ) as resp:
                body = resp.read().decode("utf-8")
                return json.loads(body) if body.strip() else {}

        except urllib.error.HTTPError as e:
            if e.code == 401:
                raise ProviderAuthError(
                    f"IBKR session expired or not authenticated. "
                    f"Login at: https://localhost:5055"
                )
            if e.code in (429, 503) and attempt <= self._max_retries:
                wait = 2 ** attempt
                logger.warning(f"IBKR rate limit/unavailable — retry {attempt} in {wait}s")
                time.sleep(wait)
                return self._request(url, method, data, attempt + 1)
            raise ProviderConnectionError(f"IBKR HTTP {e.code}: {e.reason}")

        except urllib.error.URLError as e:
            if attempt <= self._max_retries:
                wait = 2 ** attempt
                logger.warning(f"IBKR connection error — retry {attempt} in {wait}s: {e}")
                time.sleep(wait)
                return self._request(url, method, data, attempt + 1)
            raise ProviderConnectionError(
                f"Cannot reach IBKR gateway at {self._base_url}. "
                f"Is the gateway running? Start: bin/run.sh root/conf.yaml"
            )

        except json.JSONDecodeError as e:
            raise ProviderDataError(f"Invalid JSON from IBKR: {e}")

    # ── Contract resolution ────────────────────────────────────────────────────

    def _get_conid(self, symbol: str) -> str:
        """
        Get IBKR contract ID (conid) for a symbol.
        Cached after first lookup to avoid repeated searches.
        """
        if symbol in self._conid_cache:
            return self._conid_cache[symbol]

        logger.debug(f"[{symbol}] Searching for conid...")
        result = self._post(
            "/iserver/secdef/search",
            {"symbol": symbol, "name": False, "secType": "STK"}
        )

        if not result:
            raise ProviderDataError(
                f"No contract found for symbol '{symbol}'. "
                f"Check symbol is valid and listed on a US exchange."
            )

        contracts = result if isinstance(result, list) else [result]

        # Prefer US STK contract
        conid = None
        for contract in contracts:
            conid_candidate = str(contract.get("conid", ""))
            if conid_candidate:
                conid = conid_candidate
                break

        if not conid:
            raise ProviderDataError(
                f"Could not resolve conid for '{symbol}'. "
                f"Response: {result}"
            )

        self._conid_cache[symbol] = conid
        logger.debug(f"[{symbol}] Resolved conid={conid}")
        return conid

    # ── Core methods ───────────────────────────────────────────────────────────

    def get_historical(
        self,
        symbol: str,
        start_date: date,
        end_date: date,
        interval: str = "1d",
    ) -> List[dict]:
        """
        Fetch historical OHLCV from IBKR Client Portal.
        Maps IBKR response to standard OHLCV dict format.

        IBKR API note:
          - Returns data ending at current time or market close
          - Period = total lookback from end, not a date range
          - We calculate period from start_date to end_date
        """
        if not self.supports_interval(interval):
            raise ProviderNotSupportedError(
                f"IBKR provider does not support interval '{interval}'. "
                f"Supported: {self.supported_intervals}"
            )

        bar      = _INTERVAL_TO_BAR[interval]
        days     = (end_date - start_date).days + 1
        period   = _days_to_period(days)

        logger.info(
            f"[{symbol}] IBKR fetch: interval={interval}, "
            f"{start_date} → {end_date} ({days} days, period={period})"
        )

        fetch_start = time.time()

        try:
            # Step 1: Get conid
            conid = self._get_conid(symbol)

            # Step 2: Keepalive before data request
            self._tickle()

            # Step 3: Fetch historical data
            endpoint = (
                f"/iserver/marketdata/history"
                f"?conid={conid}"
                f"&period={period}"
                f"&bar={bar}"
                f"&outsideRth=false"
            )
            result = self._get(endpoint)

        except ProviderAuthError as e:
            # Gateway reachable but not authenticated — degrade gracefully.
            # Caller (BronzeIngestionJob) checks health_check() before calling
            # get_historical(), so this path means session expired mid-run.
            logger.warning(
                f"[{symbol}] IBKR not authenticated — returning empty. "
                f"Login at https://localhost:5055. Error: {e}"
            )
            return []
        except ProviderConnectionError as e:
            # Gateway unreachable — degrade gracefully.
            logger.warning(
                f"[{symbol}] IBKR gateway unreachable — returning empty. "
                f"Error: {e}"
            )
            return []
        except Exception as e:
            raise ProviderConnectionError(
                f"IBKR historical fetch failed for {symbol}: {e}"
            ) from e

        fetch_duration_ms = int((time.time() - fetch_start) * 1000)

        data = result.get("data", [])
        if not data:
            logger.warning(f"[{symbol}] IBKR returned empty data for {period}/{bar}")
            return []

        # Step 4: Map to standard format
        records = []
        for bar_data in data:
            record = self._map_bar_to_record(
                bar_data=bar_data,
                symbol=symbol,
                interval=interval,
                fetch_duration_ms=fetch_duration_ms,
            )
            if record is None:
                continue

            # Filter to requested date range
            record_date = date.fromisoformat(record["date"])
            if start_date <= record_date <= end_date:
                records.append(record)

        logger.info(
            f"[{symbol}] IBKR: {len(records)} records fetched "
            f"in {fetch_duration_ms}ms"
        )
        return records

    def get_latest_quote(self, symbol: str) -> Optional[dict]:
        """
        Fetch real-time quote via snapshot endpoint.
        Fields: 31=last, 84=bid, 86=ask, 7762=volume, 7295=open
        """
        try:
            conid  = self._get_conid(symbol)
            result = self._get(
                f"/iserver/marketdata/snapshot"
                f"?conids={conid}&fields=31,84,86,7762,7295,70,71"
            )
            data  = result if isinstance(result, list) else [result]
            quote = data[0] if data else {}

            def _safe_float(val):
                try:
                    return float(val) if val and val != "N/A" else None
                except (ValueError, TypeError):
                    return None

            return {
                "symbol":    symbol,
                "last":      _safe_float(quote.get("31")),
                "bid":       _safe_float(quote.get("84")),
                "ask":       _safe_float(quote.get("86")),
                "volume":    _safe_float(quote.get("7762")),
                "open":      _safe_float(quote.get("7295")),
                "high":      _safe_float(quote.get("70")),
                "low":       _safe_float(quote.get("71")),
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "source":    self.provider_name,
                "is_delayed": False,
            }
        except Exception as e:
            logger.warning(f"[{symbol}] IBKR get_latest_quote failed: {e}")
            return None

    def get_options_chain(self, symbol: str, expiry: date) -> List[dict]:
        """
        Fetch options chain for symbol and expiry.
        Phase 4 implementation — stub for now.
        """
        logger.warning(
            f"IBKRProvider.get_options_chain() not yet implemented. "
            f"Coming in Phase 4."
        )
        return []

    def is_market_open(self) -> bool:
        """Check market status from US Eastern time."""
        now_eastern = datetime.now(_EASTERN)
        if now_eastern.weekday() >= 5:
            return False
        open_time  = now_eastern.replace(hour=9, minute=30, second=0, microsecond=0)
        close_time = now_eastern.replace(hour=16, minute=0, second=0, microsecond=0)
        return open_time <= now_eastern < close_time

    def health_check(self) -> bool:
        """
        Verify IBKR Client Portal is reachable and authenticated.
        Uses /tickle endpoint — lightweight and maintains session.
        """
        try:
            result        = self._post("/tickle")
            session       = result.get("session", "")
            authenticated = result.get("iserver", {}).get("authStatus", {}).get(
                "authenticated", False
            )

            # Also check auth status endpoint
            auth   = self._get("/iserver/auth/status")
            is_auth = auth.get("authenticated", False)

            healthy = bool(session) and is_auth
            if healthy:
                logger.info("IBKR health check passed — authenticated")
            else:
                logger.warning(
                    f"IBKR health check failed — "
                    f"session={bool(session)}, authenticated={is_auth}"
                )
            return healthy

        except Exception as e:
            logger.error(f"IBKR health check failed: {e}")
            return False

    # ── Private helpers ────────────────────────────────────────────────────────

    def _tickle(self) -> None:
        """Send keepalive to prevent session timeout."""
        try:
            self._post("/tickle")
        except Exception as e:
            logger.warning(f"IBKR tickle failed: {e}")

    def _map_bar_to_record(
        self,
        bar_data: dict,
        symbol: str,
        interval: str,
        fetch_duration_ms: int,
    ) -> Optional[dict]:
        """
        Map a single IBKR bar to our standard OHLCV dict format.

        IBKR bar format:
          o = open, h = high, l = low, c = close
          v = volume, t = timestamp in milliseconds UTC
        """
        try:
            ts_ms = bar_data.get("t", 0)
            if not ts_ms:
                return None

            # Convert millisecond timestamp to date
            bar_dt = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
            bar_date_str = bar_dt.strftime("%Y-%m-%d")

            # Intraday bar times
            bar_time_utc     = None
            bar_time_eastern = None
            bar_end_time_utc = None
            timezone_source  = None

            if interval != "1d":
                bar_time_utc     = bar_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
                eastern_dt       = bar_dt.astimezone(_EASTERN)
                bar_time_eastern = eastern_dt.strftime("%Y-%m-%dT%H:%M:%S")
                timezone_source  = "America/New_York"

                # Compute bar end time
                interval_minutes = {
                    "1h": 60, "4h": 240, "5m": 5, "1m": 1
                }.get(interval, 60)
                bar_end = bar_dt + timedelta(minutes=interval_minutes)
                bar_end_time_utc = bar_end.strftime("%Y-%m-%dT%H:%M:%SZ")

            open_  = float(bar_data.get("o", 0))
            high   = float(bar_data.get("h", 0))
            low    = float(bar_data.get("l", 0))
            close  = float(bar_data.get("c", 0))
            volume = int(float(bar_data.get("v", 0)))

            # Basic sanity check
            if open_ <= 0 or close <= 0:
                logger.warning(
                    f"[{symbol}] Skipping bar with invalid prices: "
                    f"open={open_}, close={close}"
                )
                return None

            return {
                # Identity
                "symbol":   symbol,
                "date":     bar_date_str,
                "interval": interval,
                "source":   self.provider_name,

                # Raw price
                "open":   open_,
                "high":   high,
                "low":    low,
                "close":  close,
                "volume": volume,

                # Adjusted (IBKR returns adjusted by default)
                "adj_close":  close,
                "adj_factor": 1.0,
                "unadj_close": close,

                # Session context
                "exchange":        "SMART",
                "currency":        "USD",
                "market_session":  "regular",
                "trade_count":     None,  # not in history endpoint
                "vwap":            None,  # not in history endpoint
                "prev_close":      None,  # not available in batch

                # Pre/post market
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

                # Corporate actions (not in history endpoint)
                "has_corporate_action": False,
                "split_factor":         None,
                "dividend_amount":      0.0,
                "dividend_type":        None,
                "is_ex_dividend_date":  False,
                "is_ex_split_date":     False,

                # Market conditions
                "trading_halt":       False,
                "halt_reason":        None,
                "is_trading_day":     True,
                "shares_outstanding": None,
                "market_cap_at_date": None,

                # Pipeline audit
                "fetch_duration_ms": fetch_duration_ms,
                "data_as_of":        datetime.now(timezone.utc).isoformat(),
                "source_version":    "client_portal_v1",
            }

        except Exception as e:
            logger.warning(
                f"[{symbol}] Failed to map IBKR bar: {e}. bar_data={bar_data}"
            )
            return None


# Self-register with factory
from src.ingestion.factory.provider_factory import MarketDataFactory
MarketDataFactory.register("ibkr", IBKRProvider)
