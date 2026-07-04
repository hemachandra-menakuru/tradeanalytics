"""
TradeAnalytics IB Gateway Provider (ib_insync)
===============================================
Secondary IBKR provider using IB Gateway socket API via ib_insync.

Connects to the EC2-hosted IB Gateway Docker container (port 4002, paper trading).
Use this when running ingestion from a cloud/scheduled context where the local
Client Portal gateway (localhost:5055) is not reachable.

Gateway: EC2 instance at 54.197.158.82, port 4002 (paper)
Switch:  config/sources.yml → sources.ibinsync.gateway_mode: ec2

API used:
  ib.qualifyContracts()       → resolve symbol to IBKR Contract object
  ib.reqHistoricalData()      → fetch OHLCV bars
  ib.isConnected()            → health check

Phase 1-4: use this provider for cloud-scheduled ingestion
Phase 5:   extend this provider with order placement methods
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timedelta, timezone
from typing import List

from src.shared.config.config_loader import ConfigNode
from src.shared.base.data_provider import HistoricalDataProvider
from src.bronze.base.market_data_provider import (
    ProviderAuthError,
    ProviderConnectionError,
    ProviderDataError,
    ProviderNotSupportedError,
)

logger = logging.getLogger(__name__)

# ib_insync bar size mapping: our interval → IB Gateway barSizeSetting string
_INTERVAL_TO_BAR_SIZE = {
    "1d":  "1 day",
    "1h":  "1 hour",
    "4h":  "4 hours",
    "5m":  "5 mins",
    "1m":  "1 min",
}


def _date_range_to_duration(start_date: date, end_date: date) -> str:
    """
    Convert a date range to an IB Gateway durationStr.
    IB Gateway requires a duration string like '30 D', '4 W', '2 M', '1 Y'.
    We calculate from end_date back to start_date and round up to the nearest unit.
    """
    days = (end_date - start_date).days + 1  # +1 to include end_date
    if days <= 365:
        return f"{max(1, days)} D"
    weeks = (days // 7) + 1
    if weeks <= 52:
        return f"{weeks} W"
    months = (days // 30) + 1
    return f"{months} M"


class IBInsyncProvider(HistoricalDataProvider):
    """
    IB Gateway provider using ib_insync socket protocol.
    Connects to the persistent IB Gateway process (local or EC2).
    """

    def __init__(self, config: ConfigNode):
        super().__init__(config)

        cfg               = config.sources.ibinsync
        gateway_mode      = getattr(cfg, "gateway_mode", "ec2")
        self._trading_mode = getattr(cfg, "trading_mode", "paper")
        gateway_cfg       = getattr(cfg.gateways, gateway_mode)
        self._host        = gateway_cfg.host
        port_key          = f"port_{self._trading_mode}"   # port_paper or port_live
        self._port        = int(getattr(gateway_cfg, port_key))
        self._timeout     = getattr(cfg, "timeout_seconds", 30)
        self._client_id   = getattr(cfg, "client_id", 10)

        self._ib = None  # lazy-connect on first use

        logger.info(
            f"IBInsyncProvider initialised — "
            f"gateway_mode={gateway_mode}, trading_mode={self._trading_mode}, "
            f"host={self._host}, port={self._port}"
        )

    # ── Provider identity ──────────────────────────────────────────────────────

    @property
    def provider_name(self) -> str:
        return "ibinsync"

    @property
    def supported_intervals(self) -> List[str]:
        return list(_INTERVAL_TO_BAR_SIZE.keys())

    @property
    def supports_options(self) -> bool:
        return False

    @property
    def supports_realtime(self) -> bool:
        return False

    # ── Connection management ──────────────────────────────────────────────────

    def _connect(self) -> None:
        """Establish socket connection to IB Gateway. Idempotent."""
        from ib_insync import IB
        if self._ib is None:
            self._ib = IB()
        if not self._ib.isConnected():
            try:
                self._ib.connect(
                    host=self._host,
                    port=self._port,
                    clientId=self._client_id,
                    timeout=self._timeout,
                    readonly=True,   # never place orders from the data provider
                )
                logger.info(f"Connected to IB Gateway at {self._host}:{self._port}")
            except Exception as e:
                raise ProviderConnectionError(
                    f"Cannot connect to IB Gateway at {self._host}:{self._port}. "
                    f"Check that the gateway is running and port is open. Error: {e}"
                ) from e

    def _disconnect(self) -> None:
        if self._ib and self._ib.isConnected():
            self._ib.disconnect()
            logger.info("Disconnected from IB Gateway")

    # ── Health check ───────────────────────────────────────────────────────────

    def health_check(self) -> bool:
        """Return True if IB Gateway is reachable and authenticated."""
        try:
            self._connect()
            return self._ib.isConnected()
        except Exception as e:
            logger.warning(f"IBInsyncProvider health check failed: {e}")
            return False

    # ── Historical data ────────────────────────────────────────────────────────

    def get_historical(
        self,
        symbol: str,
        start_date: date,
        end_date: date,
        interval: str = "1d",
    ) -> List[dict]:
        """
        Fetch OHLCV bars from IB Gateway for the given symbol and date range.
        Returns a list of OHLCVRecord-compatible dicts.
        """
        if not self.supports_interval(interval):
            raise ProviderNotSupportedError(
                f"IBInsyncProvider does not support interval '{interval}'. "
                f"Supported: {self.supported_intervals}"
            )

        from ib_insync import Stock

        self._connect()

        bar_size   = _INTERVAL_TO_BAR_SIZE[interval]
        duration   = _date_range_to_duration(start_date, end_date)
        # Explicit-UTC format (yyyymmdd-hh:mm:ss) — legacy space-separated form
        # triggers IBKR deprecation warning 2174 and will be rejected in a
        # future gateway API release.
        end_dt_str = end_date.strftime("%Y%m%d-23:59:59")

        # Resolve symbol to a qualified IBKR Contract
        contract = Stock(symbol, "SMART", "USD")
        try:
            self._ib.qualifyContracts(contract)
        except Exception as e:
            raise ProviderDataError(
                f"Could not qualify contract for symbol '{symbol}': {e}"
            ) from e

        # Fetch historical bars
        try:
            bars = self._ib.reqHistoricalData(
                contract,
                endDateTime=end_dt_str,
                durationStr=duration,
                barSizeSetting=bar_size,
                whatToShow="TRADES",
                useRTH=True,        # regular trading hours only
                formatDate=1,       # date as string YYYYMMDD
                keepUpToDate=False,
            )
        except Exception as e:
            raise ProviderDataError(
                f"Failed to fetch historical data for '{symbol}': {e}"
            ) from e

        if not bars:
            logger.warning(f"No bars returned for {symbol} {start_date}→{end_date}")
            return []

        records = []
        for bar in bars:
            bar_date = _parse_bar_date(bar.date)

            # Filter to requested date range (IB may return extra bars from duration rounding)
            if bar_date < start_date or bar_date > end_date:
                continue

            records.append({
                "symbol":        symbol,
                "date":          bar_date,
                "interval":      interval,
                "open":          float(bar.open),
                "high":          float(bar.high),
                "low":           float(bar.low),
                "close":         float(bar.close),
                "volume":        int(bar.volume),
                "vwap":          None,
                "trade_count":   None,
                "source":        "ibinsync",
                "ingested_by":   "ibinsync_provider_v1",
                "raw_response":  None,
            })

        logger.info(
            f"IBInsyncProvider fetched {len(records)} bars for {symbol} "
            f"{start_date}→{end_date} ({interval})"
        )
        return records


def _parse_bar_date(bar_date) -> date:
    """Parse IB Gateway bar date — can be a string 'YYYYMMDD' or datetime."""
    if isinstance(bar_date, datetime):
        return bar_date.date()
    if isinstance(bar_date, date):
        return bar_date
    # String format: '20260617' or '20260617  00:00:00'
    return datetime.strptime(str(bar_date).strip()[:8], "%Y%m%d").date()


# Self-register with factory
from src.bronze.factory.provider_factory import MarketDataFactory
MarketDataFactory.register("ibinsync", IBInsyncProvider)
