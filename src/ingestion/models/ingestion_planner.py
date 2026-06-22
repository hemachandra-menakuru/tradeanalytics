
"""
TradeAnalytics Ingestion Planner
==================================
Determines the correct IngestionMode and FetchPlan for each
symbol + interval combination based on:
  - Current watermark (what we already have)
  - Stream config settings (modes, overrides, thresholds)
  - Market calendar (holidays, weekends)

Config path changes (after stream config refactor):
  OLD: config.data.ingestion.*        NEW: config.{stream}.ingestion.*
  OLD: config.data.intervals.daily.*  NEW: config.{stream}.history.*
  OLD: config.data.source             NEW: config.sources.primary

Usage:
    planner = IngestionPlanner(config, stream_name="daily")
    plan = planner.plan(symbol="AAPL", interval="1d", watermark=wm)
"""

from __future__ import annotations

import logging
from datetime import date, timedelta
from typing import List, Optional

from src.config.config_loader import ConfigNode
from src.ingestion.models.ingestion_mode import (
    IngestionMode, IngestionWatermark, FetchPlan
)

logger = logging.getLogger(__name__)

# US market holidays 2010–2040.
# Source: NYSE holiday schedule (historical + projected).
# TODO Phase 3: migrate to reference table in Unity Catalog
#   (tradeanalytics.reference.market_holidays) and load dynamically.
_US_MARKET_HOLIDAYS: set = {
    # 2010
    date(2010,1,1), date(2010,1,18), date(2010,2,15), date(2010,4,2),
    date(2010,5,31), date(2010,7,5), date(2010,9,6), date(2010,11,25), date(2010,12,24),
    # 2011
    date(2011,1,17), date(2011,2,21), date(2011,4,22),
    date(2011,5,30), date(2011,7,4), date(2011,9,5), date(2011,11,24), date(2011,12,26),
    # 2012
    date(2012,1,2), date(2012,1,16), date(2012,2,20), date(2012,4,6),
    date(2012,5,28), date(2012,7,4), date(2012,9,3), date(2012,10,29), date(2012,10,30),
    date(2012,11,22), date(2012,12,25),
    # 2013
    date(2013,1,1), date(2013,1,21), date(2013,2,18), date(2013,3,29),
    date(2013,5,27), date(2013,7,4), date(2013,9,2), date(2013,11,28), date(2013,12,25),
    # 2014
    date(2014,1,1), date(2014,1,20), date(2014,2,17), date(2014,4,18),
    date(2014,5,26), date(2014,7,4), date(2014,9,1), date(2014,11,27), date(2014,12,25),
    # 2015
    date(2015,1,1), date(2015,1,19), date(2015,2,16), date(2015,4,3),
    date(2015,5,25), date(2015,7,3), date(2015,9,7), date(2015,11,26), date(2015,12,25),
    # 2016
    date(2016,1,1), date(2016,1,18), date(2016,2,15), date(2016,3,25),
    date(2016,5,30), date(2016,7,4), date(2016,9,5), date(2016,11,24), date(2016,12,26),
    # 2017
    date(2017,1,2), date(2017,1,16), date(2017,2,20), date(2017,4,14),
    date(2017,5,29), date(2017,7,4), date(2017,9,4), date(2017,11,23), date(2017,12,25),
    # 2018
    date(2018,1,1), date(2018,1,15), date(2018,2,19), date(2018,3,30),
    date(2018,5,28), date(2018,7,4), date(2018,9,3), date(2018,11,22), date(2018,12,5),
    date(2018,12,25),
    # 2019
    date(2019,1,1), date(2019,1,21), date(2019,2,18), date(2019,4,19),
    date(2019,5,27), date(2019,7,4), date(2019,9,2), date(2019,11,28), date(2019,12,25),
    # 2020
    date(2020,1,1), date(2020,1,20), date(2020,2,17), date(2020,4,10),
    date(2020,5,25), date(2020,7,3), date(2020,9,7), date(2020,11,26), date(2020,12,25),
    # 2021
    date(2021,1,1), date(2021,1,18), date(2021,2,15), date(2021,4,2),
    date(2021,5,31), date(2021,7,5), date(2021,9,6), date(2021,11,25), date(2021,12,24),
    # 2022
    date(2022,1,17), date(2022,2,21), date(2022,4,15),
    date(2022,5,30), date(2022,6,20), date(2022,7,4), date(2022,9,5),
    date(2022,11,24), date(2022,12,26),
    # 2023
    date(2023,1,2), date(2023,1,16), date(2023,2,20), date(2023,4,7),
    date(2023,5,29), date(2023,7,4), date(2023,9,4), date(2023,11,23), date(2023,12,25),
    # 2024
    date(2024,1,1), date(2024,1,15), date(2024,2,19), date(2024,3,29),
    date(2024,5,27), date(2024,7,4), date(2024,9,2), date(2024,11,28), date(2024,12,25),
    # 2025
    date(2025,1,1), date(2025,1,9), date(2025,1,20), date(2025,2,17), date(2025,4,18),
    date(2025,5,26), date(2025,7,4), date(2025,9,1), date(2025,11,27), date(2025,12,25),
    # 2026
    date(2026,1,1), date(2026,1,19), date(2026,2,16), date(2026,4,3),
    date(2026,5,25), date(2026,7,3), date(2026,9,7), date(2026,11,26), date(2026,12,25),
    # 2027
    date(2027,1,1), date(2027,1,18), date(2027,2,15), date(2027,3,26),
    date(2027,5,31), date(2027,7,5), date(2027,9,6), date(2027,11,25), date(2027,12,24),
    # 2028
    date(2028,1,17), date(2028,2,21), date(2028,4,14),
    date(2028,5,29), date(2028,7,4), date(2028,9,4), date(2028,11,23), date(2028,12,25),
    # 2029
    date(2029,1,1), date(2029,1,15), date(2029,2,19), date(2029,3,30),
    date(2029,5,28), date(2029,7,4), date(2029,9,3), date(2029,11,22), date(2029,12,25),
    # 2030
    date(2030,1,1), date(2030,1,21), date(2030,2,18), date(2030,4,19),
    date(2030,5,27), date(2030,7,4), date(2030,9,2), date(2030,11,28), date(2030,12,25),
    # 2031
    date(2031,1,1), date(2031,1,20), date(2031,2,17), date(2031,4,11),
    date(2031,5,26), date(2031,7,4), date(2031,9,1), date(2031,11,27), date(2031,12,25),
    # 2032
    date(2032,1,1), date(2032,1,19), date(2032,2,16), date(2032,3,26),
    date(2032,5,31), date(2032,7,5), date(2032,9,6), date(2032,11,25), date(2032,12,24),
    # 2033
    date(2033,1,17), date(2033,2,21), date(2033,4,15),
    date(2033,5,30), date(2033,7,4), date(2033,9,5), date(2033,11,24), date(2033,12,26),
    # 2034
    date(2034,1,2), date(2034,1,16), date(2034,2,20), date(2034,3,31),
    date(2034,5,29), date(2034,7,4), date(2034,9,4), date(2034,11,23), date(2034,12,25),
    # 2035
    date(2035,1,1), date(2035,1,15), date(2035,2,19), date(2035,4,20),
    date(2035,5,28), date(2035,7,4), date(2035,9,3), date(2035,11,22), date(2035,12,25),
    # 2036
    date(2036,1,1), date(2036,1,21), date(2036,2,18), date(2036,4,11),
    date(2036,5,26), date(2036,7,4), date(2036,9,1), date(2036,11,27), date(2036,12,25),
    # 2037
    date(2037,1,1), date(2037,1,19), date(2037,2,16), date(2037,4,3),
    date(2037,5,25), date(2037,7,3), date(2037,9,7), date(2037,11,26), date(2037,12,25),
    # 2038
    date(2038,1,1), date(2038,1,18), date(2038,2,15), date(2038,4,23),
    date(2038,5,31), date(2038,7,5), date(2038,9,6), date(2038,11,25), date(2038,12,24),
    # 2039
    date(2039,1,17), date(2039,2,21), date(2039,4,8),
    date(2039,5,30), date(2039,7,4), date(2039,9,5), date(2039,11,24), date(2039,12,26),
    # 2040
    date(2040,1,2), date(2040,1,16), date(2040,2,20), date(2040,4,30),
    date(2040,5,28), date(2040,7,4), date(2040,9,3), date(2040,11,22), date(2040,12,25),
}


def is_trading_day(d: date) -> bool:
    if d.weekday() >= 5:
        return False
    if d in _US_MARKET_HOLIDAYS:
        return False
    return True


def _symbol_in_scope(symbol: str, symbols_list) -> bool:
    if symbols_list is None:
        return True
    try:
        scope = list(symbols_list) if symbols_list else []
    except TypeError:
        scope = []
    return len(scope) == 0 or symbol in scope


class IngestionPlanner:
    """
    Pure logic class — determines what to fetch for each symbol.
    No I/O, no Spark, no provider calls — fully testable.

    Config structure (post stream-config refactor):
      config.{stream_name}.ingestion.*  — operational modes
      config.{stream_name}.history.*    — lookback/retention/buffer
      config.sources.primary            — provider name

    Priority order (highest to lowest):
      1. force_reload       — nuclear option, requires confirm=true
      2. restatement        — re-fetch + amend changed records
      3. explicit_date_range — manual date range override
      4. history_extension  — extend backwards beyond earliest_date
      5. initial_load       — no watermark exists (first run)
      6. gap_fill           — gap > max_gap_days threshold
      7. no_op              — already up to date
      8. incremental        — normal daily update + amendment buffer

    All modes support symbols: [] scoping:
      symbols: []              → applies to ALL active symbols
      symbols: ["AAPL","MSFT"] → applies ONLY to listed symbols
    """

    def __init__(self, config: ConfigNode, stream_name: str = "daily"):
        self._config      = config
        self._stream_name = stream_name
        # Stream config root: config.daily / config.intraday / config.tick
        self._stream_cfg  = getattr(config, stream_name)

    def plan(
        self,
        symbol: str,
        interval: str,
        watermark: Optional[IngestionWatermark],
        as_of_date: Optional[date] = None,
        ticker_history_start: Optional[date] = None,
    ) -> FetchPlan:
        """
        Determine the fetch plan for a symbol + interval.

        Args:
            symbol:               Ticker e.g. "AAPL"
            interval:             Interval e.g. "1d"
            watermark:            Current watermark (None = first run)
            as_of_date:           Planning date (default: today)
            ticker_history_start: Per-ticker override from ref_tickers

        Returns:
            FetchPlan with mode, start_date, end_date, batch_size
        """
        today         = as_of_date or date.today()
        ingestion_cfg = self._stream_cfg.ingestion
        history_cfg   = self._stream_cfg.history
        batch_size    = ingestion_cfg.batch_size_days

        # ── Priority 1: Force reload ───────────────────────────────────────
        force_cfg = ingestion_cfg.force_reload
        if (force_cfg.enabled
                and force_cfg.confirm
                and _symbol_in_scope(symbol, force_cfg.symbols)):
            start = self._get_initial_start(today, history_cfg, ticker_history_start)
            logger.warning(f"[{symbol}/{interval}] FORCE RELOAD {start} → {today}")
            return FetchPlan(
                symbol=symbol, interval=interval,
                mode=IngestionMode.FORCE_RELOAD,
                start_date=start, end_date=today,
                ingestion_type=IngestionMode.FORCE_RELOAD.ingestion_type,
                batch_size_days=batch_size, watermark=watermark,
            )

        # ── Priority 2: Restatement ───────────────────────────────────────
        restate_cfg = ingestion_cfg.restatement
        if (restate_cfg.enabled
                and _symbol_in_scope(symbol, restate_cfg.symbols)):
            start = date.fromisoformat(restate_cfg.start_date)
            end   = date.fromisoformat(restate_cfg.end_date)
            logger.info(f"[{symbol}/{interval}] RESTATEMENT {start} → {end}")
            return FetchPlan(
                symbol=symbol, interval=interval,
                mode=IngestionMode.RESTATEMENT,
                start_date=start, end_date=end,
                ingestion_type=IngestionMode.RESTATEMENT.ingestion_type,
                batch_size_days=batch_size, is_amendment_run=True,
                watermark=watermark,
            )

        # ── Priority 3: Explicit date range ──────────────────────────────
        override_cfg = ingestion_cfg.date_range_override
        if (override_cfg.enabled
                and _symbol_in_scope(symbol, override_cfg.symbols)):
            start = date.fromisoformat(override_cfg.start_date)
            end   = date.fromisoformat(override_cfg.end_date)
            logger.info(f"[{symbol}/{interval}] EXPLICIT DATE RANGE {start} → {end}")
            return FetchPlan(
                symbol=symbol, interval=interval,
                mode=IngestionMode.EXPLICIT_DATE_RANGE,
                start_date=start, end_date=end,
                ingestion_type=IngestionMode.EXPLICIT_DATE_RANGE.ingestion_type,
                batch_size_days=batch_size, watermark=watermark,
            )

        # ── Priority 4: History extension ─────────────────────────────────
        ext_cfg = ingestion_cfg.history_extension
        if (ext_cfg.enabled
                and watermark is not None
                and _symbol_in_scope(symbol, ext_cfg.symbols)):
            ext_start = date.fromisoformat(ext_cfg.extend_history_start)
            if ext_start < watermark.earliest_date:
                end = watermark.earliest_date - timedelta(days=1)
                logger.info(
                    f"[{symbol}/{interval}] HISTORY EXTENSION {ext_start} → {end}"
                )
                return FetchPlan(
                    symbol=symbol, interval=interval,
                    mode=IngestionMode.HISTORY_EXTENSION,
                    start_date=ext_start, end_date=end,
                    ingestion_type=IngestionMode.HISTORY_EXTENSION.ingestion_type,
                    batch_size_days=batch_size, watermark=watermark,
                )

        # ── Priority 5: Initial load ──────────────────────────────────────
        if watermark is None:
            start = self._get_initial_start(today, history_cfg, ticker_history_start)
            logger.info(f"[{symbol}/{interval}] INITIAL LOAD {start} → {today}")
            return FetchPlan(
                symbol=symbol, interval=interval,
                mode=IngestionMode.INITIAL_LOAD,
                start_date=start, end_date=today,
                ingestion_type=IngestionMode.INITIAL_LOAD.ingestion_type,
                batch_size_days=batch_size, watermark=None,
            )

        # ── Priority 6: Gap fill ──────────────────────────────────────────
        max_gap  = ingestion_cfg.max_gap_days
        gap_days = (today - watermark.latest_date).days
        if gap_days > max_gap:
            start = watermark.latest_date + timedelta(days=1)
            logger.warning(
                f"[{symbol}/{interval}] GAP FILL — {gap_days} days, "
                f"{start} → {today}"
            )
            return FetchPlan(
                symbol=symbol, interval=interval,
                mode=IngestionMode.GAP_FILL,
                start_date=start, end_date=today,
                ingestion_type=IngestionMode.GAP_FILL.ingestion_type,
                batch_size_days=batch_size, watermark=watermark,
            )

        # ── Priority 7: No-op ─────────────────────────────────────────────
        last_trading_day = self._get_last_trading_day(today)
        if watermark.latest_date >= last_trading_day:
            logger.debug(f"[{symbol}/{interval}] NO-OP — already up to date")
            return FetchPlan(
                symbol=symbol, interval=interval,
                mode=IngestionMode.NO_OP,
                start_date=today, end_date=today,
                ingestion_type=IngestionMode.NO_OP.ingestion_type,
                batch_size_days=batch_size, watermark=watermark,
            )

        # ── Priority 8: Incremental ───────────────────────────────────────
        buffer_days = history_cfg.amendment_buffer_days
        start = watermark.latest_date - timedelta(days=buffer_days)
        start = max(start, watermark.earliest_date)
        logger.info(
            f"[{symbol}/{interval}] INCREMENTAL {start} → {today} "
            f"(buffer={buffer_days}d)"
        )
        return FetchPlan(
            symbol=symbol, interval=interval,
            mode=IngestionMode.INCREMENTAL,
            start_date=start, end_date=today,
            ingestion_type=IngestionMode.INCREMENTAL.ingestion_type,
            batch_size_days=batch_size, watermark=watermark,
        )

    def _get_initial_start(self, today, history_cfg, ticker_history_start):
        if ticker_history_start is not None:
            return ticker_history_start
        try:
            lookback_years = history_cfg.lookback_years
            return today.replace(year=today.year - lookback_years)
        except AttributeError:
            lookback_days = history_cfg.lookback_days
            return today - timedelta(days=lookback_days)

    def _get_last_trading_day(self, as_of: date) -> date:
        d = as_of
        while not is_trading_day(d):
            d -= timedelta(days=1)
        return d

