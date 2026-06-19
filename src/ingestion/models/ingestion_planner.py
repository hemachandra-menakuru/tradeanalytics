"""
TradeAnalytics Ingestion Planner
==================================
Determines the correct IngestionMode and FetchPlan for each
symbol + interval combination based on:
  - Current watermark (what we already have)
  - Config settings (modes, overrides, thresholds)
  - Market calendar (holidays, weekends)

This is pure business logic with no I/O — easy to test.
The ingestion job calls the planner, then uses the FetchPlan to fetch data.

Usage:
    from src.ingestion.models.ingestion_planner import IngestionPlanner

    planner = IngestionPlanner(config)
    plan = planner.plan(
        symbol="AAPL",
        interval="1d",
        watermark=existing_watermark,  # None if first run
        as_of_date=date.today(),
    )
    print(plan)
    # FetchPlan(symbol=AAPL, mode=incremental, start=2026-06-16, end=2026-06-19, days=4)
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

# US market holidays 2026 (key NYSE closure dates)
_US_MARKET_HOLIDAYS_2026 = {
    date(2026, 1, 1),   # New Year's Day
    date(2026, 1, 19),  # MLK Day
    date(2026, 2, 16),  # Presidents Day
    date(2026, 4, 3),   # Good Friday
    date(2026, 5, 25),  # Memorial Day
    date(2026, 7, 3),   # Independence Day (observed)
    date(2026, 9, 7),   # Labor Day
    date(2026, 11, 26), # Thanksgiving
    date(2026, 12, 25), # Christmas
}


def is_trading_day(d: date) -> bool:
    """Returns True if d is a US market trading day."""
    if d.weekday() >= 5:
        return False
    if d in _US_MARKET_HOLIDAYS_2026:
        return False
    return True


def _symbol_in_scope(symbol: str, symbols_list) -> bool:
    """
    Returns True if symbol should be included in this operation.
    Empty list means ALL symbols. Non-empty list means only listed symbols.

    Args:
        symbol:       Ticker to check e.g. "AAPL"
        symbols_list: ConfigNode list or plain list from config
                      Empty = all symbols, non-empty = specific symbols only
    """
    if symbols_list is None:
        return True
    # Convert ConfigNode list or plain list to Python list
    try:
        scope = list(symbols_list) if symbols_list else []
    except TypeError:
        scope = []
    return len(scope) == 0 or symbol in scope


class IngestionPlanner:
    """
    Pure logic class — determines what to fetch for each symbol.
    No I/O, no Spark, no provider calls — fully testable.

    Priority order (highest to lowest):
      1. force_reload       — nuclear option, ignores watermarks
      2. restatement        — re-fetch range and amend changed records
      3. explicit_date_range — manual date range override
      4. history_extension  — extend backwards beyond earliest_date
      5. initial_load       — no watermark exists (first run)
      6. gap_fill           — gap > max_gap_days threshold
      7. incremental        — normal daily update
      8. no_op              — already up to date or non-trading day

    All config modes support symbols: [] scoping:
      symbols: []             → applies to ALL active symbols
      symbols: ["AAPL"]       → applies ONLY to AAPL
      symbols: ["AAPL","MSFT"]→ applies ONLY to AAPL and MSFT
    """

    def __init__(self, config: ConfigNode):
        self._config = config

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
            symbol:               Ticker symbol e.g. "AAPL"
            interval:             Data interval e.g. "1d"
            watermark:            Current watermark (None if first run)
            as_of_date:           Date to plan as of (default: today)
            ticker_history_start: Per-ticker history start from ref_tickers
                                  Overrides global lookback_years for initial load

        Returns:
            FetchPlan with mode, start_date, end_date, batch_size
        """
        today         = as_of_date or date.today()
        ingestion_cfg = self._config.data.ingestion
        interval_cfg  = self._get_interval_config(interval)
        batch_size    = ingestion_cfg.incremental.batch_size_days

        # ── Priority 1: Force reload ───────────────────────────────────────
        # Applies to specific symbols or all symbols (symbols: [])
        # Requires BOTH enabled=true AND confirm=true (safety guard)
        force_cfg = ingestion_cfg.force_reload
        if (force_cfg.enabled
                and force_cfg.confirm
                and _symbol_in_scope(symbol, force_cfg.symbols)):
            start = self._get_initial_start(
                today, interval_cfg, ticker_history_start
            )
            logger.warning(
                f"[{symbol}/{interval}] FORCE RELOAD — "
                f"fetching {start} → {today}"
            )
            return FetchPlan(
                symbol=symbol, interval=interval,
                mode=IngestionMode.FORCE_RELOAD,
                start_date=start, end_date=today,
                ingestion_type=IngestionMode.FORCE_RELOAD.ingestion_type,
                batch_size_days=batch_size,
                watermark=watermark,
            )

        # ── Priority 2: Restatement ───────────────────────────────────────
        # Re-fetch a date range and create amendments for changed records
        # Use for: provider corrections, split adjustments, dividend restatements
        restate_cfg = ingestion_cfg.restatement
        if (restate_cfg.enabled
                and _symbol_in_scope(symbol, restate_cfg.symbols)):
            start = date.fromisoformat(restate_cfg.start_date)
            end   = date.fromisoformat(restate_cfg.end_date)
            logger.info(
                f"[{symbol}/{interval}] RESTATEMENT — "
                f"re-fetching {start} → {end}"
            )
            return FetchPlan(
                symbol=symbol, interval=interval,
                mode=IngestionMode.RESTATEMENT,
                start_date=start, end_date=end,
                ingestion_type=IngestionMode.RESTATEMENT.ingestion_type,
                batch_size_days=batch_size,
                is_amendment_run=True,
                watermark=watermark,
            )

        # ── Priority 3: Explicit date range ──────────────────────────────
        # Manual override for any date range — use for gap fills, audits
        override_cfg = ingestion_cfg.date_range_override
        if (override_cfg.enabled
                and _symbol_in_scope(symbol, override_cfg.symbols)):
            start = date.fromisoformat(override_cfg.start_date)
            end   = date.fromisoformat(override_cfg.end_date)
            logger.info(
                f"[{symbol}/{interval}] EXPLICIT DATE RANGE — "
                f"fetching {start} → {end}"
            )
            return FetchPlan(
                symbol=symbol, interval=interval,
                mode=IngestionMode.EXPLICIT_DATE_RANGE,
                start_date=start, end_date=end,
                ingestion_type=IngestionMode.EXPLICIT_DATE_RANGE.ingestion_type,
                batch_size_days=batch_size,
                watermark=watermark,
            )

        # ── Priority 4: History extension (extend backwards) ─────────────
        # Fetches data OLDER than what we already have
        # Example: have 2016→today, want to extend back to 2010
        # Only applies if extend_history_start < watermark.earliest_date
        ext_cfg = ingestion_cfg.history_extension
        if (ext_cfg.enabled
                and watermark is not None
                and _symbol_in_scope(symbol, ext_cfg.symbols)):
            ext_start = date.fromisoformat(ext_cfg.extend_history_start)
            if ext_start < watermark.earliest_date:
                end = watermark.earliest_date - timedelta(days=1)
                logger.info(
                    f"[{symbol}/{interval}] HISTORY EXTENSION — "
                    f"extending backwards: {ext_start} → {end}"
                )
                return FetchPlan(
                    symbol=symbol, interval=interval,
                    mode=IngestionMode.HISTORY_EXTENSION,
                    start_date=ext_start, end_date=end,
                    ingestion_type=IngestionMode.HISTORY_EXTENSION.ingestion_type,
                    batch_size_days=batch_size,
                    watermark=watermark,
                )

        # ── Priority 5: Initial load (no watermark) ───────────────────────
        # First ever run for this symbol + interval
        # Per-ticker history_start overrides global lookback_years
        if watermark is None:
            start = self._get_initial_start(
                today, interval_cfg, ticker_history_start
            )
            logger.info(
                f"[{symbol}/{interval}] INITIAL LOAD — "
                f"fetching {start} → {today}"
            )
            return FetchPlan(
                symbol=symbol, interval=interval,
                mode=IngestionMode.INITIAL_LOAD,
                start_date=start, end_date=today,
                ingestion_type=IngestionMode.INITIAL_LOAD.ingestion_type,
                batch_size_days=batch_size,
                watermark=None,
            )

        # ── Priority 6: Gap fill ──────────────────────────────────────────
        # Gap larger than threshold — treat as backfill, not incremental
        max_gap = ingestion_cfg.incremental.max_gap_days
        gap_days = (today - watermark.latest_date).days

        if gap_days > max_gap:
            start = watermark.latest_date + timedelta(days=1)
            logger.warning(
                f"[{symbol}/{interval}] GAP FILL — "
                f"{gap_days} day gap detected, "
                f"fetching {start} → {today}"
            )
            return FetchPlan(
                symbol=symbol, interval=interval,
                mode=IngestionMode.GAP_FILL,
                start_date=start, end_date=today,
                ingestion_type=IngestionMode.GAP_FILL.ingestion_type,
                batch_size_days=batch_size,
                watermark=watermark,
            )

        # ── Priority 7: No-op check ───────────────────────────────────────
        # Already up to date — watermark >= last trading day
        last_trading_day = self._get_last_trading_day(today)
        if watermark.latest_date >= last_trading_day:
            logger.debug(
                f"[{symbol}/{interval}] NO-OP — "
                f"watermark={watermark.latest_date}, "
                f"last_trading_day={last_trading_day}"
            )
            return FetchPlan(
                symbol=symbol, interval=interval,
                mode=IngestionMode.NO_OP,
                start_date=today, end_date=today,
                ingestion_type=IngestionMode.NO_OP.ingestion_type,
                batch_size_days=batch_size,
                watermark=watermark,
            )

        # ── Priority 8: Incremental (normal daily) ────────────────────────
        # Re-fetch last N days as buffer for provider amendments
        buffer_days = interval_cfg.amendment_buffer_days
        start = watermark.latest_date - timedelta(days=buffer_days)
        # Never go before earliest_date
        start = max(start, watermark.earliest_date)

        logger.info(
            f"[{symbol}/{interval}] INCREMENTAL — "
            f"watermark={watermark.latest_date}, "
            f"fetching {start} → {today} "
            f"(buffer={buffer_days} days)"
        )
        return FetchPlan(
            symbol=symbol, interval=interval,
            mode=IngestionMode.INCREMENTAL,
            start_date=start, end_date=today,
            ingestion_type=IngestionMode.INCREMENTAL.ingestion_type,
            batch_size_days=batch_size,
            watermark=watermark,
        )

    def _get_initial_start(
        self,
        today: date,
        interval_cfg,
        ticker_history_start: Optional[date],
    ) -> date:
        """
        Determine start date for initial load.
        Per-ticker history_start always takes precedence over global config.
        """
        if ticker_history_start is not None:
            return ticker_history_start
        try:
            lookback_years = interval_cfg.lookback_years
            return today.replace(year=today.year - lookback_years)
        except AttributeError:
            lookback_days = interval_cfg.lookback_days
            return today - timedelta(days=lookback_days)

    def _get_interval_config(self, interval: str):
        """Return the config section for this interval tier."""
        if interval == "1d":
            return self._config.data.intervals.daily
        elif interval in ("1m",):
            return self._config.data.intervals.tick
        else:
            return self._config.data.intervals.intraday

    def _get_last_trading_day(self, as_of: date) -> date:
        """Return most recent trading day on or before as_of."""
        d = as_of
        while not is_trading_day(d):
            d -= timedelta(days=1)
        return d
