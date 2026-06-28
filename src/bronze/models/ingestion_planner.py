
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

    # Inject a custom calendar (e.g. for non-US markets):
    from src.shared.calendar.us_equity_calendar import USEquityCalendar
    planner = IngestionPlanner(config, calendar=USEquityCalendar())
"""

from __future__ import annotations

import logging
from datetime import date, timedelta
from typing import Optional

from src.shared.config.config_loader import ConfigNode
from src.shared.base.trading_calendar import TradingCalendar
from src.shared.base.universe_reader import InstrumentInfo
from src.shared.base.watermark_store import IngestionWatermarkRecord
from src.bronze.models.ingestion_mode import (
    IngestionMode, IngestionWatermark, FetchPlan
)

logger = logging.getLogger(__name__)


# ── Backward-compatibility shim ────────────────────────────────────────────────
# Tests and any code that imported is_trading_day from this module still work.
# New code should use USEquityCalendar directly.
def is_trading_day(d: date) -> bool:
    """Backward-compat wrapper. Use USEquityCalendar.is_trading_day() for new code."""
    from src.shared.calendar.us_equity_calendar import is_trading_day as _impl
    return _impl(d)


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

    def __init__(
        self,
        config: ConfigNode,
        stream_name: str = "daily",
        calendar: Optional[TradingCalendar] = None,
    ):
        self._config      = config
        self._stream_name = stream_name
        # Stream config root: config.daily / config.intraday / config.tick
        self._stream_cfg  = getattr(config, stream_name)

        # Lazy import to avoid circular deps — only instantiated if no calendar injected
        if calendar is None:
            from src.shared.calendar.us_equity_calendar import USEquityCalendar
            calendar = USEquityCalendar()
        self._calendar: TradingCalendar = calendar

    def plan(
        self,
        symbol: str = "",
        interval: str = "",
        watermark=None,
        as_of_date: Optional[date] = None,
        ticker_history_start: Optional[date] = None,
        instrument: Optional[InstrumentInfo] = None,
        stream: Optional[str] = None,
    ) -> FetchPlan:
        """
        Determine the fetch plan for an instrument.

        Supports two calling conventions:

        Table-driven (Phase 2.5+):
            plan(instrument=InstrumentInfo, stream="daily", watermark=IngestionWatermarkRecord)

        Legacy (backward compat for existing tests):
            plan(symbol="AAPL", interval="1d", watermark=IngestionWatermark)

        Returns:
            FetchPlan with mode, start_date, end_date, batch_size, instrument_id
        """
        # Resolve inputs — table-driven takes priority over legacy params
        if instrument is not None:
            symbol               = instrument.symbol
            ticker_history_start = instrument.effective_history_start
            instrument_id        = instrument.instrument_id
        else:
            instrument_id = None

        if stream is not None:
            interval = self._stream_cfg.intervals[0] if not interval else interval

        # Normalise watermark — both IngestionWatermark and IngestionWatermarkRecord
        # expose earliest_date and latest_date, so the logic below works with either.
        # IngestionWatermarkRecord additionally has .status for SKIP detection.
        if watermark is not None and isinstance(watermark, IngestionWatermarkRecord):
            if watermark.status == "suspended":
                logger.info(f"[{symbol}] SKIP — watermark status=suspended")
                today      = as_of_date or date.today()
                batch_size = self._stream_cfg.ingestion.batch_size_days
                return FetchPlan(
                    symbol=symbol, interval=interval,
                    mode=IngestionMode.NO_OP,
                    start_date=today, end_date=today,
                    ingestion_type=IngestionMode.NO_OP.ingestion_type,
                    batch_size_days=batch_size,
                    instrument_id=instrument_id,
                )

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
                instrument_id=instrument_id,
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
                watermark=watermark, instrument_id=instrument_id,
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
                instrument_id=instrument_id,
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
                    instrument_id=instrument_id,
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
                instrument_id=instrument_id,
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
                instrument_id=instrument_id,
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
                instrument_id=instrument_id,
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
            instrument_id=instrument_id,
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
        return self._calendar.last_trading_day(as_of)

