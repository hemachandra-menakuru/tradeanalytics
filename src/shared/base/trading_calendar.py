"""
TradeAnalytics Trading Calendar — Abstract Base Class
======================================================
Contract for any market calendar implementation.

Why this exists:
  The original ingestion_planner.py hardcoded US NYSE/NASDAQ holidays as a
  module-level set. That works for US equities but breaks the moment you add
  any non-US market (LSE, ASX, TSX) or a 24/7 market (crypto, FX).

  By defining this ABC, IngestionPlanner stays pure logic — it delegates
  all calendar questions to whichever implementation is injected.

Implementations:
  USEquityCalendar          → src/shared/calendar/us_equity_calendar.py
  ExchangeCalendarsCalendar → src/shared/calendar/exchange_calendars_calendar.py  (Phase 3)
  AlwaysOpenCalendar        → src/shared/calendar/always_open_calendar.py          (crypto/FX)

Usage:
    from src.shared.base.trading_calendar import TradingCalendar
    from src.shared.calendar.us_equity_calendar import USEquityCalendar

    calendar = USEquityCalendar()
    if calendar.is_trading_day(date(2026, 7, 4)):   # False — US Independence Day
        ...
    last = calendar.last_trading_day(date.today())  # most recent trading day
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import date
from typing import List


class TradingCalendar(ABC):
    """
    Abstract base class for market calendars.

    Each implementation handles one exchange or market type.
    IngestionPlanner receives a TradingCalendar instance and calls
    only these methods — never implementation-specific logic.

    exchange_mic parameter uses ISO 10383 codes:
        XNAS — NASDAQ
        XNYS — NYSE
        XASX — Australian Securities Exchange
        XLSE — London Stock Exchange
        "CRYPTO" — 24/7 (convention, not a real MIC)
        "FX"     — 24/5 Sunday–Friday (convention)
    """

    @abstractmethod
    def is_trading_day(self, d: date, exchange_mic: str = "XNAS") -> bool:
        """
        Returns True if the given date is a trading day for the exchange.

        Args:
            d:            The date to check.
            exchange_mic: ISO 10383 exchange identifier. Defaults to XNAS.

        Returns:
            True if markets are open on this date.
        """

    @abstractmethod
    def last_trading_day(self, as_of: date, exchange_mic: str = "XNAS") -> date:
        """
        Returns the most recent trading day on or before as_of.

        Used by IngestionPlanner to determine whether a watermark
        is up to date (watermark.latest_date >= last_trading_day).

        Args:
            as_of:        Reference date (usually today).
            exchange_mic: Exchange to check. Defaults to XNAS.

        Returns:
            Most recent trading day <= as_of.
        """

    @abstractmethod
    def trading_days_between(
        self,
        start: date,
        end: date,
        exchange_mic: str = "XNAS",
    ) -> List[date]:
        """
        Returns all trading days in [start, end] inclusive.

        Used for gap detection (expected bars vs actual bars).

        Args:
            start:        Start date (inclusive).
            end:          End date (inclusive).
            exchange_mic: Exchange to check. Defaults to XNAS.

        Returns:
            Sorted list of trading dates.
        """

    def count_trading_days(
        self,
        start: date,
        end: date,
        exchange_mic: str = "XNAS",
    ) -> int:
        """
        Count trading days between start and end (inclusive).
        Concrete — implemented via trading_days_between().
        """
        return len(self.trading_days_between(start, end, exchange_mic))
