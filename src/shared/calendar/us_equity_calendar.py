"""
TradeAnalytics US Equity Trading Calendar
==========================================
Implements TradingCalendar for US equity markets (NYSE / NASDAQ).

Holiday list covers 2010–2040.
Source: NYSE holiday schedule (historical + projected).

Phase 3 upgrade path:
  Replace with ExchangeCalendarsCalendar backed by the
  `exchange_calendars` Python library, which handles DST shifts,
  half-days (e.g. day after Thanksgiving), and early closures.
  The switch requires zero changes to IngestionPlanner — just pass
  a different TradingCalendar instance at construction time.

Usage:
    from src.shared.calendar.us_equity_calendar import USEquityCalendar

    cal = USEquityCalendar()
    cal.is_trading_day(date(2026, 7, 4))         # False
    cal.last_trading_day(date(2026, 7, 4))        # date(2026, 7, 3)
    cal.trading_days_between(date(2026,7,1), date(2026,7,7))
    # [date(2026,7,1), date(2026,7,2), date(2026,7,3), date(2026,7,7)]
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import List

from src.shared.base.trading_calendar import TradingCalendar


# ── US Market Holidays 2010–2040 ───────────────────────────────────────────────
# Source: NYSE holiday schedule (historical + projected).
# Extracted from src/bronze/models/ingestion_planner.py on 2026-06-26.
_US_MARKET_HOLIDAYS: frozenset = frozenset({
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
})


class USEquityCalendar(TradingCalendar):
    """
    Trading calendar for US equity markets (NYSE / NASDAQ).

    Uses a static holiday set covering 2010–2040.
    Weekend = Saturday + Sunday.

    Note: exchange_mic parameter is accepted but ignored — this calendar
    applies uniformly to all US exchanges. When you need per-exchange
    precision (e.g. NASDAQ vs NYSE different closures), use
    ExchangeCalendarsCalendar instead.
    """

    def is_trading_day(self, d: date, exchange_mic: str = "XNAS") -> bool:
        """Returns True if d is a US equity trading day."""
        if d.weekday() >= 5:            # Saturday=5, Sunday=6
            return False
        if d in _US_MARKET_HOLIDAYS:
            return False
        return True

    def last_trading_day(self, as_of: date, exchange_mic: str = "XNAS") -> date:
        """Returns the most recent trading day on or before as_of."""
        d = as_of
        while not self.is_trading_day(d):
            d -= timedelta(days=1)
        return d

    def trading_days_between(
        self,
        start: date,
        end: date,
        exchange_mic: str = "XNAS",
    ) -> List[date]:
        """Returns all US trading days in [start, end] inclusive, sorted."""
        if start > end:
            return []
        result = []
        d = start
        while d <= end:
            if self.is_trading_day(d):
                result.append(d)
            d += timedelta(days=1)
        return result


# ── Module-level convenience function ─────────────────────────────────────────
# Backward-compatible shim so any code that imported is_trading_day directly
# from ingestion_planner still works during the transition period.
_default_calendar = USEquityCalendar()

def is_trading_day(d: date) -> bool:
    """Backward-compatible convenience wrapper. Use USEquityCalendar directly."""
    return _default_calendar.is_trading_day(d)
