"""
OHLCVRecord TypedDict — enforced provider output contract.

All providers must return List[OHLCVRecord]. Using a TypedDict makes field
name typos surface as type errors rather than silent Spark schema mismatches.

Key names match the Bronze Delta schema ("symbol", "bar_date" — not "ticker", "trade_date").
"""

from __future__ import annotations

from typing import Optional
try:
    from typing import TypedDict
except ImportError:
    from typing_extensions import TypedDict  # Python < 3.8


class OHLCVRecord(TypedDict, total=False):
    # Identity (required)
    symbol:   str
    bar_date: str        # "YYYY-MM-DD"
    bar_interval: str        # "1d" | "1h" | "4h" | "5m" | "1m"
    source:   str        # provider name e.g. "ibkr"

    # Price (required — never None)
    open:   float
    high:   float
    low:    float
    close:  float
    volume: int

    # Adjusted price (None if provider cannot supply)
    adj_close:  Optional[float]
    adj_factor: Optional[float]

    # Session context
    prev_close:  Optional[float]
    vwap:        Optional[float]
    trade_count: Optional[int]
    exchange:    Optional[str]
    currency:    str            # default "USD"

    # Pre/post market
    pre_market_high:    Optional[float]
    pre_market_low:     Optional[float]
    pre_market_close:   Optional[float]
    pre_market_volume:  Optional[int]
    post_market_high:   Optional[float]
    post_market_low:    Optional[float]
    post_market_close:  Optional[float]
    post_market_volume: Optional[int]

    # Corporate actions
    split_factor:         Optional[float]
    dividend_amount:      float           # 0.0 if no dividend
    is_ex_dividend_date:  bool
    is_ex_split_date:     bool
    has_corporate_action: bool

    # Market conditions
    trading_halt:  bool
    is_trading_day: bool

    # Intraday only (None for daily)
    bar_time_utc:     Optional[str]
    bar_time_eastern: Optional[str]
    bar_end_time_utc: Optional[str]
    timezone_source:  Optional[str]

    # Pipeline audit (set by provider)
    fetch_duration_ms: Optional[int]
    data_as_of:        Optional[str]
    source_version:    Optional[str]
    is_amended:        bool
