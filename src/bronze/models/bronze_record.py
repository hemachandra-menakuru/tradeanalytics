"""
TradeAnalytics Bronze Layer Record Models
=========================================
Defines the standard OHLCV record structure for all Bronze tables.
ALL providers (IBKR, Yahoo, Polygon) must map their responses to these models.
NEVER fabricate values — use None if provider cannot supply a field.

Eight field groups:
  1. Identity          — who, what, when
  2. Raw Price         — exactly what provider gave us
  3. Adjusted Price    — split/dividend adjusted (from provider)
  4. Session Context   — market microstructure (VWAP, pre/post market)
  5. Corporate Actions — splits, dividends on this date
  6. Market Conditions — halts, trading status
  7. Data Quality      — validator output
  8. Pipeline Audit    — full traceability

Design principles:
  - Mutable by convention — use amend() to create amendments, never mutate directly
  - Validation at construction — __post_init__ enforces ALL invariants immediately
  - validate_identity() fully mirrors __post_init__ invariants — safe to call pre-write
  - has_corporate_action is always derived — never stale after amendments
  - Fail fast — unknown intervals, missing bar times, None prices all raise

Usage:
    from src.bronze.models.bronze_record import BronzeRecord, RecordInterval

    record = BronzeRecord(
        symbol="AAPL",
        date=date(2026, 6, 19),
        interval=RecordInterval.DAILY.value,
        source="ibkr",
        batch_id="batch_20260619_100500",
        pipeline_version="9ccf799",
        open=150.00,
        high=152.50,
        low=149.75,
        close=151.25,
        volume=1_000_000,
    )
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from enum import Enum
from typing import List, Optional, Set


# ── Enumerations ───────────────────────────────────────────────────────────────

class RecordInterval(str, Enum):
    """
    Supported data intervals.
    String enum so values work directly in Spark/Delta/SQL.
    """
    DAILY       = "1d"
    HOURLY      = "1h"
    FOUR_HOUR   = "4h"
    FIFTEEN_MIN = "15m"
    FIVE_MIN    = "5m"
    ONE_MIN     = "1m"

    @classmethod
    def values(cls) -> Set[str]:
        """All valid interval string values."""
        return {i.value for i in cls}

    @classmethod
    def intraday_values(cls) -> Set[str]:
        """All valid sub-daily interval string values."""
        return {i.value for i in cls if i != cls.DAILY}

    @classmethod
    def is_intraday(cls, interval: str) -> bool:
        """
        Returns True only for known sub-daily intervals.
        Raises ValueError for unknown intervals — fail fast.
        """
        if interval not in cls.values():
            raise ValueError(
                f"Unknown interval '{interval}'. "
                f"Valid intervals: {sorted(cls.values())}"
            )
        return interval in cls.intraday_values()

    @classmethod
    def get_table_tier(cls, interval: str) -> str:
        """
        Returns which Bronze table tier this interval belongs to.
        Raises ValueError for unknown intervals.
        """
        if interval not in cls.values():
            raise ValueError(
                f"Unknown interval '{interval}'. "
                f"Valid intervals: {sorted(cls.values())}"
            )
        if interval == cls.DAILY.value:
            return "daily"
        elif interval == cls.ONE_MIN.value:
            return "tick"
        else:
            return "intraday"

    @classmethod
    def get_unique_key_fields(cls, interval: str) -> List[str]:
        """
        Returns the fields that uniquely identify a record.
        Daily:          symbol + date + interval
        Intraday/Tick:  symbol + date + bar_time_utc + interval
        """
        base = ["symbol", "date", "interval"]
        if cls.is_intraday(interval):
            base.insert(2, "bar_time_utc")
        return base


class MarketSession(str, Enum):
    REGULAR     = "regular"
    PRE_MARKET  = "pre"
    POST_MARKET = "post"
    EXTENDED    = "extended"


class IngestionType(str, Enum):
    SCHEDULED   = "scheduled"
    BACKFILL    = "backfill"
    AMENDMENT   = "amendment"
    MANUAL      = "manual"


class DividendType(str, Enum):
    CASH        = "cash"
    SPECIAL     = "special"
    STOCK       = "stock"


class HaltReason(str, Enum):
    NEWS            = "news"
    VOLATILITY      = "volatility"
    REGULATORY      = "regulatory"
    CIRCUIT_BREAKER = "circuit_breaker"


class AmendmentReason(str, Enum):
    PROVIDER_CORRECTION = "provider_correction"
    RESTATEMENT         = "restatement"
    SPLIT_ADJUSTMENT    = "split_adjustment"
    DIVIDEND_ADJUSTMENT = "dividend_adjustment"


# Fields allowed to be updated via amend()
# System-controlled fields (record_version, batch_id, etc.) are NOT here
_AMENDABLE_FIELDS: Set[str] = {
    "open", "high", "low", "close", "volume",
    "adj_close", "adj_factor", "unadj_close",
    "vwap", "prev_close", "trade_count",
    "pre_market_high", "pre_market_low", "pre_market_close", "pre_market_volume",
    "post_market_high", "post_market_low", "post_market_close", "post_market_volume",
    "split_factor", "dividend_amount", "dividend_type",
    "is_ex_dividend_date", "is_ex_split_date",
    "trading_halt", "halt_reason", "shares_outstanding", "market_cap_at_date",
}


def _require_non_blank(value: any, field_name: str) -> None:
    """Raise ValueError if value is None, empty, or whitespace-only."""
    if value is None or not str(value).strip():
        raise ValueError(
            f"'{field_name}' is required and cannot be blank. "
            f"Got: {repr(value)}"
        )


def _require_non_none_number(value: any, field_name: str) -> None:
    """Raise ValueError if value is None or not a number."""
    if value is None:
        raise ValueError(
            f"'{field_name}' is required and cannot be None. "
            f"All raw price fields must be provided by the data provider."
        )
    if not isinstance(value, (int, float)):
        raise ValueError(
            f"'{field_name}' must be a number, got {type(value).__name__}: {repr(value)}"
        )


def _derive_has_corporate_action(
    split_factor: Optional[float],
    dividend_amount: float,
) -> bool:
    """
    Derive has_corporate_action from current field values.
    Always computed — never stale after amendments.
    """
    return split_factor is not None or dividend_amount > 0


# ── Main record model ──────────────────────────────────────────────────────────

@dataclass
class BronzeRecord:
    """
    Standard OHLCV record for Bronze Delta tables.

    Mutable by convention — use amend() to create amendments.
    Never mutate a record directly after construction.
    All monetary values in USD. All times in UTC unless suffixed _eastern.

    Invariants enforced at construction AND in validate_identity():
      - symbol, source, batch_id, pipeline_version must be non-blank strings
      - interval must be a known RecordInterval value
      - intraday records must have non-blank bar_time_utc
      - open, high, low, close, volume must be non-None numbers
      - has_corporate_action is always derived — never set directly
    """

    # ── GROUP 1: IDENTITY (required) ──────────────────────────────────────────
    symbol:             str
    date:               date
    interval:           str
    source:             str
    batch_id:           str
    pipeline_version:   str

    # ── GROUP 2: RAW PRICE (required — None raises at construction) ───────────
    open:               float
    high:               float
    low:                float
    close:              float
    volume:             int

    # ── GROUP 3: ADJUSTED PRICE ───────────────────────────────────────────────
    adj_close:          Optional[float] = None
    adj_factor:         Optional[float] = None
    unadj_close:        Optional[float] = None

    # ── GROUP 4: SESSION CONTEXT ──────────────────────────────────────────────
    exchange:           Optional[str]   = None
    currency:           str             = "USD"
    market_session:     str             = MarketSession.REGULAR.value
    trade_count:        Optional[int]   = None
    vwap:               Optional[float] = None
    prev_close:         Optional[float] = None
    session_open:       Optional[float] = None
    session_close:      Optional[float] = None
    pre_market_high:    Optional[float] = None
    pre_market_low:     Optional[float] = None
    pre_market_close:   Optional[float] = None
    pre_market_volume:  Optional[int]   = None
    post_market_high:   Optional[float] = None
    post_market_low:    Optional[float] = None
    post_market_close:  Optional[float] = None
    post_market_volume: Optional[int]   = None
    bar_time_utc:       Optional[str]   = None
    bar_time_eastern:   Optional[str]   = None
    bar_end_time_utc:   Optional[str]   = None
    timezone_source:    Optional[str]   = None

    # ── GROUP 5: CORPORATE ACTIONS ────────────────────────────────────────────
    # NOTE: has_corporate_action is ALWAYS derived in __post_init__
    # Do NOT set it directly — it will be overwritten
    has_corporate_action:   bool            = False  # derived — see __post_init__
    split_factor:           Optional[float] = None
    dividend_amount:        float           = 0.0
    dividend_type:          Optional[str]   = None
    is_ex_dividend_date:    bool            = False
    is_ex_split_date:       bool            = False

    # ── GROUP 6: MARKET CONDITIONS ────────────────────────────────────────────
    trading_halt:           bool            = False
    halt_reason:            Optional[str]   = None
    is_trading_day:         bool            = True
    shares_outstanding:     Optional[int]   = None
    market_cap_at_date:     Optional[float] = None

    # ── GROUP 7: DATA QUALITY ─────────────────────────────────────────────────
    data_quality_flag:      bool            = False
    data_quality_reasons:   List[str]       = field(default_factory=list)
    is_amended:             bool            = False
    amendment_reason:       Optional[str]   = None
    supersedes_batch:       Optional[str]   = None
    data_completeness_pct:  float           = 100.0
    record_version:         int             = 1

    # ── GROUP 8: PIPELINE AUDIT ───────────────────────────────────────────────
    ingested_at:            Optional[str]   = None
    ingested_by:            Optional[str]   = None
    fetch_attempt_count:    int             = 1
    fetch_duration_ms:      Optional[int]   = None
    data_as_of:             Optional[str]   = None
    ingestion_type:         str             = IngestionType.SCHEDULED.value
    source_version:         Optional[str]   = None

    def __post_init__(self):
        """
        Enforce ALL invariants at construction time.
        Raises ValueError immediately if any invariant is violated.
        validate_identity() mirrors these same checks for pre-write re-validation.
        """
        self._enforce_invariants()

        # Set defaults after validation
        if self.ingested_at is None:
            self.ingested_at = datetime.now(timezone.utc).isoformat()

        if self.unadj_close is None:
            self.unadj_close = self.close

        # Always derive has_corporate_action — never trust caller-supplied value
        self.has_corporate_action = _derive_has_corporate_action(
            self.split_factor, self.dividend_amount
        )

    def _enforce_invariants(self) -> None:
        """
        Single source of truth for all record invariants.
        Called by both __post_init__ and validate_identity().
        """
        # Required non-blank string fields
        _require_non_blank(self.symbol,           "symbol")
        _require_non_blank(self.source,           "source")
        _require_non_blank(self.batch_id,         "batch_id")
        _require_non_blank(self.pipeline_version, "pipeline_version")

        # Interval must be known
        if self.interval not in RecordInterval.values():
            raise ValueError(
                f"Unknown interval '{self.interval}'. "
                f"Valid intervals: {sorted(RecordInterval.values())}"
            )

        # Intraday records must have non-blank bar_time_utc
        if self.interval in RecordInterval.intraday_values():
            if not self.bar_time_utc or not str(self.bar_time_utc).strip():
                raise ValueError(
                    f"'bar_time_utc' is required and cannot be blank for "
                    f"intraday interval '{self.interval}'. "
                    f"Got: {repr(self.bar_time_utc)}"
                )

        # Raw price fields must be non-None numbers
        _require_non_none_number(self.open,   "open")
        _require_non_none_number(self.high,   "high")
        _require_non_none_number(self.low,    "low")
        _require_non_none_number(self.close,  "close")
        _require_non_none_number(self.volume, "volume")

    # ── Computed properties ───────────────────────────────────────────────────

    @property
    def table_tier(self) -> str:
        return RecordInterval.get_table_tier(self.interval)

    @property
    def unique_key(self) -> dict:
        """
        Fields that uniquely identify this record.
        Safe to call — bar_time_utc guaranteed non-None for intraday by invariants.
        """
        key = {
            "symbol":   self.symbol,
            "date":     str(self.date),
            "interval": self.interval,
        }
        if self.interval in RecordInterval.intraday_values():
            key["bar_time_utc"] = self.bar_time_utc
        return key

    @property
    def price_range(self) -> float:
        return self.high - self.low

    @property
    def daily_return_pct(self) -> Optional[float]:
        if self.prev_close and self.prev_close > 0:
            return ((self.close - self.prev_close) / self.prev_close) * 100
        return None

    @property
    def gap_pct(self) -> Optional[float]:
        if self.prev_close and self.prev_close > 0:
            return ((self.open - self.prev_close) / self.prev_close) * 100
        return None

    @property
    def is_gap_up(self) -> Optional[bool]:
        gap = self.gap_pct
        return gap > 0.5 if gap is not None else None

    @property
    def is_gap_down(self) -> Optional[bool]:
        gap = self.gap_pct
        return gap < -0.5 if gap is not None else None

    # ── Validation ────────────────────────────────────────────────────────────

    def validate_identity(self) -> None:
        """
        Full invariant check — mirrors __post_init__ exactly.
        Call before any write operation as a defensive check.

        Also re-derives has_corporate_action to ensure it is never stale.
        Raises ValueError if any invariant is violated.
        """
        self._enforce_invariants()

        # Re-derive has_corporate_action — ensure it is always current
        self.has_corporate_action = _derive_has_corporate_action(
            self.split_factor, self.dividend_amount
        )

    def compute_completeness(self) -> float:
        """
        Compute what % of expected fields have non-None values.
        Core fields weighted 80%, optional fields 20%.
        """
        core_fields = [
            "open", "high", "low", "close", "volume",
            "adj_close", "prev_close", "vwap",
        ]
        optional_fields = [
            "trade_count", "pre_market_volume", "post_market_volume",
            "shares_outstanding", "exchange",
        ]

        core_present = sum(
            1 for f in core_fields if getattr(self, f) is not None
        )
        optional_present = sum(
            1 for f in optional_fields if getattr(self, f) is not None
        )

        core_score     = (core_present / len(core_fields)) * 80
        optional_score = (optional_present / len(optional_fields)) * 20
        completeness   = round(core_score + optional_score, 1)

        self.data_completeness_pct = completeness
        return completeness

    # ── Conversion ────────────────────────────────────────────────────────────

    def to_dict(self) -> dict:
        """Convert to flat dict for Spark DataFrame creation."""
        import json

        return {
            # GROUP 1
            "symbol":               self.symbol,
            "date":                 str(self.date),
            "interval":             self.interval,
            "source":               self.source,
            "batch_id":             self.batch_id,
            "pipeline_version":     self.pipeline_version,
            # GROUP 2
            "open":                 self.open,
            "high":                 self.high,
            "low":                  self.low,
            "close":                self.close,
            "volume":               self.volume,
            # GROUP 3
            "adj_close":            self.adj_close,
            "adj_factor":           self.adj_factor,
            "unadj_close":          self.unadj_close,
            # GROUP 4
            "exchange":             self.exchange,
            "currency":             self.currency,
            "market_session":       self.market_session,
            "trade_count":          self.trade_count,
            "vwap":                 self.vwap,
            "prev_close":           self.prev_close,
            "session_open":         self.session_open,
            "session_close":        self.session_close,
            "pre_market_high":      self.pre_market_high,
            "pre_market_low":       self.pre_market_low,
            "pre_market_close":     self.pre_market_close,
            "pre_market_volume":    self.pre_market_volume,
            "post_market_high":     self.post_market_high,
            "post_market_low":      self.post_market_low,
            "post_market_close":    self.post_market_close,
            "post_market_volume":   self.post_market_volume,
            "bar_time_utc":         self.bar_time_utc,
            "bar_time_eastern":     self.bar_time_eastern,
            "bar_end_time_utc":     self.bar_end_time_utc,
            "timezone_source":      self.timezone_source,
            # GROUP 5
            "has_corporate_action": self.has_corporate_action,
            "split_factor":         self.split_factor,
            "dividend_amount":      self.dividend_amount,
            "dividend_type":        self.dividend_type,
            "is_ex_dividend_date":  self.is_ex_dividend_date,
            "is_ex_split_date":     self.is_ex_split_date,
            # GROUP 6
            "trading_halt":         self.trading_halt,
            "halt_reason":          self.halt_reason,
            "is_trading_day":       self.is_trading_day,
            "shares_outstanding":   self.shares_outstanding,
            "market_cap_at_date":   self.market_cap_at_date,
            # GROUP 7
            "data_quality_flag":        self.data_quality_flag,
            "data_quality_reasons":     json.dumps(self.data_quality_reasons),
            "is_amended":               self.is_amended,
            "amendment_reason":         self.amendment_reason,
            "supersedes_batch":         self.supersedes_batch,
            "data_completeness_pct":    self.data_completeness_pct,
            "record_version":           self.record_version,
            # GROUP 8
            "ingested_at":          self.ingested_at,
            "ingested_by":          self.ingested_by,
            "fetch_attempt_count":  self.fetch_attempt_count,
            "fetch_duration_ms":    self.fetch_duration_ms,
            "data_as_of":           self.data_as_of,
            "ingestion_type":       self.ingestion_type,
            "source_version":       self.source_version,
        }

    def amend(
        self,
        new_batch_id: str,
        reason: str,
        **updated_fields,
    ) -> "BronzeRecord":
        """
        Create an amendment record based on this record.
        Returns a NEW BronzeRecord — original is NEVER modified.
        has_corporate_action is always re-derived after field updates.

        Args:
            new_batch_id:     batch_id for the amendment run
            reason:           Must be a valid AmendmentReason value
            **updated_fields: Only fields in _AMENDABLE_FIELDS are allowed

        Raises:
            ValueError: if reason is not a valid AmendmentReason
            ValueError: if any updated_field is not in _AMENDABLE_FIELDS
        """
        # Validate reason
        valid_reasons = {r.value for r in AmendmentReason}
        if reason not in valid_reasons:
            raise ValueError(
                f"Invalid amendment reason '{reason}'. "
                f"Valid reasons: {sorted(valid_reasons)}"
            )

        # Validate field allowlist
        disallowed = set(updated_fields.keys()) - _AMENDABLE_FIELDS
        if disallowed:
            raise ValueError(
                f"Fields not allowed in amendments: {sorted(disallowed)}. "
                f"System-controlled fields cannot be overridden via amend(). "
                f"Amendable fields: {sorted(_AMENDABLE_FIELDS)}"
            )

        import dataclasses
        current_dict = dataclasses.asdict(self)
        current_dict.update(updated_fields)
        current_dict.update({
            "batch_id":             new_batch_id,
            "record_version":       self.record_version + 1,
            "is_amended":           True,
            "amendment_reason":     reason,
            "supersedes_batch":     self.batch_id,
            "ingestion_type":       IngestionType.AMENDMENT.value,
            "ingested_at":          datetime.now(timezone.utc).isoformat(),
            "data_quality_flag":    False,
            "data_quality_reasons": [],
            # has_corporate_action will be re-derived by __post_init__
            # so we exclude it from the dict — let it be recomputed
        })

        # Convert date back from string if dataclasses.asdict converted it
        if isinstance(current_dict["date"], str):
            from datetime import date as date_type
            current_dict["date"] = date_type.fromisoformat(current_dict["date"])

        # Remove has_corporate_action — will be re-derived in __post_init__
        current_dict.pop("has_corporate_action", None)

        return BronzeRecord(**current_dict)

    def __repr__(self) -> str:
        return (
            f"BronzeRecord("
            f"symbol={self.symbol}, "
            f"date={self.date}, "
            f"interval={self.interval}, "
            f"close={self.close}, "
            f"source={self.source}, "
            f"version={self.record_version}, "
            f"quality_flag={self.data_quality_flag}"
            f")"
        )


# ── Rejected record model ──────────────────────────────────────────────────────

@dataclass
class RejectedRecord:
    """
    Record that failed critical data quality checks.
    Written to bronze.market_data_rejected — never to the main table.
    Queryable, reprocessable, auditable.
    """
    symbol:             str
    date:               str
    interval:           str
    source:             str
    batch_id:           str
    rejected_rule:      str
    rejection_reason:   str
    raw_record:         str
    rejected_at:        str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )
    raw_open:           Optional[float] = None
    raw_high:           Optional[float] = None
    raw_low:            Optional[float] = None
    raw_close:          Optional[float] = None
    raw_volume:         Optional[int]   = None
    reprocessed:        bool            = False
    reprocessed_at:     Optional[str]   = None
    reprocessed_batch:  Optional[str]   = None
    pipeline_version:   Optional[str]   = None

    def to_dict(self) -> dict:
        return {
            "symbol":               self.symbol,
            "date":                 self.date,
            "interval":             self.interval,
            "source":               self.source,
            "batch_id":             self.batch_id,
            "rejected_rule":        self.rejected_rule,
            "rejection_reason":     self.rejection_reason,
            "raw_record":           self.raw_record,
            "rejected_at":          self.rejected_at,
            "raw_open":             self.raw_open,
            "raw_high":             self.raw_high,
            "raw_low":              self.raw_low,
            "raw_close":            self.raw_close,
            "raw_volume":           self.raw_volume,
            "reprocessed":          self.reprocessed,
            "reprocessed_at":       self.reprocessed_at,
            "reprocessed_batch":    self.reprocessed_batch,
            "pipeline_version":     self.pipeline_version,
        }

    def __repr__(self) -> str:
        return (
            f"RejectedRecord("
            f"symbol={self.symbol}, "
            f"date={self.date}, "
            f"rule={self.rejected_rule}, "
            f"reason={self.rejection_reason}"
            f")"
        )
