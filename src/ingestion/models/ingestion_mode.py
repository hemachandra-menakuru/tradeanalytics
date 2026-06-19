"""
TradeAnalytics Ingestion Mode Models
=====================================
Defines all possible ingestion modes and the watermark model.
The ingestion job uses these to determine what date range to fetch
for each symbol + interval combination.

Ingestion modes (in priority order):
  FORCE_RELOAD        — ignore watermarks, fetch everything fresh
  RESTATEMENT         — re-fetch range and create amendments if changed
  EXPLICIT_DATE_RANGE — fetch specific date range regardless of watermark
  HISTORY_EXTENSION   — extend backwards beyond current earliest_date
  INITIAL_LOAD        — first ever run, no watermark exists
  GAP_FILL            — gap > max_gap_days, treat as backfill
  INCREMENTAL         — normal daily run, fetch since last watermark
  NO_OP               — nothing to do (holiday, weekend, up to date)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from enum import Enum
from typing import Optional


class IngestionMode(str, Enum):
    """
    How the ingestion job should determine the fetch date range.
    Higher priority modes override lower priority ones.
    """
    FORCE_RELOAD        = "force_reload"        # nuclear option
    RESTATEMENT         = "restatement"         # re-fetch + amend
    EXPLICIT_DATE_RANGE = "explicit_date_range" # manual override
    HISTORY_EXTENSION   = "history_extension"   # extend backwards
    INITIAL_LOAD        = "initial_load"        # first run
    GAP_FILL            = "gap_fill"            # large gap
    INCREMENTAL         = "incremental"         # normal daily
    NO_OP               = "no_op"              # nothing to do

    @property
    def ingestion_type(self) -> str:
        """Maps mode to the ingestion_type field stored in Bronze records."""
        mapping = {
            self.FORCE_RELOAD:        "backfill",
            self.RESTATEMENT:         "amendment",
            self.EXPLICIT_DATE_RANGE: "backfill",
            self.HISTORY_EXTENSION:   "backfill",
            self.INITIAL_LOAD:        "backfill",
            self.GAP_FILL:            "backfill",
            self.INCREMENTAL:         "scheduled",
            self.NO_OP:               "scheduled",
        }
        return mapping[self]

    @property
    def is_backfill(self) -> bool:
        return self.ingestion_type == "backfill"

    @property
    def description(self) -> str:
        descriptions = {
            self.FORCE_RELOAD:        "Force full reload — ignoring all watermarks",
            self.RESTATEMENT:         "Restatement — re-fetch and amend changed records",
            self.EXPLICIT_DATE_RANGE: "Explicit date range override",
            self.HISTORY_EXTENSION:   "Extending history backwards",
            self.INITIAL_LOAD:        "Initial load — no prior data exists",
            self.GAP_FILL:            "Gap fill — catching up after outage",
            self.INCREMENTAL:         "Incremental — normal daily update",
            self.NO_OP:               "No operation — up to date or non-trading day",
        }
        return descriptions[self]


@dataclass
class IngestionWatermark:
    """
    Tracks ingestion progress per symbol + interval.
    Stored in handh_trade.bronze.ingestion_watermark Delta table.

    Tracks BOTH ends of the date range:
      earliest_date — how far back we have data (for backward extension)
      latest_date   — how current our data is (for incremental updates)
    """
    symbol:             str
    interval:           str
    earliest_date:      date                # oldest record we have
    latest_date:        date                # most recent record we have
    record_count:       int                 # total records in Bronze
    last_run_at:        str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )
    last_batch_id:      Optional[str]   = None
    last_mode:          Optional[str]   = None  # IngestionMode value
    last_run_status:    str             = "success"  # success | partial | failed
    error_message:      Optional[str]   = None

    def to_dict(self) -> dict:
        return {
            "symbol":           self.symbol,
            "interval":         self.interval,
            "earliest_date":    str(self.earliest_date),
            "latest_date":      str(self.latest_date),
            "record_count":     self.record_count,
            "last_run_at":      self.last_run_at,
            "last_batch_id":    self.last_batch_id,
            "last_mode":        self.last_mode,
            "last_run_status":  self.last_run_status,
            "error_message":    self.error_message,
        }

    def __repr__(self) -> str:
        return (
            f"IngestionWatermark("
            f"symbol={self.symbol}, "
            f"interval={self.interval}, "
            f"earliest={self.earliest_date}, "
            f"latest={self.latest_date}, "
            f"records={self.record_count}"
            f")"
        )


@dataclass
class FetchPlan:
    """
    The resolved fetch plan for one symbol + interval.
    Produced by IngestionPlanner, consumed by ingestion job.
    """
    symbol:             str
    interval:           str
    mode:               IngestionMode
    start_date:         date
    end_date:           date
    ingestion_type:     str              # from mode.ingestion_type
    batch_size_days:    int = 30         # chunk large ranges into batches
    is_amendment_run:   bool = False     # True for restatement mode
    watermark:          Optional[IngestionWatermark] = None

    @property
    def date_range_days(self) -> int:
        return (self.end_date - self.start_date).days + 1

    @property
    def estimated_batches(self) -> int:
        return max(1, self.date_range_days // self.batch_size_days)

    def __repr__(self) -> str:
        return (
            f"FetchPlan("
            f"symbol={self.symbol}, "
            f"mode={self.mode.value}, "
            f"start={self.start_date}, "
            f"end={self.end_date}, "
            f"days={self.date_range_days}, "
            f"batches={self.estimated_batches}"
            f")"
        )
