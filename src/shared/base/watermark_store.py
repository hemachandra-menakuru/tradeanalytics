"""
WatermarkStore ABC — decouples ingestion logic from watermark storage backend.

Phase 2 (old): WatermarkManager, keyed by symbol + interval, bronze schema.
Phase 2.5+:    DeltaWatermarkStore, keyed by instrument_id + stream, control schema.

Any new backend (e.g. Redis, Postgres) implements this ABC and drops in via config.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import date
from typing import List, Optional


class IngestionWatermarkRecord:
    """Value object for a single watermark entry."""

    def __init__(
        self,
        instrument_id: int,
        stream: str,
        earliest_date: date,
        latest_date: date,
        record_count: int,
        batch_id: str,
        mode: str,
        status: str,
        consecutive_failures: int = 0,
        last_error_message: Optional[str] = None,
        interval: str = "",
        vendor: Optional[str] = None,
    ):
        self.instrument_id        = instrument_id
        self.stream               = stream
        self.bar_interval         = interval
        self.earliest_date        = earliest_date
        self.latest_date          = latest_date
        self.record_count         = record_count
        self.batch_id             = batch_id
        self.mode                 = mode
        self.status               = status
        self.consecutive_failures = consecutive_failures
        self.last_error_message   = last_error_message
        self.vendor               = vendor


class WatermarkStore(ABC):
    """
    Read/write access to ingestion watermarks.

    Watermarks are ACTUAL STATE — never touch them manually.
    They are a system mirror of what has been fetched per instrument.
    """

    @abstractmethod
    def get_watermark(
        self,
        instrument_id: int,
        stream: str,
    ) -> Optional[IngestionWatermarkRecord]:
        """
        Return the current watermark for instrument_id + stream.
        Returns None if no data has been ingested yet → signals INITIAL_LOAD.
        """

    @abstractmethod
    def update_watermark(
        self,
        instrument_id: int,
        stream: str,
        earliest_date: date,
        latest_date: date,
        record_count: int,
        batch_id: str,
        mode: str,
        status: str,
        error_message: Optional[str] = None,
        interval: str = "",
        vendor: Optional[str] = None,
    ) -> None:
        """
        Upsert the watermark for instrument_id + stream.
        Called after every write — never on dry runs.
        """

    @abstractmethod
    def list_watermarks(self, stream: Optional[str] = None) -> List[IngestionWatermarkRecord]:
        """Return all watermark records, optionally filtered by stream."""

    def update_watermarks_bulk(self, entries: List[dict]) -> None:
        """Upsert many watermarks in one shot. Default implementation loops
        update_watermark (correct, unoptimised) so existing backends keep working;
        Delta overrides it with a single set-based MERGE.

        Each entry: instrument_id, stream, interval, vendor, batch_id, mode,
        run_min_date (date), run_max_date (date), rows_written (int).
        Dates only ever widen; rows_written is the delta appended THIS run.
        """
        for e in entries:
            self.update_watermark(
                instrument_id=e["instrument_id"], stream=e["stream"],
                earliest_date=e["run_min_date"], latest_date=e["run_max_date"],
                record_count=e["rows_written"], batch_id=e["batch_id"],
                mode=e["mode"], status="success",
                interval=e.get("interval", ""), vendor=e.get("vendor"),
            )
