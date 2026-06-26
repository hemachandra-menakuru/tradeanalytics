"""
WatermarkStore ABC — decouples ingestion logic from watermark storage backend.

Current implementation: WatermarkManager (Delta table in bronze schema).
Phase 2.5 target:       migrate to control.ingestion_watermark with instrument_id key.

Any new backend (e.g. Redis, Postgres) implements this ABC and drops in via config.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import date
from typing import Optional


class IngestionWatermarkRecord:
    """Value object for a single watermark entry."""

    def __init__(
        self,
        symbol: str,
        interval: str,
        earliest_date: date,
        latest_date: date,
        record_count: int,
        batch_id: str,
        mode: str,
        status: str,
    ):
        self.symbol        = symbol
        self.interval      = interval
        self.earliest_date = earliest_date
        self.latest_date   = latest_date
        self.record_count  = record_count
        self.batch_id      = batch_id
        self.mode          = mode
        self.status        = status


class WatermarkStore(ABC):
    """
    Read/write access to ingestion watermarks.

    Watermarks are ACTUAL STATE — never touch them manually.
    They are a system mirror of what has been fetched.
    """

    @abstractmethod
    def get_watermark(
        self,
        symbol: str,
        interval: str,
    ) -> Optional[IngestionWatermarkRecord]:
        """
        Return the current watermark for symbol + interval, or None if no data
        has been ingested yet (signals INITIAL_LOAD to IngestionPlanner).
        """

    @abstractmethod
    def update_watermark(
        self,
        symbol: str,
        interval: str,
        earliest_date: date,
        latest_date: date,
        record_count: int,
        batch_id: str,
        mode: str,
        status: str,
    ) -> None:
        """
        Upsert the watermark for symbol + interval.
        Called after every successful write — never on dry runs.
        """

    @abstractmethod
    def list_watermarks(self, interval: Optional[str] = None) -> list:
        """Return all watermark records, optionally filtered by interval."""
