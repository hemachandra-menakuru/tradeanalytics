"""
FetchRequestBuilder — turns an IngestionPlanner FetchPlan into chunked
FetchRequest units.

Pure logic, no Spark, no I/O — fully unit-testable. Chunking (splitting a long
date range into batch_size_days slices) lives HERE, not in the agent: if chunk
4 of 6 fails overnight, chunks 1-3 are already landed and only the failed slice
is retried. The agent stays a dumb courier.
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import List

from src.control.fetch.models import FetchRequest


class FetchRequestBuilder:
    """Builds chunked FetchRequests from a plan for one instrument."""

    def __init__(self, raw_bucket: str, task_type: str = "FETCH_OHLCV"):
        self._raw_bucket = raw_bucket
        self._task_type  = task_type

    def build(
        self,
        *,
        batch_id:      str,
        instrument_id: int,
        symbol:        str,
        vendor:        str,
        stream:        str,
        bar_interval:  str,
        start_date:    date,
        end_date:      date,
        load_type:     str,
        batch_size_days: int,
    ) -> List[FetchRequest]:
        """Split [start_date, end_date] into batch_size_days chunks."""
        if end_date < start_date:
            raise ValueError(
                f"end_date {end_date} before start_date {start_date} for {symbol}"
            )
        if batch_size_days < 1:
            raise ValueError(f"batch_size_days must be >= 1, got {batch_size_days}")

        requests: List[FetchRequest] = []
        chunk_start = start_date
        seq = 0

        while chunk_start <= end_date:
            chunk_end = min(
                chunk_start + timedelta(days=batch_size_days - 1),
                end_date,
            )
            seq += 1
            ingest_partition = f"ingest_date={date.today().isoformat()}"
            requests.append(FetchRequest(
                # vendor in the key: same symbol via two vendors must never
                # collide in request_keys or landed filenames
                request_key=f"{batch_id}_{vendor}_{symbol}_{seq:03d}",
                batch_id=batch_id,
                instrument_id=instrument_id,
                symbol=symbol,
                vendor=vendor,
                stream=stream,
                bar_interval=bar_interval,
                start_date=chunk_start,
                end_date=chunk_end,
                load_type=load_type,
                task_type=self._task_type,
                land_to=(
                    f"s3://{self._raw_bucket}/{vendor}/ohlcv_{stream}/"
                    f"{ingest_partition}/"
                ),
            ))
            chunk_start = chunk_end + timedelta(days=1)

        return requests
