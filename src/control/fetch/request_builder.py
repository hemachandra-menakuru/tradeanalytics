"""
FetchRequestBuilder — turns an IngestionPlanner FetchPlan into chunked
FetchRequest units (Contract v2).

Pure logic, no Spark, no I/O — fully unit-testable.

Chunk sizing is LOAD-TYPE-AWARE (backfill improvement, 2026-07-05):
  - INITIAL_LOAD / HISTORY_EXTENSION → backfill_chunk_days (default 365).
    IBKR pacing is charged per REQUEST, not per bar — one request returns a
    year of daily bars for the same ~2.8s as a month. 15y × 2000 tickers:
    366k requests (~11 days) at 30d chunks vs 30k (~23h) at 365d chunks.
  - INCREMENTAL / GAP_FILL / others → batch_size_days (default 30) — small
    windows, small chunks, fine retry granularity.
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import List, Optional

from src.control.fetch.models import FetchRequest

_BACKFILL_LOAD_TYPES = {"INITIAL_LOAD", "HISTORY_EXTENSION", "FORCE_RELOAD"}


class FetchRequestBuilder:
    """Builds chunked, fully-enriched FetchRequests for one instrument plan."""

    def __init__(
        self,
        raw_bucket: str,
        task_type: str = "FETCH_OHLCV",
        backfill_chunk_days: int = 365,
        requested_by: str = "fetch_planner_job",
        pipeline_version: str = "unknown",
    ):
        self._raw_bucket          = raw_bucket
        self._task_type           = task_type
        self._backfill_chunk_days = backfill_chunk_days
        self._requested_by        = requested_by
        self._pipeline_version    = pipeline_version

    def build(
        self,
        *,
        batch_id:      str,
        stream:        str,
        load_type:     str,
        bar_interval:  str,
        start_date:    date,
        end_date:      date,
        batch_size_days: int,
        # instrument enrichment (planner-resolved from reference tables)
        instrument_id: int,
        symbol:        str,
        vendor:        str,
        vendor_instrument_id: Optional[str] = None,
        asset_class:   str = "equity",
        security_type: str = "STK",
        exchange:      str = "SMART",
        exchange_mic:  Optional[str] = None,
        currency:      str = "USD",
        vendor_exchange: Optional[str] = None,
        # fetch behaviour (planner/stream config decides; agent obeys)
        what_to_show:  str = "TRADES",
        use_rth:       bool = True,
    ) -> List[FetchRequest]:
        if end_date < start_date:
            raise ValueError(
                f"end_date {end_date} before start_date {start_date} for {symbol}"
            )
        if batch_size_days < 1:
            raise ValueError(f"batch_size_days must be >= 1, got {batch_size_days}")

        chunk_days = (
            self._backfill_chunk_days
            if load_type.upper() in _BACKFILL_LOAD_TYPES
            else batch_size_days
        )

        ingest_partition = f"ingest_date={date.today().isoformat()}"
        land_to = f"s3://{self._raw_bucket}/{vendor}/ohlcv_{stream}/{ingest_partition}/"

        requests: List[FetchRequest] = []
        chunk_start = start_date
        seq = 0
        while chunk_start <= end_date:
            chunk_end = min(chunk_start + timedelta(days=chunk_days - 1), end_date)
            seq += 1
            requests.append(FetchRequest(
                request_key=f"{batch_id}_{vendor}_{symbol}_{seq:03d}",
                batch_id=batch_id,
                stream=stream,
                load_type=load_type,
                task_type=self._task_type,
                instrument_id=instrument_id,
                symbol=symbol,
                vendor=vendor,
                vendor_instrument_id=vendor_instrument_id,
                asset_class=asset_class,
                security_type=security_type,
                exchange=exchange,
                exchange_mic=exchange_mic,
                currency=currency,
                vendor_exchange=vendor_exchange,
                bar_interval=bar_interval,
                start_date=chunk_start,
                end_date=chunk_end,
                what_to_show=what_to_show,
                use_rth=use_rth,
                land_to=land_to,
                payload_format="ohlcv_json_v1",
                requested_by=self._requested_by,
                pipeline_version=self._pipeline_version,
            ))
            chunk_start = chunk_end + timedelta(days=1)

        return requests
