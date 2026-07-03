"""
FetchPlannerJob — Plane-1 side of Bronze ingestion (Two-Plane Architecture).

For each active instrument:
  1. Read actual state (control.ingestion_watermark)
  2. IngestionPlanner derives load type + date range (desired vs actual)
  3. Skip NO_OP; skip instruments with in-flight requests (PENDING/LANDED)
  4. Chunk the range (FetchRequestBuilder) and persist
     (FetchRequestRepository → Delta rows + S3 manifests)

The EC2 bridge agent consumes the manifests; the ingestion job reconciles.
This job never fetches data and never needs network egress.
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timezone
from typing import List, Optional

from src.shared.config.config_loader import ConfigNode
from src.bronze.models.ingestion_mode import IngestionMode
from src.bronze.models.ingestion_planner import IngestionPlanner
from src.control.fetch.request_builder import FetchRequestBuilder
from src.control.fetch.request_repository import FetchRequestRepository
from src.control.watermark.delta_watermark_store import DeltaWatermarkStore
from src.reference.readers.delta_universe_reader import DeltaUniverseReader

logger = logging.getLogger(__name__)


class FetchPlannerJob:

    def __init__(
        self,
        config: ConfigNode,
        spark,
        fs_put,
        stream_name: str = "daily",
        vendor: str = "ibkr",
    ):
        self._config      = config
        self._spark       = spark
        self._stream_name = stream_name
        self._stream_cfg  = getattr(config, stream_name)
        self._vendor      = vendor

        catalog          = config.databricks.catalog
        raw_bucket       = config.aws.s3.raw
        self._catalog    = catalog

        self._universe   = DeltaUniverseReader(mode="spark", spark=spark, catalog=catalog)
        self._watermarks = DeltaWatermarkStore(mode="spark", spark=spark, catalog=catalog)
        self._planner    = IngestionPlanner(config, stream_name)
        self._builder    = FetchRequestBuilder(raw_bucket=raw_bucket)
        self._repo       = FetchRequestRepository(
            spark=spark, catalog=catalog, raw_bucket=raw_bucket, fs_put=fs_put,
        )

    def run(
        self,
        symbols: Optional[List[str]] = None,
        as_of_date: Optional[date] = None,
        dry_run: bool = False,
    ) -> dict:
        """Plan and emit fetch requests. Returns a summary dict."""
        today    = as_of_date or date.today()
        batch_id = f"batch_{self._stream_name}_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"
        interval = self._stream_cfg.intervals[0]

        instruments = self._universe.get_active_instruments(symbols=symbols or None)
        in_flight   = self._in_flight_instrument_ids()

        emitted, skipped_noop, skipped_inflight = [], [], []

        for inst in instruments:
            if inst.instrument_id in in_flight:
                skipped_inflight.append(inst.symbol)
                continue

            watermark = self._watermarks.get_watermark(inst.instrument_id, self._stream_name)
            plan = self._planner.plan(
                instrument=inst,
                stream=self._stream_name,
                watermark=watermark,
                as_of_date=today,
            )

            if plan.mode == IngestionMode.NO_OP:
                skipped_noop.append(inst.symbol)
                continue

            requests = self._builder.build(
                batch_id=batch_id,
                instrument_id=inst.instrument_id,
                symbol=inst.symbol,
                vendor=self._vendor,
                stream=self._stream_name,
                bar_interval=interval,
                start_date=plan.start_date,
                end_date=plan.end_date,
                load_type=plan.mode.value.upper(),
                batch_size_days=plan.batch_size_days,
            )
            logger.info(
                f"[{inst.symbol}] {plan.mode.value}: {plan.start_date} → {plan.end_date} "
                f"({len(requests)} chunk(s))"
            )
            emitted.extend(requests)

        if dry_run:
            logger.info(f"DRY RUN — would emit {len(emitted)} requests")
        elif emitted:
            self._repo.save_all(emitted)

        summary = {
            "batch_id":          batch_id,
            "instruments":       len(instruments),
            "requests_emitted":  len(emitted),
            "skipped_noop":      skipped_noop,
            "skipped_inflight":  skipped_inflight,
            "dry_run":           dry_run,
        }
        logger.info(f"FetchPlannerJob complete: {summary}")
        return summary

    def _in_flight_instrument_ids(self) -> set:
        """Instruments with unfinished requests — never double-queue them.
        Makes planner re-runs idempotent even if the agent is mid-backlog."""
        rows = self._spark.sql(f"""
            SELECT DISTINCT instrument_id
            FROM {self._catalog}.control.fetch_request
            WHERE stream = '{self._stream_name}'
              AND status IN ('PENDING', 'LANDED')
        """).collect()
        return {r.instrument_id for r in rows}
