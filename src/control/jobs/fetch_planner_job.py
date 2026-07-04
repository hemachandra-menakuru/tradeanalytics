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

        import os
        ingestion_cfg = self._stream_cfg.ingestion
        self._builder = FetchRequestBuilder(
            raw_bucket=raw_bucket,
            backfill_chunk_days=int(getattr(ingestion_cfg, "backfill_chunk_days", 365)),
            requested_by="fetch_planner_job",
            pipeline_version=os.environ.get("PIPELINE_VERSION", "unknown"),
        )
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
        enrichment  = self._load_instrument_enrichment()

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

            enr = enrichment.get(inst.instrument_id, {})
            if not enr.get("vendor_instrument_id"):
                logger.warning(
                    f"[{inst.symbol}] no {self._vendor} vendor_instrument_id in "
                    f"reference.instrument_vendor_id — agent will fall back to "
                    f"symbol qualification"
                )
            requests = self._builder.build(
                batch_id=batch_id,
                stream=self._stream_name,
                load_type=plan.mode.value.upper(),
                bar_interval=interval,
                start_date=plan.start_date,
                end_date=plan.end_date,
                batch_size_days=plan.batch_size_days,
                instrument_id=inst.instrument_id,
                symbol=inst.symbol,
                vendor=self._vendor,
                vendor_instrument_id=enr.get("vendor_instrument_id"),
                asset_class=enr.get("asset_class") or "equity",
                security_type="STK",   # equities/ETFs only until new asset classes onboard
                exchange="SMART",
                exchange_mic=enr.get("exchange_mic"),
                currency=enr.get("currency") or "USD",
                vendor_exchange=enr.get("vendor_exchange"),
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

    def _load_instrument_enrichment(self) -> dict:
        """
        One JOIN across the reference tables → everything the manifest needs
        (Contract v2 principle: planner resolves once, agent looks up nothing).
        Keyed by instrument_id.
        """
        rows = self._spark.sql(f"""
            SELECT i.instrument_id,
                   i.asset_class,
                   l.exchange_mic,
                   l.currency,
                   v.vendor_instrument_id,
                   v.vendor_exchange
            FROM {self._catalog}.reference.instrument i
            LEFT JOIN {self._catalog}.reference.instrument_listing l
                   ON l.instrument_id = i.instrument_id AND l.is_current = true
            LEFT JOIN {self._catalog}.reference.instrument_vendor_id v
                   ON v.instrument_id = i.instrument_id
                  AND v.vendor = '{self._vendor}' AND v.is_current = true
        """).collect()
        return {r.instrument_id: r.asDict() for r in rows}

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
