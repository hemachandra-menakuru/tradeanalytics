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
        fs_ls=None,
        stream_name: str = "daily",
        vendor: str = "ibkr",
        require_vendor_id: bool = True,
    ):
        """
        require_vendor_id (default True — PRODUCTION POLICY): instruments with
        no current mapping in reference.instrument_vendor_id are SKIPPED
        (reported in summary.skipped_unmapped), never emitted as symbol-only
        manifests. Rationale: symbol qualification at fetch time can silently
        bind a REUSED ticker to the wrong company (wrong data under our
        instrument_id — the exact failure instrument_id exists to prevent).
        Set False only for supervised bootstrap runs.
        """
        self._config      = config
        self._spark       = spark
        self._stream_name = stream_name
        self._stream_cfg  = getattr(config, stream_name)
        self._vendor      = vendor
        self._require_vendor_id = require_vendor_id
        # fs_ls: callable(path) -> iterable with .path attrs (dbutils.fs.ls).
        # None disables orphan repair (unit tests / environments without dbutils).
        self._fs_ls = fs_ls

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

        orphans_repaired = self._repair_orphaned_requests()

        instruments = self._universe.get_active_instruments(symbols=symbols or None)
        in_flight   = self._in_flight_instrument_ids()
        enrichment  = self._load_instrument_enrichment()

        emitted, skipped_noop, skipped_inflight, skipped_unmapped = [], [], [], []

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
                if self._require_vendor_id:
                    logger.warning(
                        f"[{inst.symbol}] SKIPPED — no current {self._vendor} mapping in "
                        f"reference.instrument_vendor_id (require_vendor_id policy). "
                        f"Seed the mapping (scripts/seed_vendor_ids.py) to enable fetching."
                    )
                    skipped_unmapped.append(inst.symbol)
                    continue
                logger.warning(
                    f"[{inst.symbol}] BOOTSTRAP MODE — no {self._vendor} mapping; agent "
                    f"will fall back to symbol qualification (reuse risk accepted)"
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
            "skipped_unmapped":  skipped_unmapped,   # unmapped = blocked by policy, needs seeding
            "orphans_repaired":  orphans_repaired,   # PENDING rows without manifests, marked ORPHANED
            "dry_run":           dry_run,
        }
        logger.info(f"FetchPlannerJob complete: {summary}")
        return summary

    def _repair_orphaned_requests(self) -> int:
        """
        Crash-recovery: if a previous planner run died between the Delta insert
        and the S3 manifest write, PENDING rows exist with no manifest — the
        agent will never see them, and the in-flight check (status-only) would
        skip the instrument forever. Repair: mark such rows ORPHANED so the
        instrument becomes plannable again this very run (fresh requests +
        manifests are then emitted normally). ORPHANED is terminal and audit-
        preserving — we never delete rows.
        """
        if self._fs_ls is None:
            return 0
        rows = self._spark.sql(f"""
            SELECT request_key, s3_manifest_path
            FROM {self._catalog}.control.fetch_request
            WHERE stream = '{self._stream_name}' AND status = 'PENDING'
        """).collect()
        if not rows:
            return 0

        # One listing of the pending prefix (not per-row HEADs)
        pending_prefix = self._repo.pending_prefix(self._vendor)
        try:
            existing = {f.path.rstrip("/") for f in self._fs_ls(pending_prefix)}
        except Exception:
            existing = set()   # prefix absent = no manifests at all

        orphaned = [r.request_key for r in rows
                    if r.s3_manifest_path.rstrip("/") not in existing]
        if not orphaned:
            return 0

        keys_sql = ", ".join(f"'{k}'" for k in orphaned)
        self._spark.sql(f"""
            UPDATE {self._catalog}.control.fetch_request
            SET status = 'ORPHANED',
                error_message = 'planner crash window: PENDING row had no manifest in pending/ — repaired {datetime.now(timezone.utc).isoformat()}'
            WHERE request_key IN ({keys_sql})
        """)
        logger.warning(
            f"Repaired {len(orphaned)} orphaned request(s) (PENDING without manifest) "
            f"— instruments re-planned this run: {orphaned[:5]}{'…' if len(orphaned) > 5 else ''}"
        )
        return len(orphaned)

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
