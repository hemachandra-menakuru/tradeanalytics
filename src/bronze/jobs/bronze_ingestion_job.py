
"""
TradeAnalytics Bronze Ingestion Job
=====================================
Orchestrates the complete Bronze ingestion pipeline for one stream.

Flow per symbol:
  1. Read watermark (what do we already have?)
  2. IngestionPlanner → FetchPlan (what should we fetch?)
  3. Skip if NO_OP
  4. Provider.get_historical() → raw records
  5. DataQualityValidator → ValidationSummary (clean/flagged/rejected)
  6. BronzeWriter → write to Delta tables (with Layer 2 dedup)
  7. WatermarkManager → update watermark

Three-layer deduplication:
  Layer 1: IngestionPlanner — determines date range (avoids re-fetching)
  Layer 2: BronzeWriter._classify_record() — new/amend/skip per record
  Layer 3: Silver ROW_NUMBER window — one record per key in downstream

Usage (local/test):
    job = BronzeIngestionJob(config, stream_name="daily")
    results = job.run()

Usage (Databricks):
    job = BronzeIngestionJob(config, stream_name="daily", spark=spark)
    results = job.run(symbols=["AAPL", "MSFT"])  # or None for all active
"""

from __future__ import annotations

import logging
import subprocess
import time
from datetime import date, datetime, timezone
from typing import List, Optional

from src.shared.config.config_loader import ConfigNode
from src.bronze.factory.provider_factory import MarketDataFactory
from src.bronze.models.ingestion_mode import FetchPlan, IngestionMode
from src.bronze.models.ingestion_planner import IngestionPlanner
from src.shared.base.universe_reader import UniverseReader, InstrumentInfo
from src.reference.readers.ticker_reader import TickerReader, TickerInfo  # kept for backward compat
from src.reference.readers.delta_universe_reader import DeltaUniverseReader
from src.bronze.validation.validator import DataQualityValidator
from src.bronze.writers.bronze_writer import BronzeWriter, BronzeWriteResult
from src.bronze.writers.watermark_manager import WatermarkManager
from src.shared.base.watermark_store import WatermarkStore
from src.control.watermark.delta_watermark_store import DeltaWatermarkStore

logger = logging.getLogger(__name__)


def _get_pipeline_version() -> str:
    """
    Get pipeline version for audit trail.
    Reads PIPELINE_VERSION env var first (set by DABs at deploy time via databricks.yml).
    Falls back to git SHA for local development.
    """
    import os
    version = os.environ.get("PIPELINE_VERSION")
    if version and version != "unknown":
        return version
    try:
        result = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            capture_output=True, text=True, timeout=5
        )
        return result.stdout.strip() or "unknown"
    except Exception:
        return "unknown"


class JobRunSummary:
    """Summary of a complete job run across all symbols."""

    def __init__(self, stream_name: str, batch_id: str):
        self.stream_name    = stream_name
        self.batch_id       = batch_id
        self.started_at     = datetime.now(timezone.utc).isoformat()
        self.completed_at:  Optional[str] = None
        self.results:       List[BronzeWriteResult] = []
        self.skipped:       List[str] = []       # symbols with NO_OP plan
        self.failed:        List[str] = []       # symbols that errored
        self.errors:        dict = {}            # symbol → error message

    def add_result(self, result: BronzeWriteResult) -> None:
        self.results.append(result)

    def add_skip(self, symbol: str) -> None:
        self.skipped.append(symbol)

    def add_failure(self, symbol: str, error: str) -> None:
        self.failed.append(symbol)
        self.errors[symbol] = error

    def complete(self) -> None:
        self.completed_at = datetime.now(timezone.utc).isoformat()

    @property
    def total_symbols(self) -> int:
        return len(self.results) + len(self.skipped) + len(self.failed)

    @property
    def total_records_written(self) -> int:
        return sum(r.records_written + r.records_amended for r in self.results)

    @property
    def total_records_rejected(self) -> int:
        return sum(r.rejected_written for r in self.results)

    @property
    def success_rate_pct(self) -> float:
        if self.total_symbols == 0:
            return 100.0
        successful = len(self.results) + len(self.skipped)
        return round(successful / self.total_symbols * 100, 1)

    def __repr__(self) -> str:
        return (
            f"JobRunSummary("
            f"stream={self.stream_name}, "
            f"symbols={self.total_symbols}, "
            f"written={self.total_records_written}, "
            f"rejected={self.total_records_rejected}, "
            f"failed={len(self.failed)}, "
            f"skipped={len(self.skipped)})"
        )


class BronzeIngestionJob:
    """
    Orchestrates Bronze ingestion for a single stream (daily/intraday/tick).
    Connects all pipeline components end-to-end.
    """

    def __init__(
        self,
        config: ConfigNode,
        stream_name:   str = "daily",
        spark=None,
        ticker_reader: Optional[UniverseReader] = None,
        validator:     Optional[DataQualityValidator] = None,
        writer:        Optional[BronzeWriter] = None,
        watermark_mgr=None,
    ):
        """
        Args:
            config:        Loaded ConfigNode
            stream_name:   "daily" | "intraday" | "tick"
            spark:         SparkSession (None = local/test mode)
            ticker_reader: Override universe reader (for testing)
            validator:     Override validator (for testing)
            writer:        Override writer (for testing)
            watermark_mgr: Override watermark store — accepts WatermarkStore
                           (DeltaWatermarkStore) or legacy WatermarkManager
        """
        self._config      = config
        self._stream_name = stream_name
        self._stream_cfg  = getattr(config, stream_name)
        self._spark       = spark
        self._mode        = "spark" if spark is not None else "local"

        # Determine catalog/schema from config
        catalog = config.databricks.catalog
        schema  = config.databricks.schemas.bronze

        # Wire up components (inject or create defaults)
        # Default: DeltaUniverseReader (reads from reference.ticker_feed_config via Spark)
        # Tests inject TickerReader (CSV) or a mock via ticker_reader param
        if ticker_reader is not None:
            self._ticker_reader: UniverseReader = ticker_reader
        elif spark is not None:
            self._ticker_reader = DeltaUniverseReader(
                mode="spark", spark=spark, catalog=catalog
            )
        else:
            self._ticker_reader = TickerReader(config)  # local/test mode only

        self._validator = validator or DataQualityValidator.for_stream(
            config, stream_name
        )

        self._writer = writer or BronzeWriter(
            mode=self._mode, spark=spark,
            catalog=catalog, schema=schema,
        )

        self._watermark_mgr = watermark_mgr or DeltaWatermarkStore(
            mode=self._mode, spark=spark, catalog=catalog,
        )

        self._planner          = IngestionPlanner(config, stream_name)
        self._provider         = MarketDataFactory.get_provider(config)
        self._pipeline_version = _get_pipeline_version()

        logger.info(
            f"BronzeIngestionJob initialised — "
            f"stream={stream_name}, "
            f"mode={self._mode}, "
            f"provider={self._provider.provider_name}, "
            f"pipeline_version={self._pipeline_version}"
        )

    def run(
        self,
        symbols:     Optional[List[str]] = None,
        as_of_date:  Optional[date] = None,
        start_date:  Optional[date] = None,
        end_date:    Optional[date] = None,
        dry_run:     bool = False,
    ) -> JobRunSummary:
        """
        Run ingestion for all active tickers (or specified symbols).

        Args:
            symbols:    Override active ticker list (None = all active)
            as_of_date: Override today's date (for testing/backfill)
            start_date: Force explicit date range start (overrides planner)
            end_date:   Force explicit date range end (overrides planner)
            dry_run:    Plan only — do not fetch or write (for validation)

        Returns:
            JobRunSummary with results for all symbols
        """
        today    = as_of_date or date.today()
        batch_id = self._generate_batch_id()

        summary = JobRunSummary(
            stream_name=self._stream_name,
            batch_id=batch_id,
        )

        # Get ticker list
        tickers = self._get_tickers(symbols)
        if not tickers:
            logger.warning(f"No active tickers found for stream={self._stream_name}")
            summary.complete()
            return summary

        logger.info(
            f"Starting {self._stream_name} ingestion — "
            f"{len(tickers)} symbols, batch_id={batch_id}, "
            f"dry_run={dry_run}"
        )

        # Provider health check before starting
        if not dry_run and not self._provider.health_check():
            logger.warning(
                f"Primary provider {self._provider.provider_name} "
                f"health check failed — trying fallback"
            )
            self._provider = MarketDataFactory.get_fallback_provider(
                self._config
            )
            logger.info(
                f"Switched to fallback provider: "
                f"{self._provider.provider_name}"
            )

        # interval is the primary interval for this stream — needed in
        # error handler below where _process_ticker may not have run yet
        stream_interval = self._stream_cfg.intervals[0]

        # Process each ticker
        for ticker in tickers:
            try:
                result = self._process_ticker(
                    ticker=ticker,
                    batch_id=batch_id,
                    today=today,
                    dry_run=dry_run,
                    job_summary=summary,
                    start_date_override=start_date,
                    end_date_override=end_date,
                )
                if result is not None:
                    summary.add_result(result)

            except Exception as e:
                error_msg = f"{type(e).__name__}: {e}"
                logger.error(
                    f"[{ticker.symbol}] Failed: {error_msg}",
                    exc_info=True,
                )
                summary.add_failure(ticker.symbol, error_msg)

                # Update watermark with failure status
                try:
                    wm = self._watermark_mgr.get_watermark(
                        ticker.symbol, stream_interval
                    )
                    if wm is not None:
                        self._watermark_mgr.update_watermark(
                            symbol=ticker.symbol,
                            interval=stream_interval,
                            earliest_date=wm.earliest_date,
                            latest_date=wm.latest_date,
                            record_count=wm.record_count,
                            batch_id=batch_id,
                            mode="unknown",
                            status="failed",
                            error_message=error_msg,
                        )
                except Exception:
                    pass

        summary.complete()

        logger.info(
            f"Completed {self._stream_name} ingestion — "
            f"batch_id={batch_id}, "
            f"{summary.total_records_written} records written, "
            f"{len(summary.skipped)} symbols skipped (no-op), "
            f"{len(summary.failed)} symbols failed"
        )

        return summary

    def _process_ticker(
        self,
        ticker: InstrumentInfo,
        batch_id: str,
        today: date,
        dry_run: bool,
        job_summary: JobRunSummary,
        start_date_override: Optional[date] = None,
        end_date_override:   Optional[date] = None,
    ) -> Optional[BronzeWriteResult]:
        """
        Process one ticker through the full pipeline.
        Returns BronzeWriteResult or None if NO_OP.
        """
        symbol    = ticker.symbol
        interval  = self._stream_cfg.intervals[0]
        stream    = self._stream_name

        # Step 1: Read watermark — use instrument_id if available, else symbol fallback
        if ticker.instrument_id is not None and isinstance(self._watermark_mgr, DeltaWatermarkStore):
            watermark = self._watermark_mgr.get_watermark(ticker.instrument_id, stream)
        elif isinstance(self._watermark_mgr, DeltaWatermarkStore):
            # instrument_id=None (CSV path) + DeltaWatermarkStore — key types incompatible.
            # Treat as first run; watermark will not be persisted for this ticker.
            watermark = None
        else:
            watermark = self._watermark_mgr.get_watermark(symbol, interval)

        # Step 2: Determine fetch plan — table-driven when instrument_id available
        if ticker.instrument_id is not None:
            plan = self._planner.plan(
                instrument=ticker,
                stream=stream,
                watermark=watermark,
                as_of_date=today,
            )
        else:
            plan = self._planner.plan(
                symbol=symbol,
                interval=interval,
                watermark=watermark,
                as_of_date=today,
                ticker_history_start=ticker.effective_history_start,
            )

        # Override plan date range if explicit start/end provided
        # (used by smoke tests and ad-hoc backfills via notebook widgets)
        if start_date_override is not None and end_date_override is not None:
            logger.info(
                f"[{symbol}] Date range override: "
                f"{start_date_override} → {end_date_override} "
                f"(was: {plan.mode.value} {plan.start_date} → {plan.end_date})"
            )
            plan.mode       = IngestionMode.EXPLICIT_DATE_RANGE
            plan.start_date = start_date_override
            plan.end_date   = end_date_override
            plan.ingestion_type = IngestionMode.EXPLICIT_DATE_RANGE.ingestion_type

        logger.info(
            f"[{symbol}] Plan: {plan.mode.value} — "
            f"{plan.start_date} → {plan.end_date} "
            f"({plan.date_range_days} days, "
            f"~{plan.estimated_batches} batches)"
        )

        # Step 3: Skip if NO_OP
        if plan.mode == IngestionMode.NO_OP:
            logger.debug(f"[{symbol}] NO-OP — already up to date")
            job_summary.add_skip(symbol)
            return None

        if dry_run:
            logger.info(f"[{symbol}] DRY RUN — would fetch {plan}")
            job_summary.add_skip(symbol)
            return None

        # Step 4: Fetch data in batches
        raw_records = self._fetch_in_batches(plan, symbol, interval)

        if not raw_records:
            logger.warning(
                f"[{symbol}] No data returned from provider — "
                f"skipping write"
            )
            job_summary.add_skip(symbol)
            return None

        # Step 5: Validate
        validation_summary = self._validator.validate_batch(
            symbol=symbol,
            interval=interval,
            batch_id=batch_id,
            raw_records=raw_records,
            pipeline_version=self._pipeline_version,
            ingestion_type=plan.ingestion_type,
            instrument_id=ticker.instrument_id,
            ingested_by=self._provider.provider_name,
        )

        logger.info(
            f"[{symbol}] Validation: "
            f"{validation_summary.total_passed} passed, "
            f"{validation_summary.total_flagged} flagged, "
            f"{validation_summary.total_rejected} rejected"
        )

        # Step 6: Write to Bronze
        write_result = self._writer.write_batch(
            symbol=symbol,
            interval=interval,
            batch_id=batch_id,
            clean_records=validation_summary.writable_records,
            rejected_records=validation_summary.rejected_records,
            main_table=self._stream_cfg.table,
            rejected_table=self._stream_cfg.rejected_table,
        )

        # Step 7: Update watermark
        self._update_watermark_after_write(
            symbol=symbol,
            interval=interval,
            plan=plan,
            existing_watermark=watermark,
            validation_summary=validation_summary,
            write_result=write_result,
            batch_id=batch_id,
            instrument_id=ticker.instrument_id,
        )

        return write_result

    def _update_watermark_after_write(
        self,
        symbol: str,
        interval: str,
        plan,
        existing_watermark,
        validation_summary,
        write_result,
        batch_id: str,
        instrument_id: Optional[int] = None,
    ) -> None:
        """Update ingestion watermark after a successful write."""
        if write_result.records_written == 0 and write_result.records_amended == 0:
            return

        written_dates = [
            r.get("bar_date") for r in validation_summary.writable_records
            if r.get("bar_date")
        ]
        if not written_dates:
            return

        min_date = date.fromisoformat(min(written_dates))
        max_date = date.fromisoformat(max(written_dates))

        if existing_watermark is not None:
            min_date = min(min_date, existing_watermark.earliest_date)
            max_date = max(max_date, existing_watermark.latest_date)

        record_count = self._writer.get_record_count(
            symbol=symbol, interval=interval, table_name=self._stream_cfg.table,
        )

        if instrument_id is not None and isinstance(self._watermark_mgr, DeltaWatermarkStore):
            # Full path — instrument_id keyed, writes to control.ingestion_watermark
            self._watermark_mgr.update_watermark(
                instrument_id=instrument_id,
                stream=self._stream_name,
                interval=interval,
                earliest_date=min_date,
                latest_date=max_date,
                record_count=record_count,
                batch_id=batch_id,
                mode=plan.mode.value,
                status="success",
                vendor=self._provider.provider_name,
            )
        elif isinstance(self._watermark_mgr, DeltaWatermarkStore):
            # instrument_id=None (CSV path) + DeltaWatermarkStore — skip watermark update.
            # Watermark will be written on next run when DeltaUniverseReader is used.
            logger.warning(
                f"[{symbol}] Skipping watermark update — instrument_id not available. "
                "Use DeltaUniverseReader to enable watermark persistence."
            )
        else:
            # Legacy WatermarkManager path (symbol+interval key)
            self._watermark_mgr.update_watermark(
                symbol=symbol,
                interval=interval,
                earliest_date=min_date,
                latest_date=max_date,
                record_count=record_count,
                batch_id=batch_id,
                mode=plan.mode.value,
                status="success",
            )

    def _fetch_in_batches(
        self,
        plan: FetchPlan,
        symbol: str,
        interval: str,
    ) -> List[dict]:
        """
        Fetch data in batches to avoid API rate limits.
        Splits large date ranges into batch_size_days chunks.
        """
        from datetime import timedelta

        all_records = []
        current_start = plan.start_date
        batch_num     = 0

        while current_start <= plan.end_date:
            batch_end = min(
                current_start + timedelta(days=plan.batch_size_days - 1),
                plan.end_date,
            )
            batch_num += 1

            logger.debug(
                f"[{symbol}] Batch {batch_num}/{plan.estimated_batches}: "
                f"{current_start} → {batch_end}"
            )

            try:
                batch_records = self._provider.get_historical(
                    symbol=symbol,
                    start_date=current_start,
                    end_date=batch_end,
                    interval=interval,
                )
                all_records.extend(batch_records)
                logger.debug(
                    f"[{symbol}] Batch {batch_num}: "
                    f"{len(batch_records)} records fetched"
                )
            except Exception as e:
                logger.error(
                    f"[{symbol}] Batch {batch_num} failed "
                    f"({current_start} → {batch_end}): {e}"
                )
                # Continue with next batch — partial data is better than none

            current_start = batch_end + timedelta(days=1)

        logger.info(
            f"[{symbol}] Fetched {len(all_records)} total records "
            f"in {batch_num} batches"
        )
        return all_records

    def _get_tickers(
        self,
        symbols: Optional[List[str]],
    ) -> List[InstrumentInfo]:
        """Get instrument list — filtered by symbols if provided."""
        return self._ticker_reader.get_active_instruments(symbols=symbols or None)

    def _generate_batch_id(self) -> str:
        """Generate unique batch ID for this job run."""
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        return f"batch_{self._stream_name}_{timestamp}"

