"""
DeltaWatermarkStore — WatermarkStore implementation backed by control.ingestion_watermark.

Key: instrument_id (BIGINT surrogate) + stream (daily | intraday | tick)
Table: tradeanalytics.control.ingestion_watermark

This replaces the old WatermarkManager (symbol+interval key, bronze schema).
BronzeIngestionJob is updated to use this in Phase 2.5 Step 3.

Modes:
  local — in-memory dict (unit tests, no Spark needed)
  spark — Delta MERGE via Databricks Connect or cluster
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timezone
from typing import Dict, List, Optional, Tuple

from src.shared.base.watermark_store import IngestionWatermarkRecord, WatermarkStore

logger = logging.getLogger(__name__)


class DeltaWatermarkStore(WatermarkStore):
    """
    Reads and writes ingestion watermarks to control.ingestion_watermark.

    Usage (local / tests):
        store = DeltaWatermarkStore(mode="local")
        wm = store.get_watermark(instrument_id=505, stream="daily")

    Usage (Spark / Databricks):
        store = DeltaWatermarkStore(mode="spark", spark=spark, catalog="tradeanalytics")
        store.update_watermark(instrument_id=505, stream="daily", ...)
    """

    _FULL_TABLE_TEMPLATE = "{catalog}.control.ingestion_watermark"

    def __init__(
        self,
        mode: str = "local",
        spark=None,
        catalog: str = "tradeanalytics",
    ):
        if mode == "spark" and spark is None:
            raise ValueError("SparkSession required for spark mode")

        self._mode    = mode
        self._spark   = spark
        self._catalog = catalog
        self._table   = self._FULL_TABLE_TEMPLATE.format(catalog=catalog)

        # Local mode: keyed by (instrument_id, stream)
        self._local_store: Dict[Tuple[int, str], IngestionWatermarkRecord] = {}

        logger.info(f"DeltaWatermarkStore initialised — mode={mode}, table={self._table}")

    # ------------------------------------------------------------------
    # Public API (WatermarkStore ABC)
    # ------------------------------------------------------------------

    def get_watermark(
        self,
        instrument_id: int,
        stream: str,
    ) -> Optional[IngestionWatermarkRecord]:
        """Return watermark for instrument_id + stream, or None on first run."""
        if self._mode == "local":
            return self._local_store.get((instrument_id, stream))
        return self._spark_get(instrument_id, stream)

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
        Upsert watermark after a pipeline write.

        Dates only ever widen — earliest never moves forward, latest never backward.
        On failure: increments consecutive_failures; on success: resets to 0.
        """
        existing = self.get_watermark(instrument_id, stream)

        if existing is not None:
            earliest_date = min(earliest_date, existing.earliest_date)
            latest_date   = max(latest_date,   existing.latest_date)
            consecutive_failures = (
                0 if status == "success"
                else existing.consecutive_failures + 1
            )
        else:
            consecutive_failures = 0 if status == "success" else 1

        record = IngestionWatermarkRecord(
            instrument_id        = instrument_id,
            stream               = stream,
            interval             = interval,
            earliest_date        = earliest_date,
            latest_date          = latest_date,
            record_count         = record_count,
            batch_id             = batch_id,
            mode                 = mode,
            status               = status,
            consecutive_failures = consecutive_failures,
            last_error_message   = error_message,
            vendor               = vendor,
        )

        if self._mode == "local":
            self._local_store[(instrument_id, stream)] = record
        else:
            self._spark_upsert(record)

        logger.info(
            f"Watermark updated: instrument_id={instrument_id}, stream={stream} — "
            f"earliest={earliest_date}, latest={latest_date}, "
            f"records={record_count}, mode={mode}, status={status}"
        )

    def list_watermarks(self, stream: Optional[str] = None) -> List[IngestionWatermarkRecord]:
        """Return all watermarks, optionally filtered by stream."""
        if self._mode == "local":
            records = list(self._local_store.values())
            if stream:
                records = [r for r in records if r.stream == stream]
            return records
        return self._spark_list(stream)

    # ------------------------------------------------------------------
    # Spark internals
    # ------------------------------------------------------------------

    def _spark_get(self, instrument_id: int, stream: str) -> Optional[IngestionWatermarkRecord]:
        """Read a single watermark row from Delta."""
        try:
            df = self._spark.sql(f"""
                SELECT * FROM {self._table}
                WHERE instrument_id = {instrument_id}
                  AND stream = '{stream}'
                LIMIT 1
            """)
            if df.count() == 0:
                return None
            return self._row_to_record(df.first().asDict())
        except Exception as e:
            logger.warning(
                f"Could not read watermark for instrument_id={instrument_id}, "
                f"stream={stream}: {e}. Treating as first run."
            )
            return None

    def _spark_list(self, stream: Optional[str]) -> List[IngestionWatermarkRecord]:
        """Read all watermarks from Delta, optionally filtered by stream."""
        try:
            where = f"WHERE stream = '{stream}'" if stream else ""
            df = self._spark.sql(f"SELECT * FROM {self._table} {where}")
            return [self._row_to_record(r.asDict()) for r in df.collect()]
        except Exception as e:
            logger.warning(f"Could not list watermarks: {e}")
            return []

    def _spark_upsert(self, record: IngestionWatermarkRecord) -> None:
        """MERGE watermark into control.ingestion_watermark."""
        from pyspark.sql.types import (
            StructType, StructField, LongType, StringType, DateType, IntegerType, TimestampType
        )

        now = datetime.now(timezone.utc).replace(tzinfo=None)

        # Column names match the actual control.ingestion_watermark DDL:
        #   last_run_at   (not last_successful_run)
        #   last_batch_id, last_load_type (stored from record)
        #   last_error    (not last_error_message)
        row = {
            "instrument_id":        record.instrument_id,
            "stream":               record.stream,
            "interval":             record.interval,
            "earliest_date":        record.earliest_date,
            "latest_date":          record.latest_date,
            "last_run_at":          now,
            "record_count":         record.record_count,
            "last_batch_id":        record.batch_id,
            "last_load_type":       record.mode,
            "status":               "active",
            "consecutive_failures": record.consecutive_failures,
            "last_error":           record.last_error_message,
            "vendor":               record.vendor,
            "created_at":           now,
            "updated_at":           now,
        }

        schema = StructType([
            StructField("instrument_id",        LongType(),      False),
            StructField("stream",               StringType(),    False),
            StructField("interval",             StringType(),    False),
            StructField("earliest_date",        DateType(),      True),
            StructField("latest_date",          DateType(),      True),
            StructField("last_run_at",          TimestampType(), True),
            StructField("record_count",         LongType(),      False),
            StructField("last_batch_id",        StringType(),    True),
            StructField("last_load_type",       StringType(),    True),
            StructField("status",               StringType(),    False),
            StructField("consecutive_failures", IntegerType(),   False),
            StructField("last_error",           StringType(),    True),
            StructField("vendor",               StringType(),    True),
            StructField("created_at",           TimestampType(), False),
            StructField("updated_at",           TimestampType(), False),
        ])

        df = self._spark.createDataFrame([row], schema=schema)
        df.createOrReplaceTempView("_wm_update")

        # MERGE on instrument_id + stream — created_at preserved on MATCH
        self._spark.sql(f"""
            MERGE INTO {self._table} AS target
            USING _wm_update AS source
            ON target.instrument_id = source.instrument_id
            AND target.stream       = source.stream
            WHEN MATCHED THEN UPDATE SET
                target.interval             = source.interval,
                target.earliest_date        = source.earliest_date,
                target.latest_date          = source.latest_date,
                target.last_run_at          = source.last_run_at,
                target.record_count         = source.record_count,
                target.last_batch_id        = source.last_batch_id,
                target.last_load_type       = source.last_load_type,
                target.status               = source.status,
                target.consecutive_failures = source.consecutive_failures,
                target.last_error           = source.last_error,
                target.vendor               = source.vendor,
                target.updated_at           = source.updated_at
            WHEN NOT MATCHED THEN INSERT (
                instrument_id, stream, interval, earliest_date, latest_date,
                last_run_at, record_count, last_batch_id, last_load_type,
                status, consecutive_failures, last_error, vendor, created_at, updated_at
            ) VALUES (
                source.instrument_id, source.stream, source.interval,
                source.earliest_date, source.latest_date,
                source.last_run_at, source.record_count, source.last_batch_id, source.last_load_type,
                source.status, source.consecutive_failures, source.last_error, source.vendor,
                source.created_at, source.updated_at
            )
        """)

    @staticmethod
    def _row_to_record(row: dict) -> IngestionWatermarkRecord:
        """Convert a Delta row dict to IngestionWatermarkRecord."""
        return IngestionWatermarkRecord(
            instrument_id        = row["instrument_id"],
            stream               = row["stream"],
            interval             = row.get("interval") or "",
            earliest_date        = date.fromisoformat(str(row["earliest_date"])[:10]),
            latest_date          = date.fromisoformat(str(row["latest_date"])[:10]),
            record_count         = row.get("record_count", 0),
            batch_id             = row.get("last_batch_id") or "",
            mode                 = row.get("last_load_type") or "",
            status               = row.get("status", "active"),
            consecutive_failures = row.get("consecutive_failures", 0),
            last_error_message   = row.get("last_error"),
            vendor               = row.get("vendor"),
        )
