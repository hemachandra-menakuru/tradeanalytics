
"""
TradeAnalytics Watermark Manager
==================================
Reads and updates the ingestion watermark table.
Tracks earliest_date and latest_date per symbol + interval.

The watermark table tells the IngestionPlanner:
  - Is this a first run? (no watermark exists)
  - What date range do we already have? (earliest_date → latest_date)
  - Do we need to extend backwards? (earliest_date > requested start)

Watermark table: tradeanalytics.bronze.ingestion_watermark_{stream}
  e.g. tradeanalytics.bronze.ingestion_watermark_daily

Local mode: in-memory dict (for tests and local development)
Spark mode: Delta table (Databricks)
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timezone
from typing import Dict, List, Optional

from src.ingestion.models.ingestion_mode import IngestionWatermark

logger = logging.getLogger(__name__)


class WatermarkManager:
    """
    Reads and writes ingestion watermarks.
    Supports local (in-memory) and Spark (Delta) modes.

    Usage:
        manager = WatermarkManager(mode="local")

        # Read
        wm = manager.get_watermark("AAPL", "1d")

        # Update after successful ingestion
        manager.update_watermark(
            symbol="AAPL",
            interval="1d",
            earliest_date=date(2016, 1, 4),
            latest_date=date(2026, 6, 22),
            record_count=2626,
            batch_id="batch_001",
            mode="incremental",
        )
    """

    def __init__(
        self,
        mode: str = "local",
        spark=None,
        catalog: str = "tradeanalytics",
        schema: str = "bronze",
        watermark_table: str = "ingestion_watermark_daily",
    ):
        if mode == "spark" and spark is None:
            raise ValueError("SparkSession required for spark mode")

        self._mode            = mode
        self._spark           = spark
        self._catalog         = catalog
        self._schema          = schema
        self._watermark_table = watermark_table

        # Local mode: keyed by (symbol, interval)
        self._local_store: Dict[tuple, IngestionWatermark] = {}

        logger.info(
            f"WatermarkManager initialised — "
            f"mode={mode}, table={watermark_table}"
        )

    def get_watermark(
        self,
        symbol: str,
        interval: str,
    ) -> Optional[IngestionWatermark]:
        """
        Get current watermark for symbol + interval.
        Returns None if no watermark exists (first run).
        """
        if self._mode == "local":
            return self._local_store.get((symbol, interval))
        else:
            return self._spark_get_watermark(symbol, interval)

    def get_all_watermarks(self) -> List[IngestionWatermark]:
        """Return all watermarks (useful for monitoring)."""
        if self._mode == "local":
            return list(self._local_store.values())
        else:
            return self._spark_get_all_watermarks()

    def update_watermark(
        self,
        symbol: str,
        interval: str,
        earliest_date: date,
        latest_date: date,
        record_count: int,
        batch_id: str,
        mode: str,
        status: str = "success",
        error_message: Optional[str] = None,
    ) -> IngestionWatermark:
        """
        Update watermark after successful (or partial) ingestion.

        For history_extension: earliest_date may move backwards.
        For incremental: latest_date moves forwards.
        For force_reload: both reset.

        Args:
            symbol:        Ticker symbol
            interval:      Data interval
            earliest_date: Oldest date now in Bronze for this symbol
            latest_date:   Most recent date now in Bronze for this symbol
            record_count:  Total Bronze records for this symbol+interval
            batch_id:      Batch that produced this update
            mode:          IngestionMode value (for audit trail)
            status:        "success" | "partial" | "failed"
            error_message: Error details if status != success

        Returns:
            Updated IngestionWatermark
        """
        # If existing watermark, preserve the wider date range
        existing = self.get_watermark(symbol, interval)
        if existing is not None:
            # Never move earliest_date forward (only backwards)
            earliest_date = min(earliest_date, existing.earliest_date)
            # Never move latest_date backward (only forwards)
            latest_date   = max(latest_date, existing.latest_date)

        watermark = IngestionWatermark(
            symbol          = symbol,
            interval        = interval,
            earliest_date   = earliest_date,
            latest_date     = latest_date,
            record_count    = record_count,
            last_run_at     = datetime.now(timezone.utc).isoformat(),
            last_batch_id   = batch_id,
            last_mode       = mode,
            last_run_status = status,
            error_message   = error_message,
        )

        if self._mode == "local":
            self._local_store[(symbol, interval)] = watermark
        else:
            self._spark_upsert_watermark(watermark)

        logger.info(
            f"Watermark updated: {symbol}/{interval} — "
            f"earliest={earliest_date}, latest={latest_date}, "
            f"records={record_count}, mode={mode}"
        )

        return watermark

    def _spark_get_watermark(
        self, symbol: str, interval: str
    ) -> Optional[IngestionWatermark]:
        """Read watermark from Delta table."""
        full_table = (
            f"{self._catalog}.{self._schema}.{self._watermark_table}"
        )
        try:
            df = self._spark.sql(f"""
                SELECT * FROM {full_table}
                WHERE symbol = '{symbol}'
                  AND interval = '{interval}'
                LIMIT 1
            """)
            if df.count() == 0:
                return None
            row = df.first().asDict()
            return IngestionWatermark(
                symbol          = row["symbol"],
                interval        = row["interval"],
                earliest_date   = date.fromisoformat(str(row["earliest_date"])),
                latest_date     = date.fromisoformat(str(row["latest_date"])),
                record_count    = row["record_count"],
                last_run_at     = str(row.get("last_run_at", "")),
                last_batch_id   = row.get("last_batch_id"),
                last_mode       = row.get("last_mode"),
                last_run_status = row.get("last_run_status", "success"),
                error_message   = row.get("error_message"),
            )
        except Exception as e:
            logger.warning(
                f"Could not read watermark from {full_table}: {e}. "
                f"Treating as first run."
            )
            return None

    def _spark_get_all_watermarks(self) -> List[IngestionWatermark]:
        """Read all watermarks from Delta table."""
        full_table = (
            f"{self._catalog}.{self._schema}.{self._watermark_table}"
        )
        try:
            df = self._spark.sql(f"SELECT * FROM {full_table}")
            results = []
            for row in df.collect():
                r = row.asDict()
                results.append(IngestionWatermark(
                    symbol          = r["symbol"],
                    interval        = r["interval"],
                    earliest_date   = date.fromisoformat(str(r["earliest_date"])),
                    latest_date     = date.fromisoformat(str(r["latest_date"])),
                    record_count    = r["record_count"],
                    last_run_at     = str(r.get("last_run_at", "")),
                    last_batch_id   = r.get("last_batch_id"),
                    last_mode       = r.get("last_mode"),
                    last_run_status = r.get("last_run_status", "success"),
                ))
            return results
        except Exception as e:
            logger.warning(f"Could not read watermarks: {e}")
            return []

    def _spark_upsert_watermark(self, watermark: IngestionWatermark) -> None:
        """Upsert watermark in Delta table."""
        full_table = (
            f"{self._catalog}.{self._schema}.{self._watermark_table}"
        )
        wm_dict = watermark.to_dict()
        from pyspark.sql.types import (
            StructType, StructField, StringType, LongType, DateType
        )
        # Explicit schema prevents CANNOT_DETERMINE_TYPE on None fields
        wm_schema = StructType([
            StructField("symbol",          StringType(), False),
            StructField("interval",        StringType(), False),
            StructField("earliest_date",   DateType(),   False),
            StructField("latest_date",     DateType(),   False),
            StructField("record_count",    LongType(),   True),
            StructField("last_run_at",     StringType(), True),
            StructField("last_batch_id",   StringType(), True),
            StructField("last_mode",       StringType(), True),
            StructField("last_run_status", StringType(), True),
            StructField("error_message",   StringType(), True),
        ])
        from datetime import date as _date
        # Ensure date fields are python datetime.date objects
        wm_dict["earliest_date"] = (
            _date.fromisoformat(str(wm_dict["earliest_date"])[:10])
            if not isinstance(wm_dict["earliest_date"], _date)
            else wm_dict["earliest_date"]
        )
        wm_dict["latest_date"] = (
            _date.fromisoformat(str(wm_dict["latest_date"])[:10])
            if not isinstance(wm_dict["latest_date"], _date)
            else wm_dict["latest_date"]
        )
        df = self._spark.createDataFrame([wm_dict], schema=wm_schema)

        # Watermark uses MERGE (not append-only) — it tracks current state
        df.createOrReplaceTempView("watermark_update")
        self._spark.sql(f"""
            MERGE INTO {full_table} AS target
            USING watermark_update AS source
            ON target.symbol   = source.symbol
            AND target.interval = source.interval
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)

