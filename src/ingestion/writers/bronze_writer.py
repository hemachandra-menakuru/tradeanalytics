"""
TradeAnalytics Bronze Writer
=============================
Handles writing validated records to Bronze Delta tables.

In Databricks: uses PySpark to write Delta tables.
In local dev/tests: uses an in-memory dict (no Spark required).

The writer is the ONLY place that touches Delta tables.
All deduplication decisions are made BEFORE the writer is called.

Write rules (Bronze is APPEND-ONLY):
  - Never UPDATE existing records
  - Never DELETE records
  - Never MERGE/UPSERT
  - Always APPEND — amendments are new records with higher record_version

Deduplication happens at THREE layers:
  Layer 1: IngestionPlanner — determines correct date range (skip if up to date)
  Layer 2: DuplicateDetector — classifies each record as new/amend/skip
  Layer 3: Silver ROW_NUMBER — ensures exactly one record per key downstream
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


class BronzeWriteResult:
    """Result of a Bronze write operation."""

    def __init__(
        self,
        symbol: str,
        interval: str,
        records_written: int,
        records_skipped: int,
        records_amended: int,
        rejected_written: int,
        batch_id: str,
        duration_ms: int,
        table_name: str,
    ):
        self.symbol           = symbol
        self.interval         = interval
        self.records_written  = records_written
        self.records_skipped  = records_skipped
        self.records_amended  = records_amended
        self.rejected_written = rejected_written
        self.batch_id         = batch_id
        self.duration_ms      = duration_ms
        self.table_name       = table_name

    @property
    def total_processed(self) -> int:
        return self.records_written + self.records_skipped + self.records_amended

    def __repr__(self) -> str:
        return (
            f"BronzeWriteResult("
            f"symbol={self.symbol}, "
            f"written={self.records_written}, "
            f"skipped={self.records_skipped}, "
            f"amended={self.records_amended}, "
            f"rejected={self.rejected_written})"
        )


class BronzeWriter:
    """
    Writes validated OHLCV records to Bronze Delta tables.
    Supports both Spark (Databricks) and local (in-memory) modes.

    Usage:
        # Local / test mode (default)
        writer = BronzeWriter(mode="local")
        result = writer.write_batch(symbol, interval, batch_id, summary)

        # Databricks mode
        writer = BronzeWriter(mode="spark", spark=spark_session)
        result = writer.write_batch(symbol, interval, batch_id, summary)
    """

    def __init__(
        self,
        mode: str = "local",
        spark=None,
        catalog: str = "handh_trade",
        schema: str = "bronze",
    ):
        """
        Args:
            mode:    "local" (in-memory) or "spark" (Databricks Delta)
            spark:   SparkSession (required for spark mode)
            catalog: Unity Catalog name
            schema:  Bronze schema name
        """
        if mode == "spark" and spark is None:
            raise ValueError("SparkSession required for spark mode")

        self._mode    = mode
        self._spark   = spark
        self._catalog = catalog
        self._schema  = schema

        # Local mode storage — keyed by table_name
        self._local_store: Dict[str, List[dict]] = {}

        logger.info(f"BronzeWriter initialised — mode={mode}")

    def write_batch(
        self,
        symbol: str,
        interval: str,
        batch_id: str,
        clean_records: List[dict],
        rejected_records: List[dict],
        stream_cfg,
        pipeline_version: str = "unknown",
    ) -> BronzeWriteResult:
        """
        Write validated records from a ValidationSummary to Bronze tables.

        Args:
            symbol:           Ticker symbol
            interval:         Data interval
            batch_id:         Unique batch identifier
            clean_records:    Passed + flagged records → main table
            rejected_records: Rejected records → rejected table
            stream_cfg:       Stream config node (has table names)
            pipeline_version: Git SHA

        Returns:
            BronzeWriteResult with counts and timing
        """
        import time
        start_ms = time.time()

        main_table     = stream_cfg.table
        rejected_table = stream_cfg.rejected_table

        # Layer 2 deduplication — classify each record
        new_records   = []
        amended       = []
        skipped_count = 0

        for record in clean_records:
            action = self._classify_record(record, main_table)
            if action == "new":
                new_records.append(record)
            elif action == "amend":
                amended.append(record)
            else:  # skip
                skipped_count += 1

        # Write new records
        if new_records:
            self._append_records(new_records, main_table)
            logger.info(
                f"[{symbol}/{interval}] Wrote {len(new_records)} "
                f"new records to {main_table}"
            )

        # Write amendments
        if amended:
            self._append_records(amended, main_table)
            logger.info(
                f"[{symbol}/{interval}] Wrote {len(amended)} "
                f"amendments to {main_table}"
            )

        # Write rejected records
        if rejected_records:
            self._append_records(rejected_records, rejected_table)
            logger.info(
                f"[{symbol}/{interval}] Wrote {len(rejected_records)} "
                f"rejected records to {rejected_table}"
            )

        if skipped_count > 0:
            logger.debug(
                f"[{symbol}/{interval}] Skipped {skipped_count} "
                f"duplicate records (already in Bronze)"
            )

        duration_ms = int((time.time() - start_ms) * 1000)

        return BronzeWriteResult(
            symbol=symbol,
            interval=interval,
            records_written=len(new_records),
            records_skipped=skipped_count,
            records_amended=len(amended),
            rejected_written=len(rejected_records),
            batch_id=batch_id,
            duration_ms=duration_ms,
            table_name=main_table,
        )

    def _classify_record(self, record: dict, table_name: str) -> str:
        """
        Layer 2 deduplication — classify incoming record.

        Returns:
            "new"   — no existing record for this key, write it
            "amend" — existing record but data changed, write amendment
            "skip"  — exact duplicate, skip
        """
        existing = self._get_existing_record(record, table_name)

        if existing is None:
            return "new"

        # Compare price fields with tolerance
        price_fields = ["open", "high", "low", "close", "volume"]
        tolerance    = 0.001

        for field in price_fields:
            existing_val = existing.get(field)
            incoming_val = record.get(field)

            if existing_val is None or incoming_val is None:
                continue

            if abs(float(existing_val) - float(incoming_val)) > tolerance:
                # Data changed — this is a legitimate amendment
                return "amend"

        return "skip"

    def _get_existing_record(
        self,
        record: dict,
        table_name: str,
    ) -> Optional[dict]:
        """
        Find the most recent existing record for this unique key.
        Local mode: searches in-memory store.
        Spark mode: queries Delta table.
        """
        if self._mode == "local":
            return self._local_get_existing(record, table_name)
        else:
            return self._spark_get_existing(record, table_name)

    def _local_get_existing(
        self,
        record: dict,
        table_name: str,
    ) -> Optional[dict]:
        """Search local in-memory store for existing record."""
        if table_name not in self._local_store:
            return None

        symbol   = record.get("symbol")
        date_val = record.get("date")
        interval = record.get("interval")
        bar_time = record.get("bar_time_utc")

        # Find matching records — get latest version
        matches = []
        for existing in self._local_store[table_name]:
            if (existing.get("symbol")   == symbol and
                existing.get("date")     == date_val and
                existing.get("interval") == interval):

                # For intraday, also match bar_time
                if bar_time is not None:
                    if existing.get("bar_time_utc") != bar_time:
                        continue
                matches.append(existing)

        if not matches:
            return None

        # Return latest version
        return max(matches, key=lambda r: r.get("record_version", 1))

    def _spark_get_existing(
        self,
        record: dict,
        table_name: str,
    ) -> Optional[dict]:
        """Query Delta table for existing record. Databricks only."""
        symbol   = record.get("symbol")
        date_val = record.get("date")
        interval = record.get("interval")

        full_table = f"{self._catalog}.{self._schema}.{table_name}"

        try:
            df = self._spark.sql(f"""
                SELECT * FROM {full_table}
                WHERE symbol = '{symbol}'
                  AND date = '{date_val}'
                  AND interval = '{interval}'
                ORDER BY record_version DESC
                LIMIT 1
            """)
            if df.count() == 0:
                return None
            return df.first().asDict()
        except Exception as e:
            logger.warning(
                f"Could not query existing record from {full_table}: {e}. "
                f"Treating as new record."
            )
            return None

    def _append_records(self, records: List[dict], table_name: str) -> None:
        """Append records to table (local or Spark)."""
        if self._mode == "local":
            if table_name not in self._local_store:
                self._local_store[table_name] = []
            self._local_store[table_name].extend(records)
        else:
            self._spark_append(records, table_name)

    def _spark_append(self, records: List[dict], table_name: str) -> None:
        """
        Append records to Delta table using Spark.

        Uses the existing Delta table schema to create the DataFrame — this
        avoids PySparkValueError: CANNOT_DETERMINE_TYPE which occurs when
        Spark infers schema from dicts that contain all-None columns.

        Strategy:
          1. Read the existing table schema from the Delta table
          2. Cast/align each record dict to that schema
          3. Create DataFrame with explicit schema → no type inference
          4. Append to Delta table
        """
        from pyspark.sql.functions import lit
        from pyspark.sql.types import (
            StringType, DoubleType, LongType, IntegerType,
            BooleanType, DateType, TimestampType, StructType, StructField
        )

        full_table = f"{self._catalog}.{self._schema}.{table_name}"

        # Step 1: Get existing table schema
        try:
            table_schema = self._spark.table(full_table).schema
        except Exception as e:
            logger.warning(
                f"Could not read schema from {full_table}: {e}. "
                f"Falling back to schema inference."
            )
            df = self._spark.createDataFrame(records)
            df.write.format("delta").mode("append").saveAsTable(full_table)
            return

        # Step 2: Align each record to the table schema.
        # For each field in the table schema, pull the value from the record
        # (or None if missing). This guarantees every column is present and
        # typed correctly — no all-None columns that confuse Spark inference.
        type_map = {
            "StringType": str,
            "DoubleType": float,
            "LongType": int,
            "IntegerType": int,
            "BooleanType": bool,
        }

        aligned_records = []
        for record in records:
            aligned = {}
            for field in table_schema.fields:
                val = record.get(field.name)
                # Cast to correct Python type if not None
                if val is not None:
                    type_name = type(field.dataType).__name__
                    py_type = type_map.get(type_name)
                    if py_type is not None:
                        try:
                            val = py_type(val)
                        except (ValueError, TypeError):
                            val = None
                aligned[field.name] = val
            aligned_records.append(aligned)

        # Step 3: Create DataFrame with explicit schema
        df = self._spark.createDataFrame(aligned_records, schema=table_schema)

        # Step 4: Append to Delta table
        df.write.format("delta").mode("append").saveAsTable(full_table)
        logger.debug(
            f"Appended {len(records)} records to {full_table} "
            f"using explicit schema ({len(table_schema.fields)} fields)"
        )

    def get_local_records(self, table_name: str) -> List[dict]:
        """Return all records in local store for a table (test helper)."""
        return self._local_store.get(table_name, [])

    def get_local_record_count(self, table_name: str) -> int:
        return len(self.get_local_records(table_name))

    def clear_local_store(self) -> None:
        """Clear all local store data (test helper)."""
        self._local_store.clear()
