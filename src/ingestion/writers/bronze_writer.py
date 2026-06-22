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
  Layer 2: BronzeWriter._bulk_classify() — classifies all records in ONE query
  Layer 3: Silver ROW_NUMBER — ensures exactly one record per key downstream
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Tuple

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


# Type alias for the bulk lookup dict
# Key: (symbol, date, interval, bar_time_utc)  — bar_time_utc is None for daily
# Value: dict of the latest existing record for that key
_ExistingRecordMap = Dict[Tuple, dict]


class BronzeWriter:
    """
    Writes validated OHLCV records to Bronze Delta tables.
    Supports both Spark (Databricks) and local (in-memory) modes.

    Layer 2 deduplication uses a BULK pre-fetch strategy:
      - ONE query fetches all existing records for the incoming symbol + dates
      - Classification (new / amend / skip) happens in-memory against a dict
      - This avoids the N+1 Spark query problem (one query per record)

    Usage:
        # Local / test mode (default)
        writer = BronzeWriter(mode="local")
        result = writer.write_batch(symbol, interval, batch_id, ...)

        # Databricks mode
        writer = BronzeWriter(mode="spark", spark=spark_session)
        result = writer.write_batch(symbol, interval, batch_id, ...)
    """

    # Price fields compared during amendment detection
    _PRICE_FIELDS = ["open", "high", "low", "close", "volume"]
    _PRICE_TOLERANCE = 0.001

    def __init__(
        self,
        mode: str = "local",
        spark=None,
        catalog: str = "tradeanalytics",
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

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def write_batch(
        self,
        symbol: str,
        interval: str,
        batch_id: str,
        clean_records: List[dict],
        rejected_records: List[dict],
        stream_cfg,
    ) -> BronzeWriteResult:
        """
        Write validated records to Bronze tables.

        Layer 2 deduplication is performed here via a single bulk pre-fetch
        (one query for the entire batch, not one query per record).

        Args:
            symbol:           Ticker symbol
            interval:         Data interval
            batch_id:         Unique batch identifier
            clean_records:    Passed + flagged records → main table
            rejected_records: Rejected records → rejected table
            stream_cfg:       Stream config node (has table names)

        Returns:
            BronzeWriteResult with counts and timing
        """
        import time
        start_ms = time.time()

        main_table     = stream_cfg.table
        rejected_table = stream_cfg.rejected_table

        # Layer 2 deduplication — bulk classify (ONE query total)
        new_records, amended, skipped_count = self._bulk_classify(
            clean_records, main_table
        )

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

    def get_record_count(
        self,
        symbol: str,
        interval: str,
        table_name: str,
    ) -> int:
        """
        Return total Bronze record count for a symbol + interval.
        Used by BronzeIngestionJob to populate watermark record_count.
        """
        if self._mode == "local":
            return sum(
                1 for r in self._local_store.get(table_name, [])
                if r.get("symbol") == symbol and r.get("interval") == interval
            )
        else:
            return self._spark_count_records(symbol, interval, table_name)

    def get_local_records(self, table_name: str) -> List[dict]:
        """Return all records in local store for a table (test helper)."""
        return self._local_store.get(table_name, [])

    def get_local_record_count(self, table_name: str) -> int:
        """Return total record count in local store for a table (test helper)."""
        return len(self.get_local_records(table_name))

    def clear_local_store(self) -> None:
        """Clear all local store data (test helper)."""
        self._local_store.clear()

    # ------------------------------------------------------------------
    # Layer 2 deduplication — bulk classify
    # ------------------------------------------------------------------

    def _bulk_classify(
        self,
        records: List[dict],
        table_name: str,
    ) -> Tuple[List[dict], List[dict], int]:
        """
        Classify all incoming records in a single bulk operation.

        Strategy:
          1. Build a set of (symbol, date, interval, bar_time_utc) keys
             from the incoming records.
          2. Fetch ALL existing records matching those keys in ONE query.
          3. Build an in-memory lookup dict keyed on the composite key,
             storing the latest record_version for each key.
          4. Classify each incoming record against the dict — pure Python,
             zero additional I/O.

        Returns:
            (new_records, amended_records, skipped_count)
        """
        if not records:
            return [], [], 0

        # Step 1 — build key set from incoming batch
        incoming_keys = self._extract_keys(records)

        # Step 2 — bulk fetch existing records for those keys (ONE query)
        existing_map = self._bulk_fetch_existing(incoming_keys, table_name)

        # Step 3 — classify each record in-memory
        new_records = []
        amended     = []
        skipped     = 0

        for record in records:
            key      = self._record_key(record)
            existing = existing_map.get(key)

            if existing is None:
                new_records.append(record)
            elif self._has_price_changed(existing, record):
                amended.append(record)
            else:
                skipped += 1

        return new_records, amended, skipped

    def _extract_keys(
        self,
        records: List[dict],
    ) -> List[Tuple]:
        """Extract unique composite keys from a list of records."""
        seen = set()
        keys = []
        for r in records:
            k = self._record_key(r)
            if k not in seen:
                seen.add(k)
                keys.append(k)
        return keys

    def _record_key(self, record: dict) -> Tuple:
        """
        Composite deduplication key for one record.
        bar_time_utc is None for daily records.
        """
        return (
            record.get("symbol"),
            record.get("date"),
            record.get("interval"),
            record.get("bar_time_utc"),  # None for daily
        )

    def _has_price_changed(self, existing: dict, incoming: dict) -> bool:
        """Return True if any price field differs beyond tolerance."""
        for field in self._PRICE_FIELDS:
            existing_val = existing.get(field)
            incoming_val = incoming.get(field)
            if existing_val is None or incoming_val is None:
                continue
            if abs(float(existing_val) - float(incoming_val)) > self._PRICE_TOLERANCE:
                return True
        return False

    # ------------------------------------------------------------------
    # Bulk fetch — one query per write_batch call
    # ------------------------------------------------------------------

    def _bulk_fetch_existing(
        self,
        keys: List[Tuple],
        table_name: str,
    ) -> _ExistingRecordMap:
        """
        Fetch the latest existing record for each key in a single operation.

        Returns:
            Dict mapping composite key → latest existing record dict.
            Keys with no existing record are absent from the dict.
        """
        if not keys:
            return {}

        if self._mode == "local":
            return self._local_bulk_fetch(keys, table_name)
        else:
            return self._spark_bulk_fetch(keys, table_name)

    def _local_bulk_fetch(
        self,
        keys: List[Tuple],
        table_name: str,
    ) -> _ExistingRecordMap:
        """
        Build lookup dict from in-memory store.
        One pass over the store — O(N) where N = stored records.
        """
        store = self._local_store.get(table_name, [])
        if not store:
            return {}

        key_set = set(keys)

        # Collect all matching records grouped by key
        candidates: Dict[Tuple, List[dict]] = {}
        for record in store:
            k = self._record_key(record)
            if k in key_set:
                candidates.setdefault(k, []).append(record)

        # For each key keep only the latest record_version
        return {
            k: max(recs, key=lambda r: r.get("record_version", 1))
            for k, recs in candidates.items()
        }

    def _spark_bulk_fetch(
        self,
        keys: List[Tuple],
        table_name: str,
    ) -> _ExistingRecordMap:
        """
        Fetch existing records from Delta table in ONE Spark query.

        Builds a WHERE clause with OR conditions for all (symbol, date,
        interval) combinations, then picks the latest record_version
        per key using a window function — all inside one SQL statement.
        """
        full_table = f"{self._catalog}.{self._schema}.{table_name}"

        # Build the IN-clause predicate
        # Keys are (symbol, date, interval, bar_time_utc)
        # For daily streams bar_time_utc is None — we only filter on 3 fields
        # For intraday bar_time_utc is set — filter on all 4

        # Separate daily (bar_time_utc=None) from intraday keys
        daily_keys    = [(s, d, i) for s, d, i, bt in keys if bt is None]
        intraday_keys = [(s, d, i, bt) for s, d, i, bt in keys if bt is not None]

        predicates = []

        if daily_keys:
            # Use (symbol, date, interval) IN ((...), (...), ...)
            tuples = ", ".join(
                f"('{s}', '{d}', '{iv}')"
                for s, d, iv in daily_keys
            )
            predicates.append(
                f"(symbol, CAST(date AS STRING), interval) IN ({tuples})"
            )

        if intraday_keys:
            tuples = ", ".join(
                f"('{s}', '{d}', '{iv}', '{bt}')"
                for s, d, iv, bt in intraday_keys
            )
            predicates.append(
                f"(symbol, CAST(date AS STRING), interval, "
                f"CAST(bar_time_utc AS STRING)) IN ({tuples})"
            )

        if not predicates:
            return {}

        where_clause = " OR ".join(f"({p})" for p in predicates)

        # Single query — window function picks latest version per key
        query = f"""
            SELECT *
            FROM (
                SELECT *,
                    ROW_NUMBER() OVER (
                        PARTITION BY symbol, date, interval, bar_time_utc
                        ORDER BY record_version DESC
                    ) AS _rn
                FROM {full_table}
                WHERE {where_clause}
            )
            WHERE _rn = 1
        """

        try:
            df = self._spark.sql(query)
            result: _ExistingRecordMap = {}
            for row in df.collect():
                r = row.asDict()
                r.pop("_rn", None)
                k = self._record_key(r)
                result[k] = r
            return result
        except Exception as e:
            logger.warning(
                f"Bulk fetch from {full_table} failed: {e}. "
                f"Treating all records as new (safe — Bronze is append-only)."
            )
            return {}

    # ------------------------------------------------------------------
    # Write helpers
    # ------------------------------------------------------------------

    def _append_records(self, records: List[dict], table_name: str) -> None:
        """Append records to table (local or Spark)."""
        if self._mode == "local":
            if table_name not in self._local_store:
                self._local_store[table_name] = []
            self._local_store[table_name].extend(records)
        else:
            self._spark_append(records, table_name)

    def _spark_append(self, records: List[dict], table_name: str) -> None:
        """Append records to Delta table using Spark."""
        full_table = f"{self._catalog}.{self._schema}.{table_name}"
        df = self._spark.createDataFrame(records)
        df.write.format("delta").mode("append").saveAsTable(full_table)

    def _spark_count_records(
        self,
        symbol: str,
        interval: str,
        table_name: str,
    ) -> int:
        """Count total Bronze records for a symbol + interval."""
        full_table = f"{self._catalog}.{self._schema}.{table_name}"
        try:
            row = self._spark.sql(f"""
                SELECT COUNT(*) AS cnt FROM {full_table}
                WHERE symbol = '{symbol}' AND interval = '{interval}'
            """).first()
            return int(row["cnt"]) if row else 0
        except Exception as e:
            logger.warning(f"Could not count records in {full_table}: {e}")
            return 0
