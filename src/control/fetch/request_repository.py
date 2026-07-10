"""
FetchRequestRepository — persists FetchRequests to both sides of the
two-plane contract:

  1. control.fetch_request Delta rows (source of truth, audit) — Spark
  2. s3://<raw>/control/fetch/pending/<request_key>.json manifests — the
     agent's inbox, written via an injected fs_put callable
     (dbutils.fs.put on Databricks; a mock in unit tests)

Write order is Delta FIRST, manifests SECOND: if the job dies between the two,
a PENDING row without a manifest is caught by the monitor (stuck PENDING) and
re-emitted; a manifest without a row would be untracked work — never allowed.

Only Databricks writes Delta. The agent signals by moving manifests between
S3 prefixes; the ingestion job reconciles those moves back into this table.
"""

from __future__ import annotations

import json
import logging
from typing import Callable, List

from src.control.fetch.models import FetchRequest

logger = logging.getLogger(__name__)


class FetchRequestRepository:

    def __init__(
        self,
        spark,
        catalog: str,
        raw_bucket: str,
        fs_put: Callable[[str, str, bool], None],
        manifest_workers: int = 16,
    ):
        """
        Args:
            spark:      SparkSession (Databricks serverless or Connect)
            catalog:    Unity Catalog name (e.g. "tradeanalytics")
            raw_bucket: raw landing bucket (e.g. "handh-trade-raw-use1")
            fs_put:     callable(path, contents, overwrite) — dbutils.fs.put
            manifest_workers: thread-pool size for concurrent manifest writes
                              (I/O-bound S3 puts; 16 is a safe default)
        """
        self._spark      = spark
        self._table      = f"{catalog}.control.fetch_request"
        self._raw_bucket = raw_bucket
        self._fs_put     = fs_put
        self._manifest_workers = manifest_workers

    def pending_prefix(self, vendor: str) -> str:
        # Vendor-scoped queues: each vendor's agent polls only its own inbox
        # (consumer-aligned queues; IAM locks each agent to its own prefix)
        return f"s3://{self._raw_bucket}/control/fetch/{vendor}/pending"

    def save_all(self, requests: List[FetchRequest]) -> int:
        """Insert Delta rows, then write one manifest per request. Returns count.

        Manifest writes are I/O-bound (one S3 put each) and independent, so they
        run in a thread pool — sequential writes dominated planner runtime
        (~110s for 435 manifests → hours at 2k tickers). Parallelising cuts that
        to roughly runtime/threads. Delta insert stays a single transaction
        first, so a manifest-write failure leaves a PENDING row the planner's
        orphan-repair heals on the next run.
        """
        if not requests:
            return 0

        self._insert_rows(requests)
        self._write_manifests_parallel(requests)

        logger.info(
            f"FetchRequestRepository: {len(requests)} requests saved to {self._table}"
        )
        return len(requests)

    def _write_manifests_parallel(self, requests: List[FetchRequest]) -> None:
        """Write all manifests concurrently (I/O-bound). Raises if any fail."""
        from concurrent.futures import ThreadPoolExecutor

        if len(requests) == 1:
            self._write_manifest(requests[0])
            return

        max_workers = min(self._manifest_workers, len(requests))
        errors = []
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = {
                pool.submit(self._write_manifest, req): req.request_key
                for req in requests
            }
            for fut in futures:
                try:
                    fut.result()
                except Exception as e:
                    errors.append(f"{futures[fut]}: {e}")

        if errors:
            raise RuntimeError(
                f"{len(errors)}/{len(requests)} manifest writes failed: "
                f"{errors[:3]}{'…' if len(errors) > 3 else ''}"
            )

    # ── internals ─────────────────────────────────────────────────────────────

    def _insert_rows(self, requests: List[FetchRequest]) -> None:
        """INSERT ... SELECT with explicit columns — request_id is
        GENERATED ALWAYS AS IDENTITY and must not appear in the column list."""
        from pyspark.sql.types import (
            StructType, StructField, StringType, LongType, DateType,
        )

        schema = StructType([
            StructField("request_key",   StringType(), False),
            StructField("batch_id",      StringType(), False),
            StructField("instrument_id", LongType(),   False),
            StructField("symbol",        StringType(), False),
            StructField("vendor",        StringType(), False),
            StructField("vendor_instrument_id", StringType(), True),
            StructField("stream",        StringType(), False),
            StructField("bar_interval",  StringType(), False),
            StructField("start_date",    DateType(),   False),
            StructField("end_date",      DateType(),   False),
            StructField("load_type",     StringType(), False),
            StructField("task_type",     StringType(), False),
            StructField("s3_manifest_path", StringType(), False),
        ])
        rows = [
            (
                r.request_key, r.batch_id, r.instrument_id, r.symbol,
                r.vendor, r.vendor_instrument_id, r.stream, r.bar_interval,
                r.start_date, r.end_date, r.load_type, r.task_type,
                self._manifest_path(r),
            )
            for r in requests
        ]
        df = self._spark.createDataFrame(rows, schema=schema)
        df.createOrReplaceTempView("_new_fetch_requests")
        self._spark.sql(f"""
            INSERT INTO {self._table}
                (request_key, batch_id, instrument_id, symbol, vendor,
                 vendor_instrument_id, stream, bar_interval, start_date,
                 end_date, load_type, task_type, s3_manifest_path)
            SELECT request_key, batch_id, instrument_id, symbol, vendor,
                   vendor_instrument_id, stream, bar_interval, start_date,
                   end_date, load_type, task_type, s3_manifest_path
            FROM _new_fetch_requests
        """)

    def _manifest_path(self, req: FetchRequest) -> str:
        return f"{self.pending_prefix(req.vendor)}/{req.request_key}.json"

    def _write_manifest(self, req: FetchRequest) -> None:
        self._fs_put(
            self._manifest_path(req),
            json.dumps(req.manifest_dict(), indent=2),
            True,  # overwrite — planner re-runs must be idempotent
        )
