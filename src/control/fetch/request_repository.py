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
    ):
        """
        Args:
            spark:      SparkSession (Databricks serverless or Connect)
            catalog:    Unity Catalog name (e.g. "tradeanalytics")
            raw_bucket: raw landing bucket (e.g. "handh-trade-raw-use1")
            fs_put:     callable(path, contents, overwrite) — dbutils.fs.put
        """
        self._spark      = spark
        self._table      = f"{catalog}.control.fetch_request"
        self._raw_bucket = raw_bucket
        self._fs_put     = fs_put

    def pending_prefix(self) -> str:
        return f"s3://{self._raw_bucket}/control/fetch/pending"

    def save_all(self, requests: List[FetchRequest]) -> int:
        """Insert Delta rows, then write one manifest per request. Returns count."""
        if not requests:
            return 0

        self._insert_rows(requests)
        for req in requests:
            self._write_manifest(req)

        logger.info(
            f"FetchRequestRepository: {len(requests)} requests saved "
            f"({self._table} + {self.pending_prefix()}/)"
        )
        return len(requests)

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
                r.vendor, r.stream, r.bar_interval, r.start_date, r.end_date,
                r.load_type, r.task_type, self._manifest_path(r),
            )
            for r in requests
        ]
        df = self._spark.createDataFrame(rows, schema=schema)
        df.createOrReplaceTempView("_new_fetch_requests")
        self._spark.sql(f"""
            INSERT INTO {self._table}
                (request_key, batch_id, instrument_id, symbol, vendor, stream,
                 bar_interval, start_date, end_date, load_type, task_type,
                 s3_manifest_path)
            SELECT request_key, batch_id, instrument_id, symbol, vendor, stream,
                   bar_interval, start_date, end_date, load_type, task_type,
                   s3_manifest_path
            FROM _new_fetch_requests
        """)

    def _manifest_path(self, req: FetchRequest) -> str:
        return f"{self.pending_prefix()}/{req.request_key}.json"

    def _write_manifest(self, req: FetchRequest) -> None:
        self._fs_put(
            self._manifest_path(req),
            json.dumps(req.manifest_dict(), indent=2),
            True,  # overwrite — planner re-runs must be idempotent
        )
