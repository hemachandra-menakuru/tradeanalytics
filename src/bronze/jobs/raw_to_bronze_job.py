"""
RawToBronzeJob — Stage 4 of the Two-Plane ingestion workflow (CLAUDE.md §14).

Turns raw payloads landed by the EC2 fetch agent into validated Bronze rows,
and closes the bookkeeping loop on control.fetch_request.

Phases per run:
  ① RECONCILE  read done/ receipts  → fetch_request PENDING → LANDED
               read failed/ manifests → fetch_request PENDING → FAILED
  ② INGEST     for each LANDED request: read payload from s3_data_path,
               bars → OHLCVRecord dicts → DataQualityValidator →
               BronzeWriter (append-only, dedup-classify)
  ③ BOOKKEEP   watermark upsert (instrument_id + stream) ·
               fetch_request → INGESTED · control.job_run_log row (per request)
  ④ ARCHIVE    done/ receipts → done/archive/<ingest-date>/

Idempotency: re-running reconcile re-applies the same statuses; INGESTED
requests are never re-ingested; if a crash happens after Bronze write but
before status update, the re-run's Bronze write dedups to zero new rows and
the status update completes (at-least-once + idempotent, end to end).

All I/O is S3 (via injected fs helpers) + Delta — serverless-safe, no egress.
The transform (payload → records) is a pure function, unit-tested separately.
"""

from __future__ import annotations

import json
import logging
from datetime import date, datetime, timezone
from typing import Callable, List, Optional

from src.shared.config.config_loader import ConfigNode
from src.bronze.validation.validator import DataQualityValidator
from src.bronze.writers.bronze_writer import BronzeWriter
from src.control.watermark.delta_watermark_store import DeltaWatermarkStore

logger = logging.getLogger(__name__)


def payload_to_records(payload: dict) -> List[dict]:
    """
    Pure transform: one landed raw payload (ohlcv_json_v1) → OHLCVRecord dicts
    exactly as providers emit them (symbol/bar_date/bar_interval/source + OHLCV).
    Kept free of I/O so it is trivially unit-testable.
    """
    fmt = payload.get("payload_format")
    if fmt != "ohlcv_json_v1":
        raise ValueError(f"Unsupported payload_format '{fmt}' — expected ohlcv_json_v1")

    inst  = payload["instrument"]
    fetch = payload["fetch"]
    records = []
    for bar in payload["bars"]:
        records.append({
            "symbol":       inst["symbol"],
            "bar_date":     bar["bar_date"],
            "bar_interval": fetch["bar_interval"],
            "source":       inst["vendor"],          # data vendor (ibkr); transport is in fetched_by
            "open":         float(bar["open"]),
            "high":         float(bar["high"]),
            "low":          float(bar["low"]),
            "close":        float(bar["close"]),
            "volume":       int(bar["volume"]),
            "currency":     inst.get("currency") or "USD",
            "exchange":     inst.get("exchange_mic"),
            "dividend_amount":      0.0,
            "is_ex_dividend_date":  False,
            "is_ex_split_date":     False,
            "has_corporate_action": False,
            "trading_halt":         False,
            "is_trading_day":       True,
            "is_amended":           False,
            "data_as_of":           payload.get("fetched_at"),
            "source_version":       payload.get("fetched_by"),
        })
    return records


class RawToBronzeJob:

    def __init__(
        self,
        config: ConfigNode,
        spark,
        fs_ls:   Callable,          # dbutils.fs.ls
        fs_head: Callable,          # lambda p: dbutils.fs.head(p, MAX)
        fs_put:  Callable,          # dbutils.fs.put
        fs_rm:   Callable,          # dbutils.fs.rm
        stream_name: str = "daily",
        vendor: str = "ibkr",
    ):
        self._config      = config
        self._spark       = spark
        self._stream_name = stream_name
        self._stream_cfg  = getattr(config, stream_name)
        self._vendor      = vendor
        self._catalog     = config.databricks.catalog
        self._raw_bucket  = config.aws.s3.raw

        self._fs_ls, self._fs_head, self._fs_put, self._fs_rm = fs_ls, fs_head, fs_put, fs_rm

        schema = config.databricks.schemas.bronze
        self._validator  = DataQualityValidator.for_stream(config, stream_name)
        self._writer     = BronzeWriter(mode="spark", spark=spark,
                                        catalog=self._catalog, schema=schema)
        self._watermarks = DeltaWatermarkStore(mode="spark", spark=spark, catalog=self._catalog)

        import os
        self._pipeline_version = os.environ.get("PIPELINE_VERSION", "unknown")

        q = f"s3://{self._raw_bucket}/control/fetch/{vendor}"
        self._done_prefix, self._failed_prefix = f"{q}/done", f"{q}/failed"
        self._archive_prefix = f"{q}/done/archive"

    # ── phase ① reconcile ─────────────────────────────────────────────────────

    def reconcile(self) -> dict:
        """Apply agent receipts (done/ + failed/) to control.fetch_request."""
        landed = failed = 0

        for m in self._list_receipts(self._done_prefix):
            if m.get("task_type") != "FETCH_OHLCV":
                continue   # e.g. QUALIFY receipts — different flow, leave in place
            n = self._spark.sql(f"""
                UPDATE {self._catalog}.control.fetch_request
                SET status = 'LANDED',
                    s3_data_path  = '{m["s3_data_path"]}',
                    record_count  = {int(m.get("record_count", 0))},
                    attempt_count = {int(m.get("attempt_count", 1))},
                    landed_at     = '{m["landed_at"]}'
                WHERE request_key = '{m["request_key"]}' AND status = 'PENDING'
            """)
            landed += 1

        for m in self._list_receipts(self._failed_prefix):
            if m.get("task_type") != "FETCH_OHLCV":
                continue
            err = (m.get("error_message") or "unknown").replace("'", "''")[:500]
            self._spark.sql(f"""
                UPDATE {self._catalog}.control.fetch_request
                SET status = 'FAILED',
                    error_message = '{err}',
                    attempt_count = {int(m.get("attempt_count", 1))}
                WHERE request_key = '{m["request_key"]}' AND status IN ('PENDING','LANDED')
            """)
            failed += 1

        logger.info(f"reconcile: {landed} receipt(s) → LANDED, {failed} → FAILED")
        return {"landed": landed, "failed": failed}

    # ── phase ② + ③ ingest & bookkeep ─────────────────────────────────────────

    def ingest(self, dry_run: bool = False) -> dict:
        rows = self._spark.sql(f"""
            SELECT request_key, batch_id, instrument_id, symbol, vendor,
                   bar_interval, load_type, s3_data_path, record_count
            FROM {self._catalog}.control.fetch_request
            WHERE stream = '{self._stream_name}' AND vendor = '{self._vendor}'
              AND status = 'LANDED' AND s3_data_path IS NOT NULL
            ORDER BY symbol, start_date
        """).collect()

        if not rows:
            logger.info("ingest: nothing LANDED to ingest")
            return {"requests_ingested": 0, "records_written": 0, "records_rejected": 0}

        total_written = total_rejected = ingested = 0

        for r in rows:
            started = datetime.now(timezone.utc)
            payload = json.loads(self._fs_head(r.s3_data_path))
            records = payload_to_records(payload)

            if len(records) != r.record_count:
                logger.warning(
                    f"[{r.request_key}] payload has {len(records)} bars but receipt "
                    f"said {r.record_count} — proceeding with payload contents"
                )

            if dry_run:
                logger.info(f"[{r.request_key}] DRY RUN — would ingest {len(records)} records")
                continue

            ingestion_type = "backfill" if r.load_type in (
                "INITIAL_LOAD", "HISTORY_EXTENSION", "FORCE_RELOAD") else "scheduled"

            vs = self._validator.validate_batch(
                symbol=r.symbol,
                interval=r.bar_interval,
                batch_id=r.batch_id,
                raw_records=records,
                pipeline_version=self._pipeline_version,
                ingestion_type=ingestion_type,
                instrument_id=r.instrument_id,
                ingested_by=payload.get("fetched_by", f"fetch_agent_{self._vendor}"),
            )
            wr = self._writer.write_batch(
                symbol=r.symbol,
                interval=r.bar_interval,
                batch_id=r.batch_id,
                clean_records=vs.writable_records,
                rejected_records=vs.rejected_records,
                main_table=self._stream_cfg.table,
                rejected_table=self._stream_cfg.rejected_table,
            )

            self._update_watermark(r, vs)
            self._mark_ingested(r.request_key)
            self._write_job_run_log(r, vs, wr, started)

            total_written  += wr.records_written + wr.records_amended
            total_rejected += wr.rejected_written
            ingested       += 1
            logger.info(
                f"[{r.request_key}] INGESTED — {wr.records_written} new, "
                f"{wr.records_amended} amended, {wr.records_skipped} skipped, "
                f"{wr.rejected_written} rejected"
            )

        summary = {"requests_ingested": ingested,
                   "records_written": total_written,
                   "records_rejected": total_rejected}
        logger.info(f"ingest complete: {summary}")
        return summary

    # ── phase ④ archive ───────────────────────────────────────────────────────

    def archive_done(self) -> int:
        """Move receipts of INGESTED requests to done/archive/<today>/."""
        ingested_keys = {
            row.request_key for row in self._spark.sql(f"""
                SELECT request_key FROM {self._catalog}.control.fetch_request
                WHERE stream = '{self._stream_name}' AND status = 'INGESTED'
            """).collect()
        }
        moved = 0
        today = date.today().isoformat()
        try:
            entries = list(self._fs_ls(self._done_prefix))
        except Exception:
            return 0
        for f in entries:
            name = f.path.rstrip("/").split("/")[-1]
            if not name.endswith(".json"):
                continue
            if name[:-5] in ingested_keys or name.replace(".json", "") in ingested_keys:
                content = self._fs_head(f.path)
                self._fs_put(f"{self._archive_prefix}/{today}/{name}", content, True)
                self._fs_rm(f.path)
                moved += 1
        logger.info(f"archive: {moved} receipt(s) → done/archive/{today}/")
        return moved

    def run(self, dry_run: bool = False) -> dict:
        rec = self.reconcile()
        ing = self.ingest(dry_run=dry_run)
        arch = self.archive_done() if not dry_run else 0
        return {**rec, **ing, "receipts_archived": arch, "dry_run": dry_run}

    # ── internals ─────────────────────────────────────────────────────────────

    def _list_receipts(self, prefix: str) -> List[dict]:
        out = []
        try:
            entries = list(self._fs_ls(prefix))
        except Exception:
            return out
        for f in entries:
            if f.path.rstrip("/").endswith(".json"):
                try:
                    out.append(json.loads(self._fs_head(f.path)))
                except Exception as e:
                    logger.error(f"unreadable receipt {f.path}: {e}")
        return out

    def _update_watermark(self, r, vs) -> None:
        dates = [rec.get("bar_date") for rec in vs.writable_records if rec.get("bar_date")]
        if not dates:
            return
        min_d, max_d = date.fromisoformat(min(dates)), date.fromisoformat(max(dates))
        existing = self._watermarks.get_watermark(r.instrument_id, self._stream_name)
        if existing is not None:
            min_d = min(min_d, existing.earliest_date)
            max_d = max(max_d, existing.latest_date)
        count = self._writer.get_record_count(
            symbol=r.symbol, interval=r.bar_interval, table_name=self._stream_cfg.table)
        self._watermarks.update_watermark(
            instrument_id=r.instrument_id, stream=self._stream_name,
            interval=r.bar_interval, earliest_date=min_d, latest_date=max_d,
            record_count=count, batch_id=r.batch_id,
            mode=r.load_type.lower(), status="success", vendor=r.vendor,
        )

    def _mark_ingested(self, request_key: str) -> None:
        self._spark.sql(f"""
            UPDATE {self._catalog}.control.fetch_request
            SET status = 'INGESTED', ingested_at = current_timestamp()
            WHERE request_key = '{request_key}' AND status = 'LANDED'
        """)

    def _write_job_run_log(self, r, vs, wr, started) -> None:
        """First writer of control.job_run_log — one audit row per request."""
        try:
            self._spark.sql(f"""
                INSERT INTO {self._catalog}.control.job_run_log
                    (job_run_id, instrument_id, stream, load_type,
                     records_new, records_amended, records_rejected,
                     status, started_at, completed_at, vendor)
                VALUES
                    ('{r.batch_id}/{r.request_key}', {r.instrument_id},
                     '{self._stream_name}', '{r.load_type}',
                     {wr.records_written}, {wr.records_amended}, {wr.rejected_written},
                     'success', '{started.isoformat()}', current_timestamp(),
                     '{r.vendor}')
            """)
        except Exception as e:
            # Audit failure must never fail ingestion — log loudly, continue.
            logger.error(f"job_run_log write failed (non-fatal): {e}")
