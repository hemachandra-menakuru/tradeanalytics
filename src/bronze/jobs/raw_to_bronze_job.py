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
  ④ CLEANUP    delete INGESTED done/ receipts (transient signalling; the raw
               payloads under s3_data_path are preserved untouched)

Idempotency (rewritten 2026-07-10 — three hard guarantees):
  • RECONCILE is set-based (ONE MERGE, not one UPDATE per receipt) and only
    transitions PENDING rows, so re-running never regresses LANDED/INGESTED and
    a stale failed/ receipt can never overwrite a good LANDED (success wins).
  • INGEST guards the skip_dedup backfill path: before re-appending, it checks
    whether this (symbol, batch_id) is already in Bronze. On a crash-then-rerun
    this prevents duplicate rows — the gap the earlier skip_dedup optimisation
    silently opened (a bypassed Layer-2 scan cannot dedup a replay).
  • FAULT ISOLATION: each instrument group is wrapped in try/except. A poison
    payload marks nothing FAILED — it is left LANDED so the next run retries and
    the monitor flags it if it stays stuck; the other 34 instruments still load.

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
        self._bronze_schema = schema   # needed for the skip_dedup idempotency guard
        self._validator  = DataQualityValidator.for_stream(config, stream_name)
        self._writer     = BronzeWriter(mode="spark", spark=spark,
                                        catalog=self._catalog, schema=schema)
        self._watermarks = DeltaWatermarkStore(mode="spark", spark=spark, catalog=self._catalog)

        import os
        self._pipeline_version = os.environ.get("PIPELINE_VERSION", "unknown")

        q = f"s3://{self._raw_bucket}/control/fetch/{vendor}"
        self._done_prefix, self._failed_prefix = f"{q}/done", f"{q}/failed"

    # ── phase ① reconcile ─────────────────────────────────────────────────────

    def reconcile(self) -> dict:
        """Apply agent receipts (done/ + failed/) to control.fetch_request in ONE MERGE.

        Was: one Delta UPDATE per receipt (435 sequential file-rewrites — the
        phase that stalled the run). Now: read all receipts, resolve each
        request_key to a single target status (LANDED wins over FAILED so a
        stale failed/ receipt can never clobber a good landing), then one
        set-based MERGE. Values flow through a typed DataFrame, not f-strings,
        so paths/timestamps/messages can't break or inject SQL. The MERGE only
        touches status='PENDING' rows, so it is idempotent on re-run and never
        regresses LANDED/INGESTED.
        """
        # request_key → (new_status, s3_data_path, record_count, landed_at, error, attempt)
        actions: dict = {}

        # FAILED first, then LANDED overlays it — done/ (success) wins on conflict.
        for m in self._list_receipts(self._failed_prefix):
            if m.get("task_type") != "FETCH_OHLCV":
                continue
            key = m.get("request_key")
            if not key:
                continue
            err = (m.get("error_message") or "unknown")[:500]
            actions[key] = ("FAILED", None, None, None, err, int(m.get("attempt_count", 1)))

        for m in self._list_receipts(self._done_prefix):
            if m.get("task_type") != "FETCH_OHLCV":
                continue   # e.g. QUALIFY receipts — different flow, leave in place
            key = m.get("request_key")
            s3  = m.get("s3_data_path")
            if not key or not s3:
                logger.error(f"reconcile: done receipt missing request_key/s3_data_path — skipped: {m.get('request_key')}")
                continue
            actions[key] = (
                "LANDED", s3, int(m.get("record_count", 0)),
                m.get("landed_at"), None, int(m.get("attempt_count", 1)),
            )

        if not actions:
            logger.info("reconcile: no receipts to apply")
            return {"receipts_seen": 0, "requests_updated": 0}

        updated = self._apply_reconcile(actions)
        landed_n = sum(1 for v in actions.values() if v[0] == "LANDED")
        failed_n = len(actions) - landed_n
        logger.info(
            f"reconcile: {len(actions)} receipt(s) resolved "
            f"({landed_n} LANDED, {failed_n} FAILED) → {updated} PENDING row(s) transitioned"
        )
        return {"receipts_seen": len(actions), "requests_updated": updated,
                "landed": landed_n, "failed": failed_n}

    def _apply_reconcile(self, actions: dict) -> int:
        """Build a typed source view from resolved receipt actions and MERGE once."""
        from pyspark.sql.types import (
            StructType, StructField, StringType, LongType,
        )
        schema = StructType([
            StructField("request_key",   StringType(), False),
            StructField("new_status",    StringType(), False),
            StructField("s3_data_path",  StringType(), True),
            StructField("record_count",  LongType(),   True),
            StructField("landed_at",     StringType(), True),   # ISO string → CAST in SQL
            StructField("error_message", StringType(), True),
            StructField("attempt_count", LongType(),   True),
        ])
        rows = [
            (k, v[0], v[1], v[2], v[3], v[4], v[5])
            for k, v in actions.items()
        ]
        df = self._spark.createDataFrame(rows, schema=schema)
        df.createOrReplaceTempView("_reconcile_src")

        result = self._spark.sql(f"""
            MERGE INTO {self._catalog}.control.fetch_request AS t
            USING _reconcile_src AS s
            ON t.request_key = s.request_key
            WHEN MATCHED AND t.status = 'PENDING' AND s.new_status = 'LANDED' THEN UPDATE SET
                t.status        = 'LANDED',
                t.s3_data_path  = s.s3_data_path,
                t.record_count  = s.record_count,
                t.attempt_count = s.attempt_count,
                t.landed_at     = CAST(s.landed_at AS TIMESTAMP)
            WHEN MATCHED AND t.status = 'PENDING' AND s.new_status = 'FAILED' THEN UPDATE SET
                t.status        = 'FAILED',
                t.error_message = s.error_message,
                t.attempt_count = s.attempt_count
        """)
        # Delta returns operation metrics; fall back to -1 if unavailable.
        try:
            return int(result.first()["num_updated_rows"])
        except Exception:
            return -1

    # ── phase ② + ③ ingest & bookkeep ─────────────────────────────────────────

    def ingest(self, dry_run: bool = False) -> dict:
        """
        Ingest all LANDED receipts, GROUPED BY instrument (ENH-1).

        Prior design looped per-receipt: each chunk did its own validate +
        write_batch, and write_batch's dedup joins against the FULL Bronze
        table. 560 receipts × full-table scan on a table that grows during the
        run = O(N^2) — a 35-instrument/15yr backfill took hours. Grouping all
        of an instrument's ~16 chunks into ONE validate + ONE write collapses
        that to ONE dedup scan per instrument (35, not 560). Idempotent as
        before: only LANDED rows are processed; a re-run skips INGESTED.
        """
        rows = self._spark.sql(f"""
            SELECT request_key, batch_id, instrument_id, symbol, vendor,
                   bar_interval, load_type, s3_data_path, record_count,
                   start_date, end_date
            FROM {self._catalog}.control.fetch_request
            WHERE stream = '{self._stream_name}' AND vendor = '{self._vendor}'
              AND status = 'LANDED' AND s3_data_path IS NOT NULL
            ORDER BY symbol, start_date
        """).collect()

        if not rows:
            logger.info("ingest: nothing LANDED to ingest")
            return {"requests_ingested": 0, "instruments": 0, "records_written": 0,
                    "records_rejected": 0, "groups_failed": 0, "batch_ids": []}

        # Group receipts by instrument (symbol + interval share instrument_id/vendor)
        groups: dict = {}
        for r in rows:
            groups.setdefault((r.instrument_id, r.symbol, r.bar_interval), []).append(r)

        total_written = total_rejected = ingested = groups_failed = 0
        batch_ids: set = set()
        # Bookkeeping is BATCHED, not per-group: accumulate here, flush once below.
        # 65 instruments × (mark UPDATE + job_log INSERT) = 130 Delta commits →
        # 2 commits. Safe because marking-after-writing widens the crash window
        # only for work the skip_dedup idempotency guard already re-absorbs.
        all_ingested_keys: List[str] = []
        all_job_log_rows: list = []
        # Watermark states buffered here, flushed as ONE set-based MERGE below.
        # ~12 scalar fields × N instruments ≈ <1MB even at 2k — safe in the driver
        # (unlike buffering data rows). Per-instrument MERGE + get_watermark reads
        # (65 × ~7s) collapse to a single MERGE.
        self._wm_buffer: list = []

        for (instrument_id, symbol, interval), grp in groups.items():
            # ── fault isolation: one poison instrument must not block the rest ──
            try:
                self._ingest_group(instrument_id, symbol, interval, grp, dry_run)
            except Exception as e:
                # Leave the group's requests LANDED (NOT marked FAILED): a re-run
                # retries safely (the skip_dedup guard prevents duplicate appends),
                # and the monitor alerts if they stay stuck. Terminal-failing a
                # transient S3/validate blip would be worse than an auto-retry.
                groups_failed += 1
                logger.error(
                    f"[{symbol}] ingest failed — left LANDED for retry: {e}",
                    exc_info=True,
                )
                continue

            rep = grp[0]
            batch_ids.add(rep.batch_id)
            res = self._last_group_result
            total_written  += res["written"]
            total_rejected += res["rejected"]
            ingested       += len(grp) if not dry_run else 0
            all_ingested_keys.extend(res["keys_to_mark"])
            if res["job_log_row"] is not None:
                all_job_log_rows.append(res["job_log_row"])

        # ── batched flush ──
        # Watermark first (reflects Bronze truth), then mark, then audit. Each is
        # idempotent; a crash between them self-heals on the next run.
        if not dry_run and self._wm_buffer:
            try:
                self._watermarks.update_watermarks_bulk(self._wm_buffer)
            except Exception as e:
                # Non-fatal: dates self-heal via LEAST/GREATEST next run; count
                # is informational. Better than failing a run whose data is safe.
                logger.error(f"watermark bulk flush failed (non-fatal, self-heals): {e}")
        if not dry_run and all_ingested_keys:   # core → may raise (re-run is safe)
            self._mark_ingested(all_ingested_keys)
        if not dry_run and all_job_log_rows:    # audit → non-fatal
            self._write_job_run_log_bulk(all_job_log_rows)

        summary = {"requests_ingested": ingested,
                   "instruments": len(groups),
                   "records_written": total_written,
                   "records_rejected": total_rejected,
                   "groups_failed": groups_failed,
                   "batch_ids": sorted(batch_ids)}
        logger.info(f"ingest complete: {summary}")
        return summary

    def _ingest_group(self, instrument_id, symbol, interval, grp, dry_run) -> None:
        """Ingest one instrument's chunks. Sets self._last_group_result for the caller.

        Fault-isolated by the caller; each guard below is a distinct edge case
        the earlier code did not handle (duplicate replay, zero-history loop)."""
        self._last_group_result = {"written": 0, "rejected": 0,
                                   "keys_to_mark": [], "job_log_row": None}
        started = datetime.now(timezone.utc)
        rep = grp[0]   # representative for batch_id / load_type
        keys = [r.request_key for r in grp]

        # Concatenate ALL chunk payloads for this instrument into one record set
        records: List[dict] = []
        for r in grp:
            payload = json.loads(self._fs_head(r.s3_data_path))
            records.extend(payload_to_records(payload))

        if dry_run:
            logger.info(f"[{symbol}] DRY RUN — would ingest {len(records)} records "
                        f"from {len(grp)} chunk(s)")
            return

        ingestion_type = "backfill" if rep.load_type in (
            "INITIAL_LOAD", "HISTORY_EXTENSION", "FORCE_RELOAD") else "scheduled"

        # Skip the Layer-2 full-table dedup scan when the incoming dates cannot
        # overlap existing Bronze (INITIAL_LOAD / HISTORY_EXTENSION). FORCE_RELOAD
        # deliberately overlaps → keep dedup. Layer 3 (Silver window) is the
        # correctness backstop for query reads either way.
        skip_dedup = rep.load_type in ("INITIAL_LOAD", "HISTORY_EXTENSION")

        # ── A1: idempotency guard for the skip_dedup replay hole ──
        # skip_dedup bypasses the dedup scan, so a crash-then-rerun would blindly
        # re-append every bar → duplicate Bronze rows. If this (symbol, batch_id)
        # is already present, the prior run's append committed; just finish the
        # bookkeeping (mark INGESTED) without re-writing.
        if skip_dedup and self._batch_already_in_bronze(symbol, rep.batch_id):
            logger.warning(
                f"[{symbol}] batch {rep.batch_id} already in Bronze — skipping "
                f"re-append (idempotent replay); will mark {len(grp)} chunk(s) INGESTED"
            )
            # Still buffer the watermark (rows_written=0 → no double-count) using
            # payload dates, so a crash-then-replay doesn't lose the watermark.
            self._buffer_watermark_from_records(rep, records, rows_written=0)
            self._last_group_result["keys_to_mark"] = keys
            return

        vs = self._validator.validate_batch(          # ONE validate for the instrument
            symbol=symbol, interval=interval, batch_id=rep.batch_id,
            raw_records=records,
            pipeline_version=self._pipeline_version,
            ingestion_type=ingestion_type,
            instrument_id=instrument_id,
            ingested_by=f"fetch_agent_{self._vendor}",
        )

        # ── A3: zero-history instrument (IBKR returned no bars at all) ──
        # Without a watermark the planner re-issues INITIAL_LOAD every run → an
        # endless full-history re-fetch of an instrument that has no data. Mark
        # the requests done and write a zero-count watermark so the planner
        # switches to (cheap) incremental. Sentinel dates = the target end-date;
        # NULL is not an option — DeltaWatermarkStore reads dates unconditionally.
        if not vs.writable_records and not vs.rejected_records:
            logger.warning(
                f"[{symbol}] 0 usable bars across {len(grp)} chunk(s) — marking "
                f"INGESTED and buffering zero-count watermark to stop re-INITIAL_LOAD"
            )
            end_dates = [r.end_date for r in grp if getattr(r, "end_date", None)]
            sentinel = max(end_dates) if end_dates else date.today()
            self._buffer_watermark(rep, sentinel, sentinel, rows_written=0)
            self._last_group_result["keys_to_mark"] = keys
            return

        wr = self._writer.write_batch(                 # dedup skipped for backfills
            symbol=symbol, interval=interval, batch_id=rep.batch_id,
            clean_records=vs.writable_records,
            rejected_records=vs.rejected_records,
            main_table=self._stream_cfg.table,
            rejected_table=self._stream_cfg.rejected_table,
            skip_dedup=skip_dedup,
        )

        # Buffer the watermark (flushed as one MERGE at end); rows_written is the
        # delta appended THIS run — additive count, no per-instrument scan.
        self._buffer_watermark_from_records(
            rep, vs.writable_records, wr.records_written + wr.records_amended)
        completed = datetime.now(timezone.utc)

        self._last_group_result = {
            "written":  wr.records_written + wr.records_amended,
            "rejected": wr.rejected_written,
            "keys_to_mark": keys,                      # batched-marked by the caller
            "job_log_row": self._build_job_log_row(rep, wr, started, completed),
        }
        logger.info(
            f"[{symbol}] INGESTED {len(grp)} chunk(s) — {wr.records_written} new, "
            f"{wr.records_amended} amended, {wr.records_skipped} skipped, "
            f"{wr.rejected_written} rejected"
        )

    # ── phase ④ archive ───────────────────────────────────────────────────────

    def archive_done(self, batch_ids: Optional[List[str]] = None) -> int:
        """DELETE INGESTED receipts from done/ (was: copy-to-archive/ then delete).

        The done/ receipts are transient *signalling* — the reprocessable raw
        payloads live under s3_data_path (a different prefix) and are NEVER
        touched, so preserving receipts buys nothing the fetch_request Delta row
        (status/landed_at/s3_data_path/record_count) doesn't already hold. So we
        just delete them, which drops 2 of the 3 S3 ops per file (head+put gone).

        Selective by design — only INGESTED keys are removed, so a stuck-LANDED
        receipt (a group left for retry) stays in done/. That is exactly why a
        bulk `rm -r done/` is unsafe and we filter per file. Non-fatal and
        idempotent (an already-gone file just isn't there next run). Scoped to
        this run's batch(es) so the lookup stays bounded.

        NEXT WIN (deferred, needs validation): replace the sequential fs_rm with
        one boto3 delete_objects call per 1,000 keys — but only after confirming
        serverless exposes working boto3 S3 credentials.
        """
        # Bound the INGESTED lookup to the batches we just processed.
        where_batch = ""
        if batch_ids:
            keys_sql = ", ".join(f"'{b}'" for b in batch_ids)
            where_batch = f" AND batch_id IN ({keys_sql})"
        try:
            ingested_keys = {
                row.request_key for row in self._spark.sql(f"""
                    SELECT request_key FROM {self._catalog}.control.fetch_request
                    WHERE stream = '{self._stream_name}' AND vendor = '{self._vendor}'
                      AND status = 'INGESTED'{where_batch}
                """).collect()
            }
        except Exception as e:
            logger.error(f"archive: could not read INGESTED keys (skipping): {e}")
            return 0

        if not ingested_keys:
            return 0

        deleted = 0
        try:
            entries = list(self._fs_ls(self._done_prefix))
        except Exception:
            return 0
        for f in entries:
            name = f.path.rstrip("/").split("/")[-1]
            if not name.endswith(".json"):
                continue
            if name[:-5] not in ingested_keys:
                continue
            try:   # per-file best-effort — one bad delete never aborts the phase
                self._fs_rm(f.path)
                deleted += 1
            except Exception as e:
                logger.warning(f"archive: failed to purge {name} (retried next run): {e}")
        logger.info(f"archive: {deleted} ingested receipt(s) purged from done/")
        return deleted

    def run(self, dry_run: bool = False) -> dict:
        rec = self.reconcile()
        ing = self.ingest(dry_run=dry_run)
        arch = 0
        if not dry_run:
            try:
                arch = self.archive_done(batch_ids=ing.get("batch_ids"))
            except Exception as e:
                logger.error(f"archive phase failed (non-fatal, data is safe): {e}")
        return {**rec, **ing, "receipts_archived": arch, "dry_run": dry_run}

    # ── internals ─────────────────────────────────────────────────────────────

    def _list_receipts(self, prefix: str) -> List[dict]:
        """Read all receipts under a prefix in ONE parallel Spark job.

        One fs_ls enumerates the .json paths (single API call), then
        spark.read.text reads them distributed across the cluster — replacing the
        old per-file sequential fs_head loop (~435 serial S3 GETs). wholetext=True
        keeps each pretty-printed multi-line receipt intact; ignoreMissingFiles is
        passed as a READER OPTION (the session conf of the same name is
        read-restricted on serverless Spark Connect) to tolerate a file that
        vanishes mid-read. A malformed receipt fails only its own json.loads
        (logged + skipped), never the batch. Any Spark-side failure falls back to
        the proven sequential path.
        """
        from pyspark.sql.functions import col

        try:
            paths = [f.path for f in self._fs_ls(prefix)
                     if f.path.rstrip("/").endswith(".json")]
        except Exception:
            return []
        if not paths:
            return []

        out: List[dict] = []
        try:
            # _metadata.file_path — NOT input_file_name() (unsupported in Unity Catalog)
            rows = (self._spark.read
                    .option("ignoreMissingFiles", "true")   # reader option, NOT session conf
                    .text(paths, wholetext=True)
                    .withColumn("_path", col("_metadata.file_path"))
                    .collect())
            for r in rows:
                try:
                    out.append(json.loads(r["value"]))
                except Exception as e:
                    logger.error(f"unreadable receipt {r['_path']}: {e}")
            return out
        except Exception as e:
            logger.warning(
                f"parallel receipt read failed for {prefix} ({type(e).__name__}: {e}) "
                f"— falling back to sequential")
            return self._list_receipts_sequential(paths)

    def _list_receipts_sequential(self, paths: List[str]) -> List[dict]:
        """Fallback: per-file fs_head + json.loads (the original path)."""
        out: List[dict] = []
        for p in paths:
            try:
                out.append(json.loads(self._fs_head(p)))
            except Exception as e:
                logger.error(f"unreadable receipt {p}: {e}")
        return out

    def _batch_already_in_bronze(self, symbol: str, batch_id: str) -> bool:
        """True if any Bronze row already exists for this (symbol, batch_id).

        The skip_dedup idempotency guard: batch_id is stamped onto every Bronze
        row by the validator and is stable across re-runs (it comes from
        fetch_request), so its presence means the prior run's append committed.
        Targeted (symbol + batch_id) — not a full-table scan. Values are our own
        generated identifiers; single-quotes escaped defensively regardless."""
        full_table = f"{self._catalog}.{self._bronze_schema}.{self._stream_cfg.table}"
        sym = symbol.replace("'", "''")
        bid = batch_id.replace("'", "''")
        try:
            row = self._spark.sql(f"""
                SELECT 1 FROM {full_table}
                WHERE symbol = '{sym}' AND batch_id = '{bid}' LIMIT 1
            """).take(1)
            return len(row) > 0
        except Exception as e:
            # Fail SAFE: if we cannot verify, assume NOT written so we do not skip
            # a real load. A duplicate append is tolerable (Layer 3 dedups reads);
            # a silently skipped load is not.
            logger.warning(f"[{symbol}] idempotency check failed ({e}) — proceeding with write")
            return False

    def _buffer_watermark(self, rep, min_d, max_d, rows_written: int) -> None:
        """Append one watermark state to the buffer (flushed as one MERGE at end).
        Widening (LEAST/GREATEST) and the additive count happen in the flush MERGE,
        so no per-instrument read here."""
        self._wm_buffer.append({
            "instrument_id": int(rep.instrument_id), "stream": self._stream_name,
            "interval": rep.bar_interval, "vendor": rep.vendor,
            "batch_id": rep.batch_id, "mode": rep.load_type.lower(),
            "run_min_date": min_d, "run_max_date": max_d,
            "rows_written": int(rows_written),
        })

    def _buffer_watermark_from_records(self, rep, records, rows_written: int) -> None:
        """Buffer a watermark from a record list, deriving min/max bar_date.
        No dates (empty list) → nothing buffered."""
        dates = [r.get("bar_date") for r in records if r.get("bar_date")]
        if not dates:
            return
        self._buffer_watermark(
            rep, date.fromisoformat(min(dates)), date.fromisoformat(max(dates)),
            rows_written)

    def _mark_ingested(self, request_keys) -> None:
        """Bulk-mark LANDED → INGESTED in ONE MERGE (was one UPDATE per group).

        Values flow through a typed temp view (no giant f-string IN-list); the
        MERGE only touches status='LANDED' rows so it is idempotent and never
        regresses a FAILED/already-INGESTED row."""
        if isinstance(request_keys, str):
            request_keys = [request_keys]
        if not request_keys:
            return
        from pyspark.sql.types import StructType, StructField, StringType
        schema = StructType([StructField("request_key", StringType(), False)])
        df = self._spark.createDataFrame([(k,) for k in request_keys], schema=schema)
        df.createOrReplaceTempView("_ingested_keys")
        self._spark.sql(f"""
            MERGE INTO {self._catalog}.control.fetch_request AS t
            USING _ingested_keys AS s
            ON t.request_key = s.request_key
            WHEN MATCHED AND t.status = 'LANDED' THEN UPDATE SET
                t.status = 'INGESTED', t.ingested_at = current_timestamp()
        """)

    def _build_job_log_row(self, r, wr, started, completed) -> tuple:
        """One control.job_run_log row per instrument, buffered for a bulk insert."""
        naive = lambda dt: dt.replace(tzinfo=None)
        return (
            r.batch_id, "raw_to_bronze", naive(started), naive(completed),
            (completed - started).total_seconds(), int(r.instrument_id),
            self._stream_name, r.bar_interval, r.load_type, "success",
            int(wr.records_written), int(wr.records_amended),
            int(wr.records_skipped), int(wr.rejected_written),
            self._pipeline_version, r.vendor,
        )

    def _write_job_run_log_bulk(self, rows: list) -> None:
        """Insert all buffered job_run_log rows in ONE write (was one INSERT per
        instrument). Non-fatal — an audit failure must never fail ingestion."""
        from pyspark.sql.types import (
            StructType, StructField, StringType, TimestampType, DoubleType,
            LongType, IntegerType,
        )
        schema = StructType([
            StructField("batch_id",         StringType(),    False),
            StructField("job_type",         StringType(),    False),
            StructField("run_started_at",   TimestampType(), False),
            StructField("run_completed_at", TimestampType(), False),
            StructField("duration_seconds", DoubleType(),    False),
            StructField("instrument_id",    LongType(),      False),
            StructField("stream",           StringType(),    False),
            StructField("interval",         StringType(),    True),
            StructField("load_type",        StringType(),    True),
            StructField("status",           StringType(),    False),
            StructField("records_new",      IntegerType(),   True),
            StructField("records_amended",  IntegerType(),   True),
            StructField("records_skipped",  IntegerType(),   True),
            StructField("records_rejected", IntegerType(),   True),
            StructField("pipeline_version", StringType(),    True),
            StructField("vendor",           StringType(),    True),
        ])
        try:
            df = self._spark.createDataFrame(rows, schema=schema)
            df.createOrReplaceTempView("_job_run_log_rows")
            self._spark.sql(f"""
                INSERT INTO {self._catalog}.control.job_run_log
                    (batch_id, job_type, run_started_at, run_completed_at,
                     duration_seconds, instrument_id, stream, interval, load_type,
                     status, records_new, records_amended, records_skipped,
                     records_rejected, pipeline_version, vendor)
                SELECT batch_id, job_type, run_started_at, run_completed_at,
                       duration_seconds, instrument_id, stream, interval, load_type,
                       status, records_new, records_amended, records_skipped,
                       records_rejected, pipeline_version, vendor
                FROM _job_run_log_rows
            """)
        except Exception as e:
            logger.error(f"job_run_log bulk write failed (non-fatal): {e}")
