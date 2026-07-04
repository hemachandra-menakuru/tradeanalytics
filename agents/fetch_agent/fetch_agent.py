#!/usr/bin/env python3
"""
TradeAnalytics Fetch Agent — Connectivity & Execution Plane (EC2)
==================================================================
Two-Plane Architecture (CLAUDE.md §14). This agent is a STATELESS COURIER:

  1. Poll s3://<raw>/control/fetch/pending/ for manifests (the planner's inbox)
  2. Fetch OHLCV from IB Gateway on localhost (same box) per the manifest
  3. Land the RAW payload to the manifest's land_to S3 prefix
  4. Move the manifest (enriched with results) to done/ — or failed/ on error

It holds NO business logic: no validation, no schemas, no watermarks, no
decisions. Everything it needs arrives inside each manifest. Delta bookkeeping
is reconciled by the Databricks ingestion job from the done/failed manifests.

Vendor scoping: this file implements the generic queue pattern with an
IBKR-specific GatewayClient. It serves ONE vendor queue (TA_VENDOR, pinned to
'ibkr' by fetch-agent-ibkr.service). A future Polygon agent gets its own
script/client — do not multiplex vendors inside this file.

Deployment: single file on the EC2 box, venv with ib_insync + boto3 only.
S3 access via IAM instance profile — no credentials on disk.
Deployed from repo path agents/fetch_agent/ via scp (no repo clone on the box).

Idempotency: fetch → land → move-manifest, in that order. A crash mid-cycle
re-processes the manifest on restart; deterministic filenames make the re-land
an overwrite, and Bronze's dedup makes replays harmless (at-least-once + idempotent).
"""

from __future__ import annotations

import json
import logging
import os
import signal
import socket
import sys
import time
from datetime import date, datetime, timezone

import boto3

# ── Configuration (env overrides; sane defaults for the current box) ─────────
RAW_BUCKET     = os.environ.get("TA_RAW_BUCKET", "handh-trade-raw-use1")
VENDOR         = os.environ.get("TA_VENDOR", "ibkr")   # this agent serves ONE vendor's queue
PENDING_PREFIX = f"control/fetch/{VENDOR}/pending/"
DONE_PREFIX    = f"control/fetch/{VENDOR}/done/"
FAILED_PREFIX  = f"control/fetch/{VENDOR}/failed/"

IB_HOST        = os.environ.get("TA_IB_HOST", "127.0.0.1")
IB_PORT        = int(os.environ.get("TA_IB_PORT", "4004"))   # paper; 4001 live
IB_CLIENT_ID   = int(os.environ.get("TA_IB_CLIENT_ID", "20"))

POLL_SECONDS   = int(os.environ.get("TA_POLL_SECONDS", "60"))
PACING_SECONDS = float(os.environ.get("TA_PACING_SECONDS", "2.0"))  # IBKR historical pacing
MAX_ATTEMPTS   = int(os.environ.get("TA_MAX_ATTEMPTS", "3"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] fetch_agent: %(message)s",
)
log = logging.getLogger("fetch_agent")

_shutdown = False


def _handle_sigterm(signum, frame):
    global _shutdown
    _shutdown = True
    log.info("SIGTERM received — finishing current request then exiting")


# ── IB Gateway ────────────────────────────────────────────────────────────────

_BAR_SIZE = {"1d": "1 day", "1h": "1 hour", "4h": "4 hours", "5m": "5 mins", "1m": "1 min"}


def _duration_str(start: date, end: date) -> str:
    days = (end - start).days + 1
    if days <= 365:
        return f"{max(1, days)} D"
    return f"{(days // 7) + 1} W"


class GatewayClient:
    """Thin ib_insync wrapper. Connects lazily, reconnects on demand."""

    def __init__(self):
        self._ib = None

    def _connect(self):
        from ib_insync import IB
        if self._ib is None:
            self._ib = IB()
        if not self._ib.isConnected():
            self._ib.connect(IB_HOST, IB_PORT, clientId=IB_CLIENT_ID,
                             timeout=30, readonly=True)
            log.info(f"Connected to IB Gateway {IB_HOST}:{IB_PORT}")

    def is_up(self) -> bool:
        try:
            with socket.create_connection((IB_HOST, IB_PORT), timeout=5):
                return True
        except OSError:
            return False

    def fetch_ohlcv(self, manifest: dict) -> list:
        """Fetch bars per manifest; returns list of raw bar dicts."""
        from ib_insync import Stock

        self._connect()
        start = date.fromisoformat(manifest["start_date"])
        end   = date.fromisoformat(manifest["end_date"])

        contract = Stock(manifest["symbol"], "SMART", "USD")
        self._ib.qualifyContracts(contract)

        # IBKR-required explicit-timezone format (yyyymmdd-hh:mm:ss = UTC).
        # The legacy space-separated form triggers deprecation warning 2174 and
        # will be REJECTED in a future gateway API release.
        bars = self._ib.reqHistoricalData(
            contract,
            endDateTime=end.strftime("%Y%m%d-23:59:59"),
            durationStr=_duration_str(start, end),
            barSizeSetting=_BAR_SIZE[manifest["bar_interval"]],
            whatToShow="TRADES",
            useRTH=True,
            formatDate=1,
            keepUpToDate=False,
        )

        out = []
        for b in bars:
            b_date = b.date if isinstance(b.date, date) else datetime.strptime(str(b.date).strip()[:8], "%Y%m%d").date()
            if b_date < start or b_date > end:
                continue  # IB duration rounding can return extra bars
            out.append({
                "bar_date": b_date.isoformat(),
                "open":     float(b.open),
                "high":     float(b.high),
                "low":      float(b.low),
                "close":    float(b.close),
                "volume":   int(b.volume),
            })
        return out

    def disconnect(self):
        if self._ib is not None and self._ib.isConnected():
            self._ib.disconnect()


# ── S3 queue operations ───────────────────────────────────────────────────────

s3 = boto3.client("s3")


def list_pending(limit: int = 200) -> list:
    """Oldest-first manifest keys in the inbox."""
    resp = s3.list_objects_v2(Bucket=RAW_BUCKET, Prefix=PENDING_PREFIX, MaxKeys=limit)
    objs = resp.get("Contents", [])
    objs.sort(key=lambda o: o["Key"])  # request_keys embed batch timestamp + seq
    return [o["Key"] for o in objs if o["Key"].endswith(".json")]


def read_manifest(key: str) -> dict:
    body = s3.get_object(Bucket=RAW_BUCKET, Key=key)["Body"].read()
    return json.loads(body)


def land_raw(manifest: dict, bars: list) -> str:
    """Write the raw payload to the manifest's land_to prefix. Returns s3 path."""
    payload = {
        "request_key":   manifest["request_key"],
        "instrument_id": manifest["instrument_id"],
        "symbol":        manifest["symbol"],
        "vendor":        manifest["vendor"],
        "stream":        manifest["stream"],
        "bar_interval":  manifest["bar_interval"],
        "load_type":     manifest["load_type"],
        "batch_id":      manifest["batch_id"],
        "fetched_at":    datetime.now(timezone.utc).isoformat(),
        "fetched_by":    f"fetch_agent_{VENDOR}_v1",
        "record_count":  len(bars),
        "bars":          bars,
    }
    land_to = manifest["land_to"]  # s3://bucket/prefix/
    assert land_to.startswith(f"s3://{RAW_BUCKET}/")
    key = land_to.replace(f"s3://{RAW_BUCKET}/", "") + f"{manifest['request_key']}.json"
    s3.put_object(Bucket=RAW_BUCKET, Key=key, Body=json.dumps(payload).encode())
    return f"s3://{RAW_BUCKET}/{key}"


def move_manifest(pending_key: str, manifest: dict, outcome_prefix: str) -> None:
    """Write enriched manifest to done/ or failed/, then remove from pending/."""
    name = pending_key.split("/")[-1]
    s3.put_object(
        Bucket=RAW_BUCKET,
        Key=outcome_prefix + name,
        Body=json.dumps(manifest, indent=2).encode(),
    )
    s3.delete_object(Bucket=RAW_BUCKET, Key=pending_key)


# ── Task dispatch ─────────────────────────────────────────────────────────────

def handle_fetch_ohlcv(gw: GatewayClient, pending_key: str, manifest: dict) -> None:
    attempts = 0
    last_err = None
    while attempts < MAX_ATTEMPTS:
        attempts += 1
        try:
            bars = gw.fetch_ohlcv(manifest)
            data_path = land_raw(manifest, bars)
            manifest.update({
                "status":       "LANDED",
                "record_count": len(bars),
                "s3_data_path": data_path,
                "landed_at":    datetime.now(timezone.utc).isoformat(),
                "attempt_count": attempts,
            })
            move_manifest(pending_key, manifest, DONE_PREFIX)
            log.info(f"{manifest['request_key']}: LANDED {len(bars)} bars → {data_path}")
            return
        except Exception as e:
            last_err = f"{type(e).__name__}: {e}"
            log.warning(f"{manifest['request_key']}: attempt {attempts}/{MAX_ATTEMPTS} failed — {last_err}")
            time.sleep(PACING_SECONDS * attempts)

    manifest.update({
        "status":        "FAILED",
        "error_message": last_err,
        "attempt_count": attempts,
        "failed_at":     datetime.now(timezone.utc).isoformat(),
    })
    move_manifest(pending_key, manifest, FAILED_PREFIX)
    log.error(f"{manifest['request_key']}: FAILED after {attempts} attempts — {last_err}")


HANDLERS = {
    "FETCH_OHLCV": handle_fetch_ohlcv,
    # Future task types (same dispatch pattern, per two-plane decision):
    # "FETCH_OPTIONS_CHAIN": handle_fetch_options_chain,
}


# ── Main loop ─────────────────────────────────────────────────────────────────

def run() -> None:
    signal.signal(signal.SIGTERM, _handle_sigterm)
    signal.signal(signal.SIGINT, _handle_sigterm)
    gw = GatewayClient()
    log.info(
        f"fetch_agent started — bucket={RAW_BUCKET}, gateway={IB_HOST}:{IB_PORT}, "
        f"poll={POLL_SECONDS}s, pacing={PACING_SECONDS}s"
    )

    while not _shutdown:
        try:
            keys = list_pending()
            if keys and not gw.is_up():
                log.warning(f"{len(keys)} pending but gateway {IB_HOST}:{IB_PORT} is down — waiting")
            elif keys:
                log.info(f"{len(keys)} pending manifest(s)")
                for key in keys:
                    if _shutdown:
                        break
                    manifest = read_manifest(key)
                    handler = HANDLERS.get(manifest.get("task_type", ""))
                    if handler is None:
                        log.error(f"{key}: unknown task_type '{manifest.get('task_type')}' — moving to failed/")
                        manifest.update({"status": "FAILED",
                                         "error_message": f"unknown task_type {manifest.get('task_type')}"})
                        move_manifest(key, manifest, FAILED_PREFIX)
                        continue
                    handler(gw, key, manifest)
                    time.sleep(PACING_SECONDS)
        except Exception as e:
            log.error(f"poll cycle error: {type(e).__name__}: {e}")

        # Sleep in 1s slices so SIGTERM is honoured promptly
        for _ in range(POLL_SECONDS):
            if _shutdown:
                break
            time.sleep(1)

    gw.disconnect()
    log.info("fetch_agent stopped cleanly")


if __name__ == "__main__":
    sys.exit(run())
