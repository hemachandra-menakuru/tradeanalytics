#!/usr/bin/env python3
"""
TradeAnalytics Fetch Agent — Connectivity & Execution Plane (EC2)
==================================================================
Two-Plane Architecture (CLAUDE.md §14). This agent is a STATELESS COURIER:

  1. Poll s3://<raw>/control/fetch/<vendor>/pending/ for Contract-v2 manifests
  2. Fetch OHLCV from IB Gateway on localhost per the manifest (zero lookups —
     the planner resolved instrument identity and fetch parameters already)
  3. Land the RAW payload to the manifest's land_to S3 prefix
  4. Move the manifest (enriched with results) to done/ — or failed/ on error

Contract v2 (2026-07-05): manifests are fully self-contained and versioned.
The agent builds the IBKR contract DIRECTLY from vendor_instrument_id (conId)
— no symbol qualification round-trips. Qualification by symbol remains only
as a logged fallback for unmapped instruments. Manifests with an unknown
contract_version are hard-failed to failed/, never guessed at.

Loop behaviour: drains continuously while work exists (no sleep between
paginated batches); sleeps POLL_SECONDS only when idle; hourly heartbeat
log line proves liveness in journald.

Vendor scoping: one script instance serves ONE vendor queue (TA_VENDOR,
pinned by fetch-agent-ibkr.service). A future vendor gets its own script.

Idempotency: fetch → land → move-manifest, in that order. At-least-once +
deterministic filenames + Bronze dedup = replays are harmless.
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

SUPPORTED_CONTRACT_VERSIONS = {"2"}

# ── Configuration (env overrides; defaults match current paper setup) ────────
RAW_BUCKET     = os.environ.get("TA_RAW_BUCKET", "handh-trade-raw-use1")
VENDOR         = os.environ.get("TA_VENDOR", "ibkr")
PENDING_PREFIX = f"control/fetch/{VENDOR}/pending/"
DONE_PREFIX    = f"control/fetch/{VENDOR}/done/"
FAILED_PREFIX  = f"control/fetch/{VENDOR}/failed/"

IB_HOST        = os.environ.get("TA_IB_HOST", "127.0.0.1")
IB_PORT        = int(os.environ.get("TA_IB_PORT", "4004"))   # paper; 4001 live
IB_CLIENT_ID   = int(os.environ.get("TA_IB_CLIENT_ID", "20"))

POLL_SECONDS      = int(os.environ.get("TA_POLL_SECONDS", "60"))
PACING_SECONDS    = float(os.environ.get("TA_PACING_SECONDS", "2.0"))
MAX_ATTEMPTS      = int(os.environ.get("TA_MAX_ATTEMPTS", "3"))
HEARTBEAT_SECONDS = int(os.environ.get("TA_HEARTBEAT_SECONDS", "3600"))

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

    def _build_contract(self, inst: dict):
        """
        Contract v2: build directly from planner-resolved identity — no
        network lookups. Fallback: qualify by symbol (logged) when the
        instrument has no vendor mapping yet.
        """
        from ib_insync import Contract, Stock

        con_id = inst.get("vendor_instrument_id")
        if con_id:
            return Contract(
                conId=int(con_id),
                secType=inst.get("security_type", "STK"),
                exchange=inst.get("exchange", "SMART"),
                currency=inst.get("currency", "USD"),
            )

        log.warning(
            f"{inst.get('symbol')}: no vendor_instrument_id in manifest — "
            f"falling back to symbol qualification (map it in "
            f"reference.instrument_vendor_id to remove this round-trip)"
        )
        contract = Stock(inst["symbol"], inst.get("exchange", "SMART"),
                         inst.get("currency", "USD"))
        self._ib.qualifyContracts(contract)
        return contract

    def fetch_ohlcv(self, manifest: dict) -> list:
        """Fetch bars per Contract-v2 manifest; returns list of raw bar dicts."""
        self._connect()

        inst  = manifest["instrument"]
        fetch = manifest["fetch"]
        start = date.fromisoformat(fetch["start_date"])
        end   = date.fromisoformat(fetch["end_date"])

        contract = self._build_contract(inst)

        # Explicit-UTC endDateTime (yyyymmdd-hh:mm:ss) — the legacy
        # space-separated form triggers IBKR deprecation warning 2174.
        bars = self._ib.reqHistoricalData(
            contract,
            endDateTime=end.strftime("%Y%m%d-23:59:59"),
            durationStr=_duration_str(start, end),
            barSizeSetting=_BAR_SIZE[fetch["bar_interval"]],
            whatToShow=fetch.get("what_to_show", "TRADES"),
            useRTH=bool(fetch.get("use_rth", True)),
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
    resp = s3.list_objects_v2(Bucket=RAW_BUCKET, Prefix=PENDING_PREFIX, MaxKeys=limit)
    objs = resp.get("Contents", [])
    objs.sort(key=lambda o: o["Key"])
    return [o["Key"] for o in objs if o["Key"].endswith(".json")]


def read_manifest(key: str) -> dict:
    body = s3.get_object(Bucket=RAW_BUCKET, Key=key)["Body"].read()
    return json.loads(body)


def land_raw(manifest: dict, bars: list) -> str:
    inst = manifest["instrument"]
    payload = {
        "payload_format": manifest["landing"].get("payload_format", "ohlcv_json_v1"),
        "request_key":    manifest["request_key"],
        "batch_id":       manifest["batch_id"],
        "stream":         manifest["stream"],
        "load_type":      manifest["load_type"],
        "instrument":     inst,                     # full planner-resolved identity
        "fetch":          manifest["fetch"],
        "lineage":        manifest.get("lineage", {}),
        "fetched_at":     datetime.now(timezone.utc).isoformat(),
        "fetched_by":     f"fetch_agent_{VENDOR}_v2",
        "record_count":   len(bars),
        "bars":           bars,
    }
    land_to = manifest["landing"]["land_to"]
    assert land_to.startswith(f"s3://{RAW_BUCKET}/")
    key = land_to.replace(f"s3://{RAW_BUCKET}/", "") + f"{manifest['request_key']}.json"
    s3.put_object(Bucket=RAW_BUCKET, Key=key, Body=json.dumps(payload).encode())
    return f"s3://{RAW_BUCKET}/{key}"


def move_manifest(pending_key: str, manifest: dict, outcome_prefix: str) -> None:
    name = pending_key.split("/")[-1]
    s3.put_object(Bucket=RAW_BUCKET, Key=outcome_prefix + name,
                  Body=json.dumps(manifest, indent=2).encode())
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
                "status":        "LANDED",
                "record_count":  len(bars),
                "s3_data_path":  data_path,
                "landed_at":     datetime.now(timezone.utc).isoformat(),
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


def handle_qualify_instruments(gw: GatewayClient, pending_key: str, manifest: dict) -> None:
    """
    Batch symbol→conId qualification for the vendor-ID seeding flow
    (notebooks/reference/06_seed_vendor_ids.py). The notebook submits one
    manifest with N instruments; we qualify each via the gateway and write
    the results manifest to done/. Databricks loads them into Delta — the
    agent never touches the reference tables.
    """
    from ib_insync import Stock

    gw._connect()
    mappings, failures = [], []
    for inst in manifest.get("instruments", []):
        symbol   = inst["symbol"]
        ibkr_sym = symbol.replace(".", " ")   # IBKR dot-class symbology (BRK.B -> BRK B)
        try:
            contract = Stock(ibkr_sym, "SMART", inst.get("currency") or "USD")
            gw._ib.qualifyContracts(contract)
            if not contract.conId:
                raise ValueError("qualification returned no conId")
            mappings.append({
                "instrument_id":        inst["instrument_id"],
                "symbol":               symbol,
                "vendor":               VENDOR,
                "vendor_instrument_id": str(contract.conId),
                "vendor_exchange":      contract.primaryExchange or None,
                "vendor_symbol":        contract.localSymbol or ibkr_sym,
                "currency":             contract.currency,
            })
            log.info(f"qualified {symbol} → conId {contract.conId} @ {contract.primaryExchange}")
        except Exception as e:
            failures.append({"symbol": symbol, "error": f"{type(e).__name__}: {e}"})
            log.warning(f"qualify {symbol} FAILED: {e}")
        time.sleep(PACING_SECONDS / 2)   # qualification is lighter than history requests

    manifest.update({
        "status":        "LANDED",
        "mappings":      mappings,
        "failures":      failures,
        "mapping_count": len(mappings),
        "landed_at":     datetime.now(timezone.utc).isoformat(),
        "fetched_by":    f"fetch_agent_{VENDOR}_v2",
    })
    move_manifest(pending_key, manifest, DONE_PREFIX)
    log.info(f"{manifest['request_key']}: QUALIFY complete — "
             f"{len(mappings)} mapped, {len(failures)} failed")


HANDLERS = {
    "FETCH_OHLCV":         handle_fetch_ohlcv,
    "QUALIFY_INSTRUMENTS": handle_qualify_instruments,
    # Future fetch-domain task types slot in here (same dispatch pattern):
    # "FETCH_OPTIONS_CHAIN": handle_fetch_options_chain,
}


def _fail_manifest(key: str, manifest: dict, reason: str) -> None:
    log.error(f"{key}: {reason} — moving to failed/")
    manifest.update({"status": "FAILED", "error_message": reason,
                     "failed_at": datetime.now(timezone.utc).isoformat()})
    move_manifest(key, manifest, FAILED_PREFIX)


# ── Main loop ─────────────────────────────────────────────────────────────────

def _acquire_singleton_lock():
    """
    One agent per vendor queue — enforced in-process, not just operationally.
    An OS-level exclusive lock on a well-known file: a second launch exits
    immediately instead of silently racing the first (4 orphaned agents raced
    the queue on 2026-07-04 — this makes that impossible).
    The lock dies with the process, so crashes never leave a stale lock.
    """
    import fcntl
    lock_path = f"/tmp/fetch_agent_{VENDOR}.lock"
    fh = open(lock_path, "w")
    try:
        fcntl.flock(fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        log.error(
            f"Another fetch_agent for vendor '{VENDOR}' is already running "
            f"(lock held on {lock_path}). Exiting — one consumer per queue. "
            f"Find it with: pgrep -af fetch_agent.py"
        )
        sys.exit(1)
    fh.write(str(os.getpid()))
    fh.flush()
    return fh  # keep the handle alive for the process lifetime


def run() -> None:
    _lock = _acquire_singleton_lock()
    signal.signal(signal.SIGTERM, _handle_sigterm)
    signal.signal(signal.SIGINT, _handle_sigterm)
    gw = GatewayClient()
    total_processed = 0
    last_heartbeat  = time.monotonic()

    log.info(
        f"fetch_agent v2 started — vendor={VENDOR}, bucket={RAW_BUCKET}, "
        f"gateway={IB_HOST}:{IB_PORT}, poll={POLL_SECONDS}s, pacing={PACING_SECONDS}s"
    )

    while not _shutdown:
        processed_this_cycle = 0
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

                    version = manifest.get("contract_version")
                    if version not in SUPPORTED_CONTRACT_VERSIONS:
                        _fail_manifest(key, manifest,
                                       f"unsupported contract_version '{version}' "
                                       f"(agent supports {sorted(SUPPORTED_CONTRACT_VERSIONS)})")
                        continue

                    handler = HANDLERS.get(manifest.get("task_type", ""))
                    if handler is None:
                        _fail_manifest(key, manifest,
                                       f"unknown task_type '{manifest.get('task_type')}'")
                        continue

                    handler(gw, key, manifest)
                    processed_this_cycle += 1
                    total_processed += 1
                    time.sleep(PACING_SECONDS)
        except Exception as e:
            log.error(f"poll cycle error: {type(e).__name__}: {e}")

        # Continuous drain: if this cycle did work, re-poll immediately —
        # more work is probably waiting (pagination). Sleep only when idle.
        if processed_this_cycle == 0 and not _shutdown:
            if time.monotonic() - last_heartbeat >= HEARTBEAT_SECONDS:
                log.info(
                    f"heartbeat — idle, queue empty, gateway "
                    f"{'up' if gw.is_up() else 'DOWN'}, "
                    f"{total_processed} request(s) processed since start"
                )
                last_heartbeat = time.monotonic()
            for _ in range(POLL_SECONDS):
                if _shutdown:
                    break
                time.sleep(1)

    gw.disconnect()
    log.info(f"fetch_agent stopped cleanly — {total_processed} request(s) processed this run")


if __name__ == "__main__":
    sys.exit(run())
