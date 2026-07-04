"""
FetchRequest — one unit of work for the EC2 bridge agent (Contract v2).

Two-Plane Architecture (CLAUDE.md §14): the Databricks planner writes these as
rows in control.fetch_request (Delta = audit truth) AND as JSON manifests in
s3://<raw>/control/fetch/<vendor>/pending/ (S3 = agent signalling).

Contract v2 design (2026-07-05): the manifest is fully self-contained — the
planner resolves EVERYTHING at planning time (one JOIN across reference
tables), so the agent performs zero lookups:
  - instrument block: our key (instrument_id) + vendor's key
    (vendor_instrument_id, e.g. IBKR conId) + contract-construction inputs
    (security_type / exchange / currency) — no symbol qualification needed
  - fetch block: behavioural parameters (what_to_show, use_rth) travel as
    DATA, not agent code branches — new asset classes need no agent deploy
  - landing block: payload_format names the raw schema for the ingestion job
  - lineage block: end-to-end audit (which planner build created this work)
  - contract_version: agent hard-fails manifests it doesn't understand
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Optional

CONTRACT_VERSION = "2"


@dataclass(frozen=True)
class FetchRequest:
    """One chunked fetch instruction. Immutable — planner output."""

    # Identity / lifecycle
    request_key:   str    # <batch_id>_<vendor>_<symbol>_<chunk_seq>
    batch_id:      str
    stream:        str    # daily | intraday | tick
    load_type:     str    # INITIAL_LOAD | INCREMENTAL | GAP_FILL | ...
    task_type:     str    # FETCH_OHLCV (fetch-domain variants only)

    # Instrument (planner-resolved from reference tables)
    instrument_id:        int
    symbol:               str            # display/logging only — never a key
    vendor:               str            # ibkr | polygon | ...
    vendor_instrument_id: Optional[str]  # IBKR conId etc.; None = agent falls back to qualification
    asset_class:          str            # equity | etf | fx | future | ...
    security_type:        str            # IBKR secType: STK | CASH | FUT | ...
    exchange:             str            # routing exchange (SMART for US equities)
    exchange_mic:         Optional[str]  # ISO 10383 listing MIC (provenance)
    currency:             str
    vendor_exchange:      Optional[str]  # vendor's own exchange code, if any

    # Fetch parameters (behaviour as data)
    bar_interval: str    # 1d | 1h | ...
    start_date:   date
    end_date:     date
    what_to_show: str    # TRADES | MIDPOINT | ADJUSTED_LAST
    use_rth:      bool

    # Landing
    land_to:        str  # s3 prefix where the agent writes raw payloads
    payload_format: str  # names the raw payload schema (ohlcv_json_v1)

    # Lineage
    requested_by:     str
    pipeline_version: str

    def manifest_dict(self) -> dict:
        """Contract v2 manifest — grouped, JSON-safe."""
        return {
            "contract_version": CONTRACT_VERSION,
            "task_type":        self.task_type,
            "request_key":      self.request_key,
            "batch_id":         self.batch_id,
            "stream":           self.stream,
            "load_type":        self.load_type,
            "instrument": {
                "instrument_id":        self.instrument_id,
                "symbol":               self.symbol,
                "asset_class":          self.asset_class,
                "security_type":        self.security_type,
                "exchange":             self.exchange,
                "exchange_mic":         self.exchange_mic,
                "currency":             self.currency,
                "vendor":               self.vendor,
                "vendor_instrument_id": self.vendor_instrument_id,
                "vendor_exchange":      self.vendor_exchange,
            },
            "fetch": {
                "bar_interval": self.bar_interval,
                "start_date":   self.start_date.isoformat(),
                "end_date":     self.end_date.isoformat(),
                "what_to_show": self.what_to_show,
                "use_rth":      self.use_rth,
            },
            "landing": {
                "land_to":        self.land_to,
                "payload_format": self.payload_format,
            },
            "lineage": {
                "requested_at":     datetime.now(timezone.utc).isoformat(),
                "requested_by":     self.requested_by,
                "pipeline_version": self.pipeline_version,
            },
        }
