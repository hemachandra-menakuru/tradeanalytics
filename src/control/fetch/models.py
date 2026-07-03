"""
FetchRequest — one unit of work for the EC2 bridge agent.

Two-Plane Architecture (CLAUDE.md §14): the Databricks planner writes these as
rows in control.fetch_request (Delta = audit truth) AND as JSON manifests in
s3://<raw>/control/fetch/pending/ (S3 = agent signalling). The manifest is
self-contained: the agent needs zero joins and zero project code to act on it.
"""

from __future__ import annotations

from dataclasses import dataclass, asdict
from datetime import date


@dataclass(frozen=True)
class FetchRequest:
    """One chunked fetch instruction. Immutable — planner output."""

    request_key:   str    # deterministic: <batch_id>_<symbol>_<chunk_seq>
    batch_id:      str
    instrument_id: int
    symbol:        str
    vendor:        str    # ibkr | polygon | ...
    stream:        str    # daily | intraday | tick
    bar_interval:  str    # 1d | 1h | ...
    start_date:    date
    end_date:      date
    load_type:     str    # INITIAL_LOAD | INCREMENTAL | GAP_FILL | ...
    task_type:     str    # FETCH_OHLCV (variants within the fetch domain only)
    land_to:       str    # s3 prefix where the agent writes raw payloads

    def manifest_dict(self) -> dict:
        """JSON-safe dict for the S3 manifest (dates as ISO strings)."""
        d = asdict(self)
        d["start_date"] = self.start_date.isoformat()
        d["end_date"]   = self.end_date.isoformat()
        return d
