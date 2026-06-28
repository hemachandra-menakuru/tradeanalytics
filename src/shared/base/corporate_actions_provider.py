"""
CorporateActionsProvider ABC — vendor-agnostic interface for fetching corporate actions.

Why this ABC exists:
    IBKR is our current vendor. In future we may add Polygon, Bloomberg, or a
    manual override provider. The rest of the pipeline (detector, classifier)
    must never import a concrete vendor class — they depend on this ABC only.

Key design decisions:
    - get_bulk_corporate_actions() is REQUIRED: at 2000+ instruments, one-call-per-ticker
      would be 33+ minutes. Any implementation that can only do per-ticker calls must
      iterate internally and is not acceptable for production use at scale.
    - Results are plain dicts (not dataclasses) — consistent with how records flow
      through the rest of the pipeline.
    - vendor_instrument_id is the vendor's own ID (IBKR conid, Polygon ticker, etc.)
      — the provider translates from our instrument_id using reference.instrument_vendor_id.

Output dict contract (one per corporate action event):
    instrument_id     int   — our internal surrogate key
    effective_date    date  — first date the adjustment applies
    event_type        str   — SPLIT | REVERSE_SPLIT | DIVIDEND | SPECIAL_DIVIDEND | SPINOFF
    split_ratio_from  int   — e.g. 10 in "10-for-1" (default 1 for non-splits)
    split_ratio_to    int   — e.g. 1  in "10-for-1" (default 1 for non-splits)
    event_factor      float — ratio_to / ratio_from (multiply historical to align to new base)
    dividend_amount   float | None
    dividend_currency str   | None
    event_detail      str   | None — human-readable: "10-for-1 split"
    source_vendor     str   — ibkr | yahoo | polygon | manual
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import date
from typing import Dict, List, Optional


class CorporateActionsProvider(ABC):
    """ABC for fetching corporate action events from any vendor."""

    @property
    @abstractmethod
    def vendor_name(self) -> str:
        """Vendor identifier string: ibkr | yahoo | polygon | bloomberg | manual."""

    @abstractmethod
    def get_corporate_actions(
        self,
        instrument_ids: List[int],
        from_date: date,
        to_date: date,
    ) -> List[Dict]:
        """
        Fetch corporate actions for a specific list of instruments over a date range.

        Use this for targeted lookups (e.g. verifying one instrument's history).
        For bulk daily detection across 2000+ instruments use get_bulk_corporate_actions().

        Args:
            instrument_ids: List of our internal instrument_id values.
            from_date:      Start of the date range (inclusive).
            to_date:        End of the date range (inclusive).

        Returns:
            List of corporate action dicts. See module docstring for field contract.
        """

    @abstractmethod
    def get_bulk_corporate_actions(
        self,
        from_date: date,
        to_date: date,
    ) -> List[Dict]:
        """
        Fetch all corporate actions for ALL tracked instruments over a date range.

        This is the primary method for daily detection at 2000+ instrument scale.
        Implementations must issue one API call (or a small fixed number) for the
        full universe — not one call per instrument.

        Args:
            from_date: Start of the date range (inclusive).
            to_date:   End of the date range (inclusive).

        Returns:
            List of corporate action dicts for all instruments that had events.
        """

    @abstractmethod
    def health_check(self) -> bool:
        """Return True if the vendor API is reachable and authenticated."""
