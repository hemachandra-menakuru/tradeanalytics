"""
IBKRCorporateActionsProvider — CorporateActionsProvider backed by IBKR Client Portal REST API.

IBKR does not have a single "bulk corporate actions" endpoint that returns events
for all instruments in one call. The closest available is the corporate actions
endpoint per conid. For production at 2000+ instruments we use a two-step approach:

    Step 1 — Detect retroactive price changes (bulk Spark query, no IBKR calls).
              This finds instruments where stored prices no longer match IBKR prices.
              CorporateActionDetector owns this step. Only affected instruments trigger
              Step 2.

    Step 2 — For the detected instruments only, call IBKR to get the actual event
              details (type, ratio, effective date). Because splits are rare events
              (typically 0–5 per week across a 2000-instrument universe), this is
              a small number of targeted calls, not 2000 calls.

This means get_bulk_corporate_actions() cannot be implemented as a single IBKR call.
Instead it fetches from a pre-materialised list of candidate instrument_ids that the
CorporateActionDetector has flagged.

For true bulk needs (e.g. initial historical load of all events for a new instrument),
use ManualCorporateActionsProvider or YahooCorporateActionsProvider to seed the table,
then use IBKR to verify.

IBKR corporate actions endpoint:
    GET /v1/api/iserver/account/trades  (not applicable)
    GET /v1/api/iserver/contract/{conid}/history  (via adjustments flag)
    -- Exact endpoint TBC when gateway is available for testing.

Gateway runs on localhost:5055 — only reachable from local Mac, never from Databricks cluster.
"""

from __future__ import annotations

import logging
import requests
from datetime import date
from typing import Dict, List, Optional

from src.shared.base.corporate_actions_provider import CorporateActionsProvider

logger = logging.getLogger(__name__)

_SPLIT_RATIO_THRESHOLD = 0.40  # price_ratio below this likely indicates a forward split
_REVERSE_THRESHOLD     = 1.60  # price_ratio above this likely indicates a reverse split


class IBKRCorporateActionsProvider(CorporateActionsProvider):
    """
    CorporateActionsProvider backed by IBKR Client Portal REST API.

    Reads vendor IDs from reference.instrument_vendor_id at construction time
    so we can translate instrument_id → conid for IBKR API calls.

    Args:
        base_url:        IBKR gateway base URL (default: https://localhost:5055/v1/api)
        conid_map:       {instrument_id: conid_str} — pre-loaded from instrument_vendor_id.
                         Pass this in so the provider does not need a Spark session.
        verify_ssl:      SSL verification (False for self-signed IBKR cert).
        request_timeout: Per-request timeout in seconds.
    """

    def __init__(
        self,
        base_url: str = "https://localhost:5055/v1/api",
        conid_map: Optional[Dict[int, str]] = None,
        verify_ssl: bool = False,
        request_timeout: int = 10,
    ):
        self._base_url = base_url.rstrip("/")
        self._conid_map = conid_map or {}
        self._verify = verify_ssl
        self._timeout = request_timeout

    @property
    def vendor_name(self) -> str:
        return "ibkr"

    def health_check(self) -> bool:
        """Return True if the IBKR gateway is reachable and authenticated."""
        try:
            resp = requests.get(
                f"{self._base_url}/iserver/auth/status",
                verify=self._verify,
                timeout=self._timeout,
            )
            data = resp.json()
            authenticated = data.get("authenticated", False)
            if not authenticated:
                logger.warning("IBKRCorporateActionsProvider: gateway not authenticated")
            return authenticated
        except Exception as e:
            logger.error(f"IBKRCorporateActionsProvider: health check failed — {e}")
            return False

    def get_corporate_actions(
        self,
        instrument_ids: List[int],
        from_date: date,
        to_date: date,
    ) -> List[Dict]:
        """
        Fetch corporate action events for specific instruments from IBKR.

        Makes one API call per instrument (acceptable for targeted lookups on a
        small flagged set — do not call this for the full 2000-instrument universe).
        """
        results = []
        for instrument_id in instrument_ids:
            conid = self._conid_map.get(instrument_id)
            if not conid:
                logger.warning(
                    f"IBKRCorporateActionsProvider: no IBKR conid for instrument_id={instrument_id}"
                )
                continue
            events = self._fetch_events_for_conid(instrument_id, conid, from_date, to_date)
            results.extend(events)
        return results

    def get_bulk_corporate_actions(
        self,
        from_date: date,
        to_date: date,
    ) -> List[Dict]:
        """
        Not available as a single IBKR call. See module docstring.

        This implementation fetches for ALL known conids and is only suitable
        when the conid_map contains a small, pre-filtered set of candidates
        (i.e. instruments flagged by CorporateActionDetector).
        """
        if len(self._conid_map) > 50:
            logger.warning(
                f"IBKRCorporateActionsProvider.get_bulk_corporate_actions: "
                f"called with {len(self._conid_map)} instruments. "
                f"Pre-filter to flagged candidates first — do not call for the full universe."
            )
        return self.get_corporate_actions(
            list(self._conid_map.keys()), from_date, to_date
        )

    def _fetch_events_for_conid(
        self,
        instrument_id: int,
        conid: str,
        from_date: date,
        to_date: date,
    ) -> List[Dict]:
        """
        Call IBKR for corporate actions for a single conid.

        Endpoint TBC — this is a stub that should be updated once the exact
        IBKR corporate actions endpoint is confirmed via gateway testing.
        The endpoint likely requires the conid and a date range.
        """
        try:
            # TODO: replace with confirmed IBKR corporate actions endpoint
            # Candidate: GET /v1/api/iserver/contract/{conid}/info-and-rules
            # or:        GET /v1/api/trsrv/secdef/schedule?conids={conid}
            # or:        history endpoint with adjustments=true
            # For now, return empty — detector handles the primary detection path.
            logger.debug(
                f"IBKRCorporateActionsProvider: stub — conid={conid}, "
                f"instrument_id={instrument_id}, range={from_date}→{to_date}"
            )
            return []
        except Exception as e:
            logger.error(
                f"IBKRCorporateActionsProvider: API error for conid={conid} "
                f"instrument_id={instrument_id} — {e}"
            )
            return []

    @classmethod
    def from_spark(
        cls,
        spark,
        catalog: str = "tradeanalytics",
        base_url: str = "https://localhost:5055/v1/api",
        verify_ssl: bool = False,
    ) -> "IBKRCorporateActionsProvider":
        """
        Build provider by loading IBKR conid map from reference.instrument_vendor_id.

        Args:
            spark:   Active SparkSession.
            catalog: Unity Catalog name.
            base_url: IBKR gateway URL.
        """
        rows = spark.sql(f"""
            SELECT instrument_id, vendor_instrument_id
            FROM {catalog}.reference.instrument_vendor_id
            WHERE vendor = 'ibkr' AND is_current = true
        """).collect()

        conid_map = {r["instrument_id"]: r["vendor_instrument_id"] for r in rows}
        logger.info(f"IBKRCorporateActionsProvider: loaded {len(conid_map)} conids from Delta")
        return cls(base_url=base_url, conid_map=conid_map, verify_ssl=verify_ssl)
