"""
TradeAnalytics IBKR Contract Enrichment Client
================================================
Calls IBKR Client Portal REST API to resolve con_id for each instrument.
Populates ibkr_con_id in reference.instrument.

Important constraint:
  IBKR gateway runs on localhost:5055 on the local Mac.
  This client CANNOT run inside a Databricks cloud job — the gateway
  is unreachable from AWS. It runs locally via Databricks Connect,
  exactly like the bronze ingestion job.

  In the universe_sync notebook, set widget skip_ibkr=true (default)
  when running in Databricks cloud. Set skip_ibkr=false only when
  running locally via Databricks Connect with gateway active.

API endpoint used:
  POST /iserver/secdef/search
  Body: {"symbol": "AAPL", "name": false, "secType": "STK"}

Reuses the same SSL + HTTP pattern as IBKRProvider to avoid duplication.

Usage:
    client = IbkrContractClient(base_url="https://localhost:5055/v1/api")
    enriched = client.enrich(instruments)
"""

from __future__ import annotations

import json
import logging
import ssl
import time
import urllib.error
import urllib.request
from typing import Dict, List, Optional

from src.reference.sources.universe_source import RawInstrument

logger = logging.getLogger(__name__)

_SECTYPE_MAP = {
    "equity": "STK",
    "etf":    "ETF",
}

_RATE_LIMIT_DELAY = 0.2     # 200ms between requests — stay within IBKR limits
_RETRY_ATTEMPTS   = 2
_RETRY_DELAY      = 3       # seconds


class IbkrContractClient:
    """
    Enriches RawInstrument objects with IBKR con_id.

    Processes one instrument at a time (IBKR contract search is per-symbol).
    Only processes instruments missing ibkr_con_id — safe to re-run.
    Instruments not found in IBKR are logged and left with con_id=None.

    Note: Only run this locally with IBKR gateway active on localhost:5055.
    """

    def __init__(self, base_url: str = "https://localhost:5055/v1/api"):
        self._base_url = base_url.rstrip("/")
        self._ssl_ctx  = ssl.create_default_context()
        self._ssl_ctx.check_hostname = False
        self._ssl_ctx.verify_mode    = ssl.CERT_NONE
        self._cache: Dict[str, Optional[int]] = {}

    def enrich(self, instruments: List[RawInstrument]) -> List[RawInstrument]:
        """
        Enrich instruments with IBKR con_id.
        Only processes instruments where ibkr_con_id is None.

        Args:
            instruments: List of RawInstrument objects.

        Returns:
            Same list with ibkr_con_id populated where found.
        """
        to_enrich = [i for i in instruments if i.ibkr_con_id is None]
        logger.info(
            f"IbkrContractClient: resolving con_id for "
            f"{len(to_enrich)}/{len(instruments)} instruments"
        )

        found     = 0
        not_found = 0

        for inst in to_enrich:
            con_id = self._resolve_conid(inst.symbol, inst.asset_class)
            if con_id:
                inst.ibkr_con_id = con_id
                found += 1
            else:
                not_found += 1
            time.sleep(_RATE_LIMIT_DELAY)

        logger.info(
            f"IbkrContractClient: resolved {found}/{len(to_enrich)} "
            f"({not_found} not found)"
        )
        return instruments

    def _resolve_conid(
        self, symbol: str, asset_class: str
    ) -> Optional[int]:
        """Resolve con_id for one symbol via IBKR secdef/search."""
        if symbol in self._cache:
            return self._cache[symbol]

        sec_type = _SECTYPE_MAP.get(asset_class, "STK")
        payload  = {"symbol": symbol, "name": False, "secType": sec_type}

        for attempt in range(_RETRY_ATTEMPTS):
            try:
                result = self._post("/iserver/secdef/search", payload)
                break
            except Exception as e:
                if attempt < _RETRY_ATTEMPTS - 1:
                    logger.warning(
                        f"[{symbol}] con_id lookup attempt {attempt+1} failed: {e}"
                    )
                    time.sleep(_RETRY_DELAY)
                else:
                    logger.error(f"[{symbol}] con_id lookup failed: {e}")
                    self._cache[symbol] = None
                    return None

        if not result:
            logger.debug(f"[{symbol}] No contracts found in IBKR")
            self._cache[symbol] = None
            return None

        contracts = result if isinstance(result, list) else [result]
        con_id    = None
        for contract in contracts:
            candidate = contract.get("conid")
            if candidate:
                con_id = int(candidate)
                break

        self._cache[symbol] = con_id
        if con_id:
            logger.debug(f"[{symbol}] Resolved con_id={con_id}")
        else:
            logger.debug(f"[{symbol}] con_id not found in response")

        return con_id

    def _post(self, endpoint: str, payload: dict) -> Optional[list]:
        """POST request to IBKR Client Portal REST API."""
        url  = f"{self._base_url}{endpoint}"
        body = json.dumps(payload).encode("utf-8")
        req  = urllib.request.Request(
            url,
            data    = body,
            headers = {"Content-Type": "application/json"},
            method  = "POST",
        )
        with urllib.request.urlopen(req, context=self._ssl_ctx, timeout=10) as resp:
            return json.loads(resp.read().decode("utf-8"))

    def health_check(self) -> bool:
        """Returns True if IBKR gateway is reachable and authenticated."""
        try:
            url = f"{self._base_url}/iserver/auth/status"
            req = urllib.request.Request(url)
            with urllib.request.urlopen(
                req, context=self._ssl_ctx, timeout=5
            ) as resp:
                data = json.loads(resp.read().decode("utf-8"))
                return data.get("authenticated", False)
        except Exception:
            return False
