#!/usr/bin/env python3
"""
Vendor-ID discovery — Step 1 of 2 of the repeatable seeding flow.

Two-plane split (CLAUDE.md §14):
  Step 1 (THIS SCRIPT, runs on the Mac — gateway reachable):
     qualify unmapped current listings via the EC2 IB Gateway and STAGE the
     discovered mappings as JSON to
        s3://handh-trade-raw-use1/reference/vendor_id_seed/ibkr/<UTC-timestamp>.json
  Step 2 (notebooks/reference/06_load_vendor_id_seed.py, Databricks):
     read the staged file, validate against existing mappings, INSERT new
     rows into reference.instrument_vendor_id. Databricks remains the only
     Delta writer.

Known IBKR symbology quirks handled: dot-class shares (BRK.B) are qualified
using IBKR's space form (BRK B) automatically.

Usage:  python scripts/seed_vendor_ids.py
"""

from __future__ import annotations

import json
import os
import sys
import time
from datetime import datetime, timezone

GATEWAY_HOST = "54.197.158.82"
GATEWAY_PORT = 4004      # paper
CLIENT_ID    = 99        # diagnostics clientId (CLAUDE.md registry)
VENDOR       = "ibkr"
CATALOG      = "tradeanalytics"
RAW_BUCKET   = "handh-trade-raw-use1"
STAGE_PREFIX = f"reference/vendor_id_seed/{VENDOR}"


def to_ibkr_symbol(symbol: str) -> str:
    """IBKR symbology: class shares use a space, not a dot (BRK.B -> BRK B)."""
    return symbol.replace(".", " ")


def main() -> int:
    from databricks.connect import DatabricksSession
    profile = os.environ.get("DATABRICKS_CONFIG_PROFILE", "handh-trade-aws")
    spark = DatabricksSession.builder.profile(profile).serverless(True).getOrCreate()

    rows = spark.sql(f"""
        SELECT l.instrument_id, l.symbol, l.currency
        FROM {CATALOG}.reference.instrument_listing l
        LEFT JOIN {CATALOG}.reference.instrument_vendor_id v
               ON v.instrument_id = l.instrument_id
              AND v.vendor = '{VENDOR}' AND v.is_current = true
        WHERE l.is_current = true AND v.vendor_instrument_id IS NULL
        ORDER BY l.symbol
    """).collect()

    if not rows:
        print("Nothing to discover — every current listing already has a mapping.")
        return 0
    print(f"{len(rows)} unmapped listing(s) to qualify\n")

    from ib_insync import IB, Stock
    ib = IB()
    ib.connect(GATEWAY_HOST, GATEWAY_PORT, clientId=CLIENT_ID, timeout=30, readonly=True)

    mappings, failures = [], []
    for r in rows:
        ibkr_sym = to_ibkr_symbol(r.symbol)
        try:
            contract = Stock(ibkr_sym, "SMART", r.currency or "USD")
            ib.qualifyContracts(contract)
            if not contract.conId:
                raise ValueError("qualification returned no conId")
            print(f"  {r.symbol:<8} → conId {contract.conId:<12} "
                  f"{contract.localSymbol} @ {contract.primaryExchange}")
            mappings.append({
                "instrument_id":        r.instrument_id,
                "symbol":               r.symbol,
                "vendor":               VENDOR,
                "vendor_instrument_id": str(contract.conId),
                "vendor_exchange":      contract.primaryExchange or None,
                "vendor_symbol":        contract.localSymbol or ibkr_sym,
                "currency":             contract.currency,
            })
        except Exception as e:
            failures.append({"symbol": r.symbol, "error": f"{type(e).__name__}: {e}"})
            print(f"  {r.symbol:<8} → FAILED: {e}")
        time.sleep(1)
    ib.disconnect()

    # Stage to S3 for the Databricks loader notebook
    import boto3
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    key = f"{STAGE_PREFIX}/{stamp}.json"
    doc = {
        "staged_at":     datetime.now(timezone.utc).isoformat(),
        "staged_by":     "scripts/seed_vendor_ids.py",
        "vendor":        VENDOR,
        "gateway":       f"{GATEWAY_HOST}:{GATEWAY_PORT}",
        "mapping_count": len(mappings),
        "failures":      failures,
        "mappings":      mappings,
    }
    boto3.client("s3").put_object(
        Bucket=RAW_BUCKET, Key=key, Body=json.dumps(doc, indent=2).encode()
    )

    print(f"\n✅ Staged {len(mappings)} mapping(s) → s3://{RAW_BUCKET}/{key}")
    if failures:
        print(f"⚠  {len(failures)} failure(s) recorded in the staged file for review:")
        for f in failures:
            print(f"   {f['symbol']}: {f['error']}")
    print("\nNext: run notebooks/reference/06_load_vendor_id_seed.py in Databricks "
          "to review and load the staged mappings.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
