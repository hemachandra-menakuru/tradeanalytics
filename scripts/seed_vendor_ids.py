#!/usr/bin/env python3
"""
One-time supervised seeding of reference.instrument_vendor_id (vendor=ibkr).

Why a SEED script instead of fetch-time symbol fallback:
  - Seeding runs NOW, while every symbol in the universe is verifiably
    current — ticker rename/reuse risk is ~zero at seed time.
  - Output is reviewable SQL — a human eyeballs company/exchange per conId
    BEFORE anything enters the reference table.
  - After seeding, the planner's require_vendor_id policy guarantees no
    fetch ever depends on a symbol lookup again.

Runs LOCALLY on the Mac (uses the EC2 IB Gateway, clientId 99 = diagnostics).
Reads the current universe from Databricks via Databricks Connect serverless,
qualifies each symbol, prints INSERT statements to review and run in Databricks.

Usage:
    python scripts/seed_vendor_ids.py            # all unmapped current listings
"""

from __future__ import annotations

import os
import sys
import time

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

GATEWAY_HOST = "54.197.158.82"
GATEWAY_PORT = 4004      # paper
CLIENT_ID    = 99        # diagnostics clientId (CLAUDE.md registry)
VENDOR       = "ibkr"
CATALOG      = "tradeanalytics"


def main() -> int:
    # 1. Current listings missing an ibkr mapping — from Delta
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
        print("Nothing to seed — every current listing already has an ibkr mapping.")
        return 0
    print(f"{len(rows)} unmapped listing(s): {[r.symbol for r in rows]}\n")

    # 2. Qualify each via the EC2 gateway
    from ib_insync import IB, Stock
    ib = IB()
    ib.connect(GATEWAY_HOST, GATEWAY_PORT, clientId=CLIENT_ID, timeout=30, readonly=True)

    inserts, failures = [], []
    for r in rows:
        try:
            contract = Stock(r.symbol, "SMART", r.currency or "USD")
            ib.qualifyContracts(contract)
            if not contract.conId:
                raise ValueError("qualification returned no conId")
            desc = f"{contract.localSymbol} @ {contract.primaryExchange} ({contract.currency})"
            print(f"  {r.symbol:<6} → conId {contract.conId:<12} {desc}")
            inserts.append(
                f"INSERT INTO {CATALOG}.reference.instrument_vendor_id\n"
                f"    (instrument_id, vendor, vendor_instrument_id, vendor_exchange,\n"
                f"     valid_from, is_current, notes, created_at)\n"
                f"VALUES ({r.instrument_id}, '{VENDOR}', '{contract.conId}', "
                f"'{contract.primaryExchange}',\n"
                f"    current_date(), true, "
                f"'seeded via scripts/seed_vendor_ids.py — verified {desc}', "
                f"current_timestamp());"
            )
        except Exception as e:
            failures.append((r.symbol, f"{type(e).__name__}: {e}"))
            print(f"  {r.symbol:<6} → FAILED: {e}")
        time.sleep(1)  # gentle pacing

    ib.disconnect()

    # 3. Reviewable SQL
    print("\n" + "=" * 70)
    print("-- REVIEW each mapping above (symbol ↔ exchange ↔ conId), then run")
    print("-- the following in a Databricks SQL cell:")
    print("=" * 70 + "\n")
    for stmt in inserts:
        print(stmt + "\n")

    if failures:
        print(f"-- ⚠ {len(failures)} symbol(s) failed qualification — investigate before seeding:")
        for sym, err in failures:
            print(f"--   {sym}: {err}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
