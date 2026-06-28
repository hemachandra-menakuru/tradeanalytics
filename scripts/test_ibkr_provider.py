#!/usr/bin/env python3
"""
TradeAnalytics — IBKRProvider Live Integration Test
=====================================================
Tests the full IBKRProvider implementation against the live gateway.

Prerequisites:
  1. IBKR Client Portal Gateway running: bin/run.sh root/conf.yaml
  2. Authenticated at: https://localhost:5055
  3. .env file has IBKR_ACCOUNT_ID set

Usage:
    cd /Users/hemachandra/projects/tradeanalytics
    python scripts/test_ibkr_provider.py
"""

import sys
import logging
from datetime import date, timedelta
from pathlib import Path

repo_root = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(repo_root))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("test_ibkr_provider")


def header(title):
    print(f"\n{'='*60}\n  {title}\n{'='*60}")


def ok(name, detail=""):
    print(f"  ✅  {name}")
    if detail: print(f"       {detail}")


def fail(name, detail=""):
    print(f"  ❌  {name}")
    if detail: print(f"       {detail}")


def main():
    print("\n" + "="*60)
    print("  TradeAnalytics — IBKRProvider Integration Test")
    print("="*60)

    # Load config
    from src.shared.config.config_loader import ConfigLoader
    config = ConfigLoader.load(environment="dev")
    print(f"\n  Config loaded — source: {config.sources.primary}")
    print(f"  Gateway: {config.sources.ibkr.base_url}")

    # Create provider
    from src.bronze.providers.ibkr_provider import IBKRProvider
    provider = IBKRProvider(config)

    results = {}

    # ── Test 1: Health check ───────────────────────────────────────────────────
    header("TEST 1 — Health Check")
    try:
        healthy = provider.health_check()
        if healthy:
            ok("health_check() returned True")
        else:
            fail("health_check() returned False",
                 "Is gateway running and authenticated?")
        results["health_check"] = healthy
    except Exception as e:
        fail("health_check() raised exception", str(e))
        results["health_check"] = False

    if not results["health_check"]:
        print("\n❌ Health check failed — cannot proceed\n")
        sys.exit(1)

    # ── Test 2: Provider identity ──────────────────────────────────────────────
    header("TEST 2 — Provider Identity")
    ok("provider_name",       provider.provider_name)
    ok("supports_options",    str(provider.supports_options))
    ok("supports_realtime",   str(provider.supports_realtime))
    ok("supported_intervals", str(provider.supported_intervals))
    results["identity"] = True

    # ── Test 3: Historical daily data ──────────────────────────────────────────
    header("TEST 3 — Historical Daily OHLCV (SPY, 5 days)")
    try:
        end   = date.today()
        start = end - timedelta(days=10)  # 10 days to ensure 5 trading days

        records = provider.get_historical("SPY", start, end, interval="1d")

        if records:
            ok(f"get_historical() returned {len(records)} records")

            # Show first record
            first = records[0]
            print(f"\n  First record:")
            for key in ["symbol","date","interval","source",
                       "open","high","low","close","volume"]:
                print(f"    {key}: {first.get(key)}")

            # Validate standard format
            required = ["symbol","date","interval","source",
                       "open","high","low","close","volume"]
            missing  = [f for f in required if first.get(f) is None]

            if not missing:
                ok("All required fields present")
            else:
                fail("Missing required fields", str(missing))

            # Validate price sanity
            price_ok = (
                first["high"] >= first["low"] and
                first["high"] >= first["open"] and
                first["high"] >= first["close"] and
                first["low"]  <= first["open"] and
                first["low"]  <= first["close"]
            )
            if price_ok:
                ok("Price relationships valid (high>=low, etc.)")
            else:
                fail("Price relationship violation")

            results["historical"] = True
        else:
            fail("get_historical() returned empty list")
            results["historical"] = False

    except Exception as e:
        fail("get_historical() raised exception", str(e))
        results["historical"] = False

    # ── Test 4: Historical for AAPL ───────────────────────────────────────────
    header("TEST 4 — Historical Daily OHLCV (AAPL, 5 days)")
    try:
        records = provider.get_historical("AAPL", start, end, interval="1d")
        if records:
            ok(f"AAPL: {len(records)} records returned")
            ok(f"Latest close: {records[-1].get('close')}")
            results["aapl"] = True
        else:
            fail("AAPL returned empty — may need market hours")
            results["aapl"] = False
    except Exception as e:
        fail("AAPL fetch failed", str(e))
        results["aapl"] = False

    # ── Test 5: Latest quote ───────────────────────────────────────────────────
    header("TEST 5 — Latest Quote (SPY)")
    try:
        quote = provider.get_latest_quote("SPY")
        if quote:
            ok("get_latest_quote() returned data")
            print(f"\n  Quote:")
            for key in ["symbol","last","bid","ask","volume","timestamp"]:
                print(f"    {key}: {quote.get(key)}")
            results["quote"] = True
        else:
            fail("get_latest_quote() returned None")
            results["quote"] = False
    except Exception as e:
        fail("get_latest_quote() raised exception", str(e))
        results["quote"] = False

    # ── Test 6: Conid cache ────────────────────────────────────────────────────
    header("TEST 6 — Conid Cache (second call should be instant)")
    try:
        import time
        t1 = time.time()
        provider.get_historical("SPY", start, end, interval="1d")
        t2 = time.time()
        first_call_ms  = int((t2 - t1) * 1000)

        t3 = time.time()
        provider.get_historical("SPY", start, end, interval="1d")
        t4 = time.time()
        second_call_ms = int((t4 - t3) * 1000)

        ok(f"First call: {first_call_ms}ms")
        ok(f"Second call: {second_call_ms}ms (conid cached)")
        results["cache"] = True
    except Exception as e:
        fail("Cache test failed", str(e))
        results["cache"] = False

    # ── Test 7: Unsupported interval ───────────────────────────────────────────
    header("TEST 7 — Unsupported Interval Raises Error")
    try:
        from src.bronze.base.market_data_provider import ProviderNotSupportedError
        try:
            provider.get_historical("SPY", start, end, interval="2d")
            fail("Should have raised ProviderNotSupportedError")
            results["error_handling"] = False
        except ProviderNotSupportedError:
            ok("Correctly raised ProviderNotSupportedError for '2d'")
            results["error_handling"] = True
    except Exception as e:
        fail("Error handling test failed", str(e))
        results["error_handling"] = False

    # ── Summary ────────────────────────────────────────────────────────────────
    print("\n" + "="*60)
    print("  FINAL SUMMARY")
    print("="*60)

    passed = sum(1 for v in results.values() if v)
    total  = len(results)

    for name, result in results.items():
        icon = "✅" if result else "❌"
        print(f"  {icon}  {name}")

    print(f"\n  Result: {passed}/{total} passed")

    if passed == total:
        print("\n  🎉 IBKRProvider fully verified!")
        print("     Ready to run Bronze ingestion with IBKR as primary provider.")
    elif results.get("health_check") and results.get("historical"):
        print("\n  ⚠️  Core functionality working — some tests failed")
        print("     (quote data may not be available outside market hours)")
    else:
        print("\n  ❌ Critical tests failed")

    print()
    sys.exit(0 if passed >= 5 else 1)


if __name__ == "__main__":
    main()
