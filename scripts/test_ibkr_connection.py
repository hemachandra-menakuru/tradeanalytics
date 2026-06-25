#!/usr/bin/env python3
"""
TradeAnalytics — IBKR Client Portal REST API Connection Test
=============================================================
Run this BEFORE deploying to Databricks to verify IBKR connectivity.

Prerequisites:
  1. IBKR Client Portal Gateway running on localhost:5000
  2. Authenticated via browser (https://localhost:5000)
  3. Live or paper trading account

Usage:
    cd /Users/hemachandra/projects/tradeanalytics
    python scripts/test_ibkr_connection.py
"""

import json
import sys
import time
import urllib.request
import urllib.error
import ssl
from datetime import date, datetime
from pathlib import Path

repo_root = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(repo_root))

BASE_URL    = "https://localhost:5055/v1/api"
TIMEOUT_SEC = 30
TEST_SYMBOL = "SPY"


def _get(endpoint: str) -> dict:
    url = f"{BASE_URL}{endpoint}"
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode    = ssl.CERT_NONE
    req = urllib.request.Request(url)
    req.add_header("User-Agent", "TradeAnalytics/1.0")
    with urllib.request.urlopen(req, context=ctx, timeout=TIMEOUT_SEC) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _post(endpoint: str, data: dict = None) -> dict:
    url     = f"{BASE_URL}{endpoint}"
    ctx     = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode    = ssl.CERT_NONE
    payload = json.dumps(data or {}).encode("utf-8")
    req     = urllib.request.Request(url, data=payload, method="POST")
    req.add_header("Content-Type", "application/json")
    req.add_header("User-Agent", "TradeAnalytics/1.0")
    with urllib.request.urlopen(req, context=ctx, timeout=TIMEOUT_SEC) as resp:
        body = resp.read().decode("utf-8")
        return json.loads(body) if body else {}


def ok(name, detail=""):
    print(f"  ✅  {name}")
    if detail: print(f"       {detail}")

def fail(name, detail=""):
    print(f"  ❌  {name}")
    if detail: print(f"       {detail}")

def header(title):
    print(f"\n{'='*60}\n  {title}\n{'='*60}")


def test_1_gateway() -> bool:
    header("TEST 1 — Gateway Reachability")
    try:
        result = _post("/tickle")
        ok("Gateway responding on localhost:5055",
           f"session={str(result.get('session',''))[:20]}")
        return True
    except Exception as e:
        fail("Gateway responding on localhost:5000", str(e))
        print("\n  ⚠️  Start the IBKR Client Portal Gateway:")
        print("     1. Download from ibkr.com → Trading → IBKR APIs")
        print("     2. cd ~/ibkr-gateway && bin/run.sh root/conf.yaml")
        print("     3. Login at https://localhost:5000")
        return False


def test_2_auth() -> bool:
    header("TEST 2 — Session Authentication")
    try:
        r = _get("/iserver/auth/status")
        authenticated = r.get("authenticated", False)
        competing     = r.get("competing", False)
        if competing:
            print("  ⚠️  Competing session — another device is logged in")
        if authenticated:
            ok("Authenticated", f"connected={r.get('connected')}")
        else:
            fail("Not authenticated — open https://localhost:5000 and log in")
        return authenticated
    except Exception as e:
        fail("Auth status check failed", str(e))
        return False


def test_3_account() -> tuple:
    header("TEST 3 — Account Information")
    try:
        result = _get("/portfolio/accounts")
        if not result:
            fail("Account list readable", "Empty response")
            return False, ""
        account    = result[0] if isinstance(result, list) else result
        account_id = account.get("accountId", account.get("id", ""))
        ok("Account list readable",
           f"account_id={account_id}, "
           f"type={account.get('type','?')}, "
           f"currency={account.get('currency','?')}")
        return bool(account_id), account_id
    except Exception as e:
        fail("Account list readable", str(e))
        return False, ""


def test_4_contract(symbol: str) -> tuple:
    header(f"TEST 4 — Contract Search ({symbol})")
    try:
        result = _post(
            "/iserver/secdef/search",
            {"symbol": symbol, "name": False, "secType": "STK"}
        )
        if not result:
            fail(f"Contract found for {symbol}", "Empty response")
            return False, ""
        contracts = result if isinstance(result, list) else [result]
        contract  = contracts[0]
        conid     = str(contract.get("conid", ""))
        name      = contract.get("companyName", contract.get("description", ""))
        ok(f"Contract found for {symbol}", f"conid={conid}, name={name}")
        return bool(conid), conid
    except Exception as e:
        fail(f"Contract search failed for {symbol}", str(e))
        return False, ""


def test_5_historical(conid: str, symbol: str) -> bool:
    header(f"TEST 5 — Historical OHLCV ({symbol})")
    try:
        result = _get(
            f"/iserver/marketdata/history"
            f"?conid={conid}&period=5d&bar=1d&outsideRth=false"
        )
        data = result.get("data", [])
        if not data:
            fail("Historical data returned", "Empty data array")
            return False

        first     = data[0]
        has_ohlcv = all(k in first for k in ["o", "h", "l", "c", "v"])
        ok(f"Historical data returned", f"{len(data)} bars")

        # Show raw response
        print(f"\n  Raw IBKR response (first bar):")
        print(f"    {json.dumps(first, indent=4)}")

        # Map to our standard format
        ts_ms = first.get("t", 0)
        try:
            bar_date = datetime.fromtimestamp(ts_ms / 1000).strftime("%Y-%m-%d")
        except Exception:
            bar_date = str(date.today())

        standard = {
            "symbol":   symbol,
            "date":     bar_date,
            "interval": "1d",
            "source":   "ibkr",
            "open":     float(first.get("o", 0)),
            "high":     float(first.get("h", 0)),
            "low":      float(first.get("l", 0)),
            "close":    float(first.get("c", 0)),
            "volume":   int(first.get("v", 0)),
        }

        print(f"\n  Mapped to standard OHLCV format:")
        for k, v in standard.items():
            print(f"    {k}: {v}")

        return has_ohlcv
    except Exception as e:
        fail("Historical data request failed", str(e))
        return False


def test_6_quote(conid: str, symbol: str) -> bool:
    header(f"TEST 6 — Real-Time Quote ({symbol})")
    try:
        result = _get(
            f"/iserver/marketdata/snapshot"
            f"?conids={conid}&fields=31,84,86,7762,7295"
        )
        data  = result if isinstance(result, list) else [result]
        quote = data[0] if data else {}
        last  = quote.get("31", "N/A")
        bid   = quote.get("84", "N/A")
        ask   = quote.get("86", "N/A")
        ok("Quote snapshot returned", f"last={last}, bid={bid}, ask={ask}")
        if last == "N/A":
            print("  ℹ️  Market closed — delayed/no real-time data")
        return True
    except Exception as e:
        fail("Quote snapshot failed", str(e))
        return False


def test_7_keepalive() -> bool:
    header("TEST 7 — Session Keepalive (tickle)")
    try:
        _post("/tickle")
        ok("Tickle endpoint responding")
        return True
    except Exception as e:
        fail("Tickle failed", str(e))
        return False


def main():
    print("\n" + "="*60)
    print("  TradeAnalytics — IBKR Connection Test")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*60)

    results    = {}
    account_id = ""

    # Gateway must be up first
    results["1_gateway"] = test_1_gateway()
    if not results["1_gateway"]:
        print("\n❌ Gateway not running — cannot continue.\n")
        sys.exit(1)

    time.sleep(1)

    # Auth must be valid
    results["2_auth"] = test_2_auth()
    if not results["2_auth"]:
        print("\n❌ Not authenticated — open https://localhost:5000\n")
        sys.exit(1)

    # Account
    results["3_account"], account_id = test_3_account()

    # Contract search
    results["4_contract"], conid = test_4_contract(TEST_SYMBOL)

    # Market data
    if conid:
        time.sleep(1)
        results["5_historical"] = test_5_historical(conid, TEST_SYMBOL)
        time.sleep(1)
        results["6_quote"]      = test_6_quote(conid, TEST_SYMBOL)
    else:
        results["5_historical"] = False
        results["6_quote"]      = False

    # Keepalive
    results["7_keepalive"] = test_7_keepalive()

    # Summary
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
        print("\n  🎉 IBKR fully verified — ready to implement IBKRProvider!")
        if account_id:
            print(f"\n  Add to your .env file:")
            print(f"    IBKR_ACCOUNT_ID={account_id}")
    elif results.get("1_gateway") and results.get("2_auth"):
        print("\n  ⚠️  Connected but some tests failed (may be outside market hours)")
    else:
        print("\n  ❌ Connection failed")

    print()
    sys.exit(0 if passed >= 4 else 1)


if __name__ == "__main__":
    main()
