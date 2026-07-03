# Databricks notebook source
# TradeAnalytics — Connectivity Test (ops diagnostic)
#
# Run this from ANY compute environment to answer:
#   1. What environment am I running in? (serverless / classic cluster / local Mac)
#   2. Is general internet egress possible?
#   3. Can this compute reach the EC2 IB Gateway (54.197.158.82:4004)?
#   4. What is this compute's egress IP? (needed for SG / NCC stable-IP decisions)
#
# No project imports, no dependencies beyond the standard library — runs on a
# bare serverless environment with zero setup. Never ingests anything.
#
# How to run:
#   - Databricks: open this notebook, attach Serverless, Run all
#   - Local Mac:  python notebooks/ops/connectivity_test.py

# COMMAND ----------
import os
import socket
import urllib.request

# ── Targets ──────────────────────────────────────────────────────────────────
EC2_GATEWAY_HOST = "54.197.158.82"
EC2_GATEWAY_PORT = 4004            # IB Gateway paper port (gnzsnz image)

# ── Environment detection ────────────────────────────────────────────────────
IS_DATABRICKS = "DATABRICKS_RUNTIME_VERSION" in os.environ
IS_SERVERLESS = IS_DATABRICKS and (
    "client." in os.environ.get("DATABRICKS_RUNTIME_VERSION", "")
    or os.environ.get("IS_SERVERLESS", "").lower() == "true"
)
EXEC_ENV = (
    "databricks-serverless" if IS_SERVERLESS
    else "databricks-cluster" if IS_DATABRICKS
    else "local"
)

# COMMAND ----------
def tcp_check(host: str, port: int, timeout: int = 10) -> str:
    """Plain TCP connect — proves network path is open, no protocol needed."""
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return "✅ reachable"
    except Exception as e:
        return f"❌ blocked ({type(e).__name__}: {e})"


def http_check(url: str, timeout: int = 10) -> str:
    try:
        with urllib.request.urlopen(url, timeout=timeout) as r:
            return f"✅ ok (HTTP {r.status})"
    except Exception as e:
        return f"❌ blocked ({type(e).__name__}: {e})"


results = {
    "exec_env":            EXEC_ENV,
    "runtime_version":     os.environ.get("DATABRICKS_RUNTIME_VERSION", "n/a (local)"),
    "internet_tcp_443":    tcp_check("www.google.com", 443),
    "internet_http_get":   http_check("https://api.ipify.org"),
    "ec2_ib_gateway":      tcp_check(EC2_GATEWAY_HOST, EC2_GATEWAY_PORT),
}

# Egress IP — the key fact for the security-group / NCC stable-IP decision
try:
    with urllib.request.urlopen("https://api.ipify.org", timeout=10) as r:
        results["egress_ip"] = r.read().decode()
except Exception:
    results["egress_ip"] = "unknown (no internet egress)"

# COMMAND ----------
print("=" * 64)
print("  STAGE 1 — NETWORK CHECKS")
print("=" * 64)
for k, v in results.items():
    print(f"  {k:<22} {v}")
print("=" * 64)
print()
print("  How to interpret (running on Databricks):")
print("  - internet ✅ + ec2 ❌ : egress works; SG blocks us.")
print("      → note egress_ip; evaluate NCC stable egress IPs.")
print("  - internet ❌ + ec2 ❌ : no egress at all from this compute.")
print("      → EC2 fetch-runner architecture is the path.")
print("  - internet ✅ + ec2 ✅ : network path fully open — verify SG rules")
print("      are as intended before trusting this.")
print("=" * 64)

# COMMAND ----------
# ── Stage 2 — real data fetch through the gateway (full confidence check) ───
# Only attempted when the network path is open AND ib_insync is available.
# Uses ib_insync directly (no project imports) so this notebook stays runnable
# on a bare environment — Stage 1 always completes regardless.

gateway_open = results["ec2_ib_gateway"].startswith("✅")
data_check   = "not attempted"

if not gateway_open:
    data_check = "SKIPPED — gateway not reachable (see Stage 1)"
else:
    try:
        from ib_insync import IB, Stock
    except ImportError:
        data_check = ("SKIPPED — ib_insync not installed in this environment "
                      "(add it via the notebook Environment panel or job env spec)")
        IB = None

    if gateway_open and IB is not None:
        print("=" * 64)
        print("  STAGE 2 — DATA FETCH VERIFICATION")
        print("=" * 64)
        ib = IB()
        try:
            ib.connect(EC2_GATEWAY_HOST, EC2_GATEWAY_PORT, clientId=99,
                       timeout=30, readonly=True)

            # Account mode check — paper accounts start with 'D', live with 'U'
            accounts = ib.managedAccounts()
            modes = {("paper" if a.startswith("D") else
                      "live"  if a.startswith("U") else "unknown")
                     for a in accounts}
            expected = "paper" if EC2_GATEWAY_PORT == 4004 else "live"
            mode_ok = expected in modes
            print(f"  Accounts          : {', '.join(accounts)}")
            print(f"  Detected mode     : {', '.join(modes)} | expected: {expected}")
            print(f"  {'✅ PASS' if mode_ok else '❌ FAIL'} — account mode "
                  f"{'matches' if mode_ok else 'DOES NOT match'} gateway port")
            print()

            # Fetch last week of SPY daily bars
            contract = Stock("SPY", "SMART", "USD")
            ib.qualifyContracts(contract)
            bars = ib.reqHistoricalData(
                contract, endDateTime="", durationStr="7 D",
                barSizeSetting="1 day", whatToShow="TRADES",
                useRTH=True, formatDate=1,
            )
            if bars:
                print(f"  ✅ PASS — {len(bars)} bar(s) returned for SPY")
                print()
                print(f"  {'Date':<12} {'Open':>8} {'High':>8} {'Low':>8} {'Close':>8} {'Volume':>12}")
                print(f"  {'-'*12} {'-'*8} {'-'*8} {'-'*8} {'-'*8} {'-'*12}")
                for b in bars:
                    print(f"  {str(b.date):<12} {b.open:>8.2f} {b.high:>8.2f} "
                          f"{b.low:>8.2f} {b.close:>8.2f} {int(b.volume):>12,}")
                data_check = f"✅ PASS — {len(bars)} bars, mode={'/'.join(modes)}"
            else:
                print("  ❌ FAIL — connected but no bars returned")
                data_check = "❌ FAIL — no bars returned"
        except Exception as e:
            print(f"  ❌ FAIL — {type(e).__name__}: {e}")
            data_check = f"❌ FAIL — {type(e).__name__}: {e}"
        finally:
            if ib.isConnected():
                ib.disconnect()
        print("=" * 64)

results["data_fetch"] = data_check

# COMMAND ----------
print("=" * 64)
print("  OVERALL SUMMARY")
print("=" * 64)
for k, v in results.items():
    print(f"  {k:<22} {v}")
print("=" * 64)
