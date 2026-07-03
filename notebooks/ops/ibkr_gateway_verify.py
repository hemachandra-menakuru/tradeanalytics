# Databricks notebook source
# MAGIC %md
# MAGIC # IBKR Gateway Verification
# MAGIC
# MAGIC Run this notebook manually before any ingestion run, and always after switching
# MAGIC between paper and live trading modes.
# MAGIC
# MAGIC **What this checks:**
# MAGIC 1. Config values in use (gateway_mode, trading_mode, host, port)
# MAGIC 2. Gateway connectivity and authentication
# MAGIC 3. Account mode matches config (paper vs live)
# MAGIC 4. Live data fetch for a known symbol (SPY)
# MAGIC 5. Summary — pass/fail for each check

# COMMAND ----------
# MAGIC %md ## Step 0 — Environment guard (must run locally)

# COMMAND ----------

import os

# This notebook connects to the IBKR EC2 gateway (54.197.158.82:4004), whose
# security group only allows the owner's Mac IP. Databricks serverless/clusters
# egress from different IPs and will always be refused — fail fast with a clear
# message instead of a confusing ModuleNotFoundError or connection timeout.
if "DATABRICKS_RUNTIME_VERSION" in os.environ:
    raise RuntimeError(
        "STOP: this notebook must run LOCALLY on the Mac, not on Databricks.\n"
        "Serverless/cluster IPs are not whitelisted on the EC2 gateway security group.\n"
        "Run from terminal:\n"
        "  cd ~/pr/tradeanalytics && python -c \"exec(open('notebooks/ops/ibkr_gateway_verify.py').read())\"\n"
        "(use the tradeanalytics conda env python)"
    )

print("✅ Running locally — OK to proceed")

# COMMAND ----------
# MAGIC %md ## Step 1 — Configuration in use

# COMMAND ----------

from src.shared.config.config_loader import ConfigLoader

config = ConfigLoader.load()
cfg    = config.sources.ibinsync

gateway_mode  = getattr(cfg, "gateway_mode", "ec2")
trading_mode  = getattr(cfg, "trading_mode", "paper")
gateway_cfg   = getattr(cfg.gateways, gateway_mode)
host          = gateway_cfg.host
port_key      = f"port_{trading_mode}"
port          = int(getattr(gateway_cfg, port_key))

print("=" * 55)
print("  IBKR Gateway Configuration")
print("=" * 55)
print(f"  gateway_mode  : {gateway_mode}")
print(f"  trading_mode  : {trading_mode}")
print(f"  host          : {host}")
print(f"  port          : {port}  ({port_key})")
print("=" * 55)

# Safety check — warn loudly if live mode
if trading_mode == "live":
    print()
    print("  ⚠️  WARNING: LIVE TRADING MODE ACTIVE")
    print("  ⚠️  Real money at risk. Verify all checks below.")
    print("=" * 55)

# COMMAND ----------
# MAGIC %md ## Step 2 — Gateway connectivity

# COMMAND ----------

from src.bronze.providers.ibinsync_provider import IBInsyncProvider

provider = IBInsyncProvider(config)
health   = provider.health_check()

print(f"Health check : {'✅ PASS — gateway reachable and authenticated' if health else '❌ FAIL — cannot connect to gateway'}")

if not health:
    print()
    print("Troubleshooting:")
    print(f"  1. SSH to EC2 and check container: docker logs ibkr-gateway --tail 30")
    print(f"  2. Verify port {port} is open: nc -z {host} {port}")
    print(f"  3. Check security group allows your IP on port {port}")
    print(f"     Your current IP: run 'curl -s https://api.ipify.org' in terminal")
    raise SystemExit("Gateway unreachable — fix connectivity before proceeding")

# COMMAND ----------
# MAGIC %md ## Step 3 — Account mode matches config

# COMMAND ----------

from ib_insync import IB

ib = provider._ib  # reuse the connected IB instance from health check

accounts      = ib.managedAccounts()
account_str   = ", ".join(accounts) if accounts else "none returned"

# Paper accounts start with 'D'; live accounts start with 'U'
detected_modes = []
for acct in accounts:
    if acct.startswith("D"):
        detected_modes.append("paper")
    elif acct.startswith("U"):
        detected_modes.append("live")
    else:
        detected_modes.append("unknown")

print(f"Accounts returned : {account_str}")
print(f"Detected mode(s)  : {', '.join(set(detected_modes))}")
print(f"Config says       : {trading_mode}")
print()

mode_match = trading_mode in detected_modes
if mode_match:
    print(f"✅ PASS — account mode matches config ({trading_mode})")
else:
    print(f"❌ FAIL — MISMATCH: config says '{trading_mode}' but gateway returned '{', '.join(set(detected_modes))}'")
    print()
    print("Fix: update sources.ibinsync.trading_mode in config/sources.yml")
    print("     AND update TRADING_MODE in ~/ibkr-gateway/.env on EC2")
    print("     AND restart the container: docker compose down && docker compose up -d")

# COMMAND ----------
# MAGIC %md ## Step 4 — Live data fetch

# COMMAND ----------

from datetime import date, timedelta

# Use last 5 calendar days to avoid weekend/holiday edge cases
end_date   = date.today()
start_date = end_date - timedelta(days=7)

print(f"Fetching SPY data: {start_date} → {end_date}")
print()

records = provider.get_historical("SPY", start_date, end_date, "1d")

if records:
    print(f"✅ PASS — {len(records)} bar(s) returned")
    print()
    print(f"  {'Date':<12} {'Open':>8} {'High':>8} {'Low':>8} {'Close':>8} {'Volume':>12}")
    print(f"  {'-'*12} {'-'*8} {'-'*8} {'-'*8} {'-'*8} {'-'*12}")
    for r in records:
        print(f"  {str(r['date']):<12} {r['open']:>8.2f} {r['high']:>8.2f} {r['low']:>8.2f} {r['close']:>8.2f} {r['volume']:>12,}")
else:
    print("❌ FAIL — no records returned (market may be closed or data unavailable)")

# COMMAND ----------
# MAGIC %md ## Step 5 — Summary

# COMMAND ----------

checks = {
    "Config loaded"         : True,
    "Gateway reachable"     : health,
    "Account mode matches"  : mode_match,
    "Data fetch successful" : len(records) > 0,
}

print("=" * 45)
print("  Verification Summary")
print("=" * 45)
for check, passed in checks.items():
    status = "✅ PASS" if passed else "❌ FAIL"
    print(f"  {status}  {check}")
print("=" * 45)

all_passed = all(checks.values())
if all_passed:
    print()
    print(f"  ✅ ALL CHECKS PASSED")
    print(f"  Gateway: {gateway_mode} | Mode: {trading_mode} | Host: {host}:{port}")
    print()
    print("  Safe to run ingestion.")
else:
    failed = [k for k, v in checks.items() if not v]
    print()
    print(f"  ❌ {len(failed)} CHECK(S) FAILED: {', '.join(failed)}")
    print("  Do NOT run ingestion until all checks pass.")
print("=" * 45)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Switching between paper and live
# MAGIC
# MAGIC To switch trading mode, make these changes in order:
# MAGIC
# MAGIC **Step 1 — Update `config/sources.yml`:**
# MAGIC ```yaml
# MAGIC ibinsync:
# MAGIC   trading_mode: live   # was: paper
# MAGIC ```
# MAGIC
# MAGIC **Step 2 — Update `.env` on EC2:**
# MAGIC ```bash
# MAGIC ssh -i ~/.ssh/handh-trade-ibkr-proxy.pem ubuntu@54.197.158.82
# MAGIC nano ~/ibkr-gateway/.env
# MAGIC # Change: TRADING_MODE=paper  →  TRADING_MODE=live
# MAGIC # Change: IB_PORT=4004        →  IB_PORT=4001
# MAGIC ```
# MAGIC
# MAGIC **Step 3 — Update security group (add port 4001, remove 4004):**
# MAGIC ```bash
# MAGIC # Get your current IP first:
# MAGIC curl -s https://api.ipify.org
# MAGIC
# MAGIC # Add live port:
# MAGIC aws ec2 authorize-security-group-ingress --group-id sg-0bda28a18bc5bb48a \
# MAGIC   --protocol tcp --port 4001 --cidr <YOUR_IP>/32 --region us-east-1
# MAGIC
# MAGIC # Remove paper port:
# MAGIC aws ec2 revoke-security-group-ingress --group-id sg-0bda28a18bc5bb48a \
# MAGIC   --protocol tcp --port 4004 --cidr <YOUR_IP>/32 --region us-east-1
# MAGIC ```
# MAGIC
# MAGIC **Step 4 — Restart container:**
# MAGIC ```bash
# MAGIC ssh -i ~/.ssh/handh-trade-ibkr-proxy.pem ubuntu@54.197.158.82 \
# MAGIC   "cd ~/ibkr-gateway && docker compose down && docker compose up -d"
# MAGIC ```
# MAGIC
# MAGIC **Step 5 — Re-run this notebook and confirm all checks pass.**
# MAGIC
# MAGIC ⚠️ Before switching to live: move EC2 credentials from `.env` to AWS Secrets Manager.
