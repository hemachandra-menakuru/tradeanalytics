# Fetch Agent — Operational Runbook

**Component:** `fetch-agent-ibkr` — Connectivity & Execution Plane (Two-Plane Architecture, CLAUDE.md §14)
**Host:** EC2 `i-04eba7d6e9f3c6f28` (t4g.small), EIP `54.197.158.82`, Ubuntu 22.04
**Location on box:** `/home/ubuntu/fetch-agent-ibkr/` (script + venv)
**Depends on:** IB Gateway Docker container (`ibkr-gateway`, port 4004 paper) on the same box; S3 via IAM instance profile `handh-trade-ibkr-proxy-profile`
**Last updated:** 2026-07-05

---

## 1. What the agent is (30-second orientation)

A stateless courier. It polls `s3://handh-trade-raw-use1/control/fetch/ibkr/pending/`
for Contract-v2 manifests written by the Databricks planner, executes them against
IB Gateway on `localhost:4004`, lands raw results in S3, and moves each manifest to
`done/` or `failed/`. It holds **no business logic and no state** — everything it
needs arrives inside each manifest; all bookkeeping lives in Delta (Databricks-side).

Task types it handles: `FETCH_OHLCV` (bar data), `QUALIFY_INSTRUMENTS` (symbol→conId
batches for the seeding flow). Unknown task types / contract versions → `failed/`, loudly.

**Iron rules:**
- Exactly ONE agent process per vendor queue (flock-enforced; see §6.1)
- The agent NEVER writes to Delta tables
- Stopping the agent never loses work — pending manifests wait in S3

---

## 2. Standard operations (systemd — the normal mode)

> Until systemd is installed, see §3 for foreground mode. After installation,
> NEVER run the agent by hand — use these only.

```bash
# All from the Mac. Alias the SSH for convenience:
#   alias ibkrbox='ssh -i ~/.ssh/handh-trade-ibkr-proxy.pem ubuntu@54.197.158.82'

# Status + last 20 log lines
ibkrbox "systemctl status fetch-agent-ibkr --no-pager && journalctl -u fetch-agent-ibkr -n 20 --no-pager"

# Start / stop / restart
ibkrbox "sudo systemctl start fetch-agent-ibkr"
ibkrbox "sudo systemctl stop fetch-agent-ibkr"      # finishes current request, exits cleanly
ibkrbox "sudo systemctl restart fetch-agent-ibkr"

# Follow logs live
ibkrbox "journalctl -u fetch-agent-ibkr -f"

# Disable across reboots (maintenance)
ibkrbox "sudo systemctl disable --now fetch-agent-ibkr"
```

**Log signals to know:**
| Log line | Meaning |
|---|---|
| `fetch_agent v2 started — vendor=ibkr ...` | Healthy start; version banner MUST say v2+ |
| `N pending manifest(s)` | Work found; processing begins |
| `<key>: LANDED n bars → s3://…` | One request completed |
| `heartbeat — idle, queue empty, gateway up, N processed` | Hourly proof of life when idle |
| *silence < 1h while idle* | NORMAL (heartbeat covers liveness) |
| `... gateway ... is down — waiting` | Work exists but IB Gateway unreachable (§6.3) |
| `unsupported contract_version` / `unknown task_type` | Version skew agent↔planner (§6.2) |
| `Another fetch_agent ... already running` | Flock refused a duplicate launch (working as intended) |

---

## 3. Foreground mode (supervised runs / pre-systemd)

```bash
ibkrbox "cd ~/fetch-agent-ibkr && venv/bin/python fetch_agent.py"
```
- Output streams to YOUR terminal only. Closing the terminal usually kills the
  agent — but not always cleanly (see §6.1: the 4-orphan incident). ALWAYS stop
  with Ctrl+C, and verify with `pgrep` afterwards.
- The flock guard makes accidental duplicates exit immediately.

---

## 4. Deployment procedure (agent code change)

The agent is ONE file; deployment is scp + restart. A running Python process
never reloads its own file — **the restart is not optional**.

```bash
# 1. Copy the new file
scp -i ~/.ssh/handh-trade-ibkr-proxy.pem \
  /Users/hemachandra/projects/tradeanalytics/agents/fetch_agent/fetch_agent.py \
  ubuntu@54.197.158.82:~/fetch-agent-ibkr/

# 2. Restart (systemd)
ibkrbox "sudo systemctl restart fetch-agent-ibkr"

# 3. Verify the version banner
ibkrbox "journalctl -u fetch-agent-ibkr -n 3 --no-pager"   # expect 'fetch_agent v2 started'
```

Dependencies changed? (`ib_insync`/`boto3` versions):
```bash
ibkrbox "~/fetch-agent-ibkr/venv/bin/pip install --quiet <pkg>==<ver>" && restart
```

**Remember the four cache layers (all bitten us):**
| Layer | Symptom | Refresh |
|---|---|---|
| Databricks bundle copy | job/notebook runs old code | `databricks bundle deploy` |
| Databricks Git folder | manual notebook stale | Pull in Repos UI |
| Warm serverless session | imports cached from prior run | Detach/terminate session |
| Running agent process | old handlers/behaviour | `systemctl restart` after scp |

---

## 5. Health checks & diagnostics

```bash
# Is exactly one agent running?
ibkrbox "pgrep -af fetch_agent.py"                      # expect exactly 1 line

# Is the gateway container up and logged in?
ibkrbox "docker ps --format '{{.Names}} {{.Status}}' && docker logs ibkr-gateway --tail 5"

# Is the gateway port listening?
ibkrbox "nc -z localhost 4004 && echo PORT-OPEN || echo PORT-CLOSED"

# Queue state (from the Mac)
aws s3 ls s3://handh-trade-raw-use1/control/fetch/ibkr/pending/ | wc -l
aws s3 ls s3://handh-trade-raw-use1/control/fetch/ibkr/failed/  | wc -l

# Delta-side view (Databricks SQL) — the operator's primary dashboard
# SELECT status, COUNT(*) FROM tradeanalytics.control.fetch_request GROUP BY status;
# SELECT * FROM tradeanalytics.control.fetch_request
#   WHERE status='PENDING' AND requested_at < current_timestamp() - INTERVAL 2 HOURS;
```

**End-to-end smoke test** (proves gateway + account + data path):
run `notebooks/ops/connectivity_test.py` locally on the Mac —
Stage 1 network checks + Stage 2 real SPY fetch.

---

## 6. Failure scenarios & recovery (everything we've actually hit)

### 6.1 Multiple agent processes racing the queue  *(hit 2026-07-04: 4 orphans)*
**Symptom:** requests failed with stale-code errors while "the" agent looks fine;
`pgrep -af fetch_agent.py` shows >1 PID.
**Cause:** foreground agents orphaned by closed SSH sessions (pre-flock, pre-systemd).
**Fix:**
```bash
ibkrbox "pkill -f fetch_agent.py; sleep 2; pgrep -af fetch_agent.py || echo ALL-DEAD"
# then start exactly one (systemd start, or foreground)
```
**Prevention (now structural):** flock singleton in the agent + systemd singleton.
Duplicates exit with "Another fetch_agent ... already running".

### 6.2 Version skew: `unsupported contract_version` / `unknown task_type`
**Symptom:** manifests moved straight to `failed/` with these messages.
**Cause:** planner and agent deployed at different code versions (either direction).
This is the CONTRACT WORKING — refusing to guess — not data loss.
**Fix:** deploy the lagging side (§4 for agent; `bundle deploy` + fresh session for
planner), then re-emit the work: delete affected `fetch_request` rows + failed
manifests, re-run the planner. For QUALIFY requests just re-run the seeding notebook.

### 6.3 IB Gateway down / not logged in
**Symptom:** agent logs `gateway ... is down — waiting` (work queues up safely);
or fetches fail with connection errors.
**Diagnose:** §5 checks; VNC for visual state: `vnc://54.197.158.82:5900` (Mac ⌘K).
**Fix:**
```bash
ibkrbox "cd ~/ibkr-gateway && docker compose restart"    # auto-relogin via IBC
# wait ~60s; check: docker logs ibkr-gateway --tail 20  → 'Login has completed'
```
**Known login gotchas:** newly created paper account shows "Application In Progress"
→ wait for IBKR processing (next business day). Nightly IBKR maintenance restarts
are normal — agent retries and recovers unattended.

### 6.4 TrustedIPs / port regressions after gateway container recreate
**Symptom:** connection reset (TrustedIPs) or refused (port) from off-box clients;
agent on-box is usually fine (localhost is always trusted).
**Cause:** IBC regenerates configs from templates; the gnzsnz image uses port 4004
(paper), not the documented 4002; `docker compose restart` does NOT apply port changes.
**Fix:** edit TEMPLATES not live files; recreate not restart:
```bash
ibkrbox "docker exec ibkr-gateway sed -i 's/TrustedTwsApiClientIPs=/TrustedTwsApiClientIPs=*/' /home/ibgateway/ibc/config.ini.tmpl"
ibkrbox "cd ~/ibkr-gateway && docker compose down && docker compose up -d"
# verify actual listening ports:
ibkrbox "docker exec ibkr-gateway cat /proc/net/tcp | awk '{print \$2}' | grep -v local | while read h; do printf '%d\n' 0x\${h#*:}; done | sort -nu"
```

### 6.5 S3 permission errors (AccessDenied)
**Symptom:** agent errors on list/get/put to S3.
**Diagnose:** instance profile still attached?
```bash
aws ec2 describe-iam-instance-profile-associations \
  --filters Name=instance-id,Values=i-04eba7d6e9f3c6f28 --region us-east-1 --output text
```
**Fix:** re-associate `handh-trade-ibkr-proxy-profile`; policy
`handh-trade-fetch-agent-ibkr-s3` must be attached to `handh-trade-ibkr-proxy-role`.
Note: the policy is scoped to `control/fetch/ibkr/*` and `ibkr/*` ONLY — errors on
other prefixes are the guardrail, not a bug.

### 6.6 Requests stuck in `failed/`
**Triage by `error_message` in the manifest:**
- Transient (pacing, disconnect): planner re-plans automatically next run
  (watermark didn't advance) — usually no action.
- Contract/version skew: §6.2.
- `No security definition` on QUALIFY: dead/placeholder ticker (e.g. `2602335D`) —
  expected; investigate the listing row, consider closing it (SCD-2).
- Persistent per-symbol failures: check `reference.instrument_vendor_id` mapping;
  a vendor-side conId change surfaces here → SCD-2 correction + corporate-action review.
Cleanup: failed manifests are audit artifacts; archive/delete after resolution.

### 6.7 Agent alive but silent
Idle silence < 1 hour is NORMAL. Judge liveness by the hourly heartbeat line,
not by chatter. No heartbeat for > 1h = restart (§2) and investigate journal.

### 6.8 EC2 box lost entirely
Nothing irreplaceable lives on the box. Rebuild ~30 min:
launch t4g.small in `handh-trade-vpc` → attach EIP + instance profile + SG
`sg-0bda28a18bc5bb48a` → install docker + compose → restore `~/ibkr-gateway/`
(compose file in repo history + paper creds from Keychain) → §4 agent deploy →
systemd unit from `agents/fetch_agent/fetch-agent-ibkr.service`.
Queue state is untouched in S3/Delta; the agent resumes the backlog.

---

## 7. Emergency stop & pause semantics

| Goal | Action | Effect |
|---|---|---|
| Stop fetching NOW | `sudo systemctl stop fetch-agent-ibkr` | Finishes in-flight request, exits; queue freezes safely |
| Pause one ticker | `UPDATE reference.ticker_feed_config SET is_active=false WHERE …` | Planner stops emitting for it |
| Stop new work | Don't run / pause the planner job | Queue drains, then idle |
| Nuke queued work | `DELETE FROM control.fetch_request WHERE status='PENDING'` + `aws s3 rm …/pending/ --recursive` | Removes not-yet-fetched work (safe: re-derivable by planner) |

Order of preference: control the WORK (Databricks-side, single SQL) before
controlling the PROCESS. Stopping the agent is rarely the right first move.

## 8. Known clientId registry (collisions break connections)

| clientId | Process |
|---|---|
| 10 | IBInsyncProvider (Mac dev/backtest) |
| 20 | fetch agent (EC2) |
| 99 | connectivity_test / seeding diagnostics |

Allocate new IDs here AND in CLAUDE.md §14.
