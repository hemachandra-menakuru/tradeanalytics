# End-to-End Data Ingestion Run Guide

A complete operational guide to running the TradeAnalytics Bronze ingestion
pipeline, written for an engineer with no prior project knowledge. Follow it
top to bottom for a full run; jump to §11–13 for troubleshooting/recovery.

**Companion docs:** [ingestion_workflow.md](../workflows/ingestion_workflow.md)
(concepts + sequence diagram), [fetch_agent_runbook.md](fetch_agent_runbook.md)
(agent ops), [systemd_service_guide.md](systemd_service_guide.md) (service model),
CLAUDE.md §14 (architecture decision record).
**Last validated end-to-end:** 2026-07-05 (SPY, 2016→2026, clean-room run).

---

## 1. Overview — what this pipeline does

Ingests daily OHLCV market data from IBKR into the Bronze Delta layer, using a
**Two-Plane Architecture** because Databricks Serverless has zero outbound
network egress in this account (verified; see CLAUDE.md §14).

```
PLANE 1 — Databricks Serverless (data & intelligence)
  ① fetch_planner ──writes──► control.fetch_request (Delta) + S3 manifests
                                            │
                    (S3 is the ONLY channel between planes)
                                            ▼
PLANE 2 — EC2 (connectivity)
  ② fetch-agent-ibkr ──polls S3, fetches via IB Gateway──► S3 raw payloads
                                            │
PLANE 1 — Databricks Serverless
  ③ raw_to_bronze ──reads raw, validates──► bronze.market_data_daily
                    ──updates──► ingestion_watermark, job_run_log
```

Every arrow between planes is S3. Databricks never opens an outbound socket;
the EC2 agent only ever connects outward (to localhost gateway + S3).

---

## 1b. Cost safety (READ THIS — real money)

Databricks Serverless bills **DBUs while a compute session is active/warm**.
Two rules keep the bill small; ignoring them cost ~77 DBU (~$68) on 2026-07-07.

1. **Run ingestion as the deployed JOB, never interactively.**
   - ✅ `databricks bundle run raw_to_bronze`  (or Workflows → Run now)
   - ❌ opening the notebook and hitting "Run all"
   - Why: a **Job** spins up serverless, runs, and **auto-terminates** — you pay
     only for the run, and the `timeout_seconds: 3600` cap kills any runaway at
     1 hour. **Interactive** serverless has NO task timeout and the session stays
     **warm and billing** as long as you keep issuing cells/queries. On 2026-07-07
     an interactive run stayed warm ~7 hours across re-runs + verification queries.
   - Safeguard in place: `raw_to_bronze` notebook has an `execute` gate (default
     false). Interactive "Run all" prints instructions and **exits without running
     the heavy job**. The Job sets `execute=true` automatically.

2. **Detach/terminate the serverless session when you finish poking around.**
   - After ad-hoc SQL / verification queries in a notebook, terminate the session:
     compute dropdown (top-right) → Terminate/Detach. A warm idle session still bills.
   - Serverless idle auto-termination exists but resets every time you run a cell —
     so a morning of running queries every few minutes = a session warm all morning.

3. **Watch usage:** Account console → Usage. If a single notebook shows many
   consecutive 10-minute intervals at a steady DBU rate, that's a warm interactive
   session — terminate it.

## 2. Prerequisites & access

| Need | Detail |
|---|---|
| AWS CLI | Configured; `AWS_PROFILE=handh-trade` (set in shell). Account `311925399625`, region `us-east-1` |
| Databricks CLI | v0.218.0+, profile `handh-trade-aws`, workspace `dbc-46a555ac-7f7b.cloud.databricks.com` |
| Conda env | `tradeanalytics` (Python 3.11) — `/Users/hemachandra/anaconda3/envs/tradeanalytics/bin/python` |
| SSH to EC2 | Key `~/.ssh/handh-trade-ibkr-proxy.pem`; alias `ibkrbox='ssh -i ~/.ssh/handh-trade-ibkr-proxy.pem ubuntu@54.197.158.82'` (add to `~/.zshrc`) |
| Databricks workspace | Admin on `handh-dev`; Git folder synced to `feature/phase3-silver` |
| Secrets | Databricks secret scope `tradeanalytics` holds `IBKR_ACCOUNT_ID`; EC2 `.env` holds paper creds |
| IB Gateway | Paper account activated; container running on EC2 |

---

## 3. Object catalogue (everything by name)

**AWS**
| Object | Name |
|---|---|
| EC2 instance | `i-04eba7d6e9f3c6f28` (t4g.small), EIP `54.197.158.82` |
| Security group | `sg-0bda28a18bc5bb48a` (SSH 22, IB API 4004, VNC 5900 — owner IP only) |
| IAM instance profile / role | `handh-trade-ibkr-proxy-profile` / `handh-trade-ibkr-proxy-role` |
| IAM policy | `handh-trade-fetch-agent-ibkr-s3` (scoped to `control/fetch/ibkr/*`, `ibkr/*`) |
| Raw bucket | `s3://handh-trade-raw-use1` |
| — queue prefix | `control/fetch/ibkr/{pending,done,failed}/` + `done/archive/<date>/` |
| — landing prefix | `ibkr/ohlcv_daily/ingest_date=<date>/` |
| — vendor-id staging | `reference/vendor_id_seed/ibkr/<timestamp>.json` |
| Refined bucket (Delta) | `s3://handh-trade-refined-use1/{bronze,silver,gold}/` |

**Databricks / Unity Catalog** (catalog `tradeanalytics`)
| Object | Name |
|---|---|
| Control tables | `control.fetch_request`, `control.ingestion_watermark`, `control.job_run_log`, `control.ingestion_command` |
| Reference tables | `reference.instrument`, `reference.instrument_listing`, `reference.instrument_vendor_id`, `reference.ticker_feed_config` |
| Bronze tables | `bronze.market_data_daily`, `bronze.market_data_rejected` |
| DABs jobs | `fetch_planner`, `raw_to_bronze`, `bronze_daily_ingestion` (dev-only, superseded) |
| Notebooks | `notebooks/control/fetch_planner.py`, `notebooks/bronze/raw_to_bronze.py`, `notebooks/reference/06_seed_vendor_ids.py` |
| EC2 service | `fetch-agent-ibkr.service` (systemd), dir `/home/ubuntu/fetch-agent-ibkr/` |

---

## 4. Dependency order (must hold before a run)

```
reference.instrument_vendor_id seeded (conIds)   ← §7 one-time
        └─► fetch-agent-ibkr service ACTIVE       ← §6 check
                └─► bundle deployed (latest code) ← §5
                        └─► RUN: planner → agent → raw_to_bronze  ← §8
```
If any upstream box is not satisfied, the run will stall or skip. §8 checks each.

---

## 5. Deploy the code (before any run after a code change)

Three cache layers — refresh the ones affected by your change:

```bash
# 1. Bundle (what the scheduled/UI jobs run) — always after code changes
cd /Users/hemachandra/projects/tradeanalytics
databricks bundle validate
databricks bundle deploy --var "pipeline_version=$(git rev-parse --short HEAD)"
#   validate: catches YAML/spec errors before hitting the API
#   deploy:   uploads notebooks + src to .bundle/.../files and updates jobs

# 2. Agent file (only if agents/fetch_agent/fetch_agent.py changed)
scp -i ~/.ssh/handh-trade-ibkr-proxy.pem \
  agents/fetch_agent/fetch_agent.py ubuntu@54.197.158.82:~/fetch-agent-ibkr/
ibkrbox "sudo systemctl restart fetch-agent-ibkr"

# 3. Interactive notebooks (06_seed etc.): Pull in the Databricks Git folder,
#    and TERMINATE the serverless session if src/ modules changed (warm sessions
#    cache imports).
```
**Success:** `Validation OK!` then `Deployment complete!`.

---

## 6. Pre-run health checks

```bash
# Agent alive?  (expect: active)
ibkrbox "systemctl is-active fetch-agent-ibkr"

# Gateway container up + port open?  (expect: running, PORT-OPEN)
ibkrbox "docker ps --format '{{.Names}} {{.Status}}' && (nc -z localhost 4004 && echo PORT-OPEN || echo PORT-CLOSED)"

# Exactly one agent process?  (expect: 1 line)
ibkrbox "pgrep -f 'venv/bin/python fetch_agent.py'"

# Queue empty from a prior run?  (expect: 0 / 0)
aws s3 ls s3://handh-trade-raw-use1/control/fetch/ibkr/pending/ | wc -l
```
If the agent is `inactive`: `ibkrbox "sudo systemctl start fetch-agent-ibkr"`.
If the gateway is down: §11.3.

---

## 7. One-time prerequisite — seed vendor IDs

Required before the planner will emit for any instrument (the `require_vendor_id`
policy blocks unmapped instruments). Fully Databricks-run; agent must be active.

1. Databricks → open `notebooks/reference/06_seed_vendor_ids.py` → Serverless → **Run all**.
2. It submits a `QUALIFY_INSTRUMENTS` manifest; the agent qualifies each symbol
   (~1s each) and returns conIds; the notebook loads new-only, refuses conflicts.
3. **Validate:**
   ```sql
   SELECT vendor, is_current, COUNT(*) FROM tradeanalytics.reference.instrument_vendor_id
   GROUP BY vendor, is_current;                       -- expect ibkr/true ≈ 505
   ```
   Re-runnable anytime (differential/idempotent). Failures (dead tickers like
   `2602335D`) are reported, not loaded.

---

## 8. Run the pipeline (the core sequence)

### Stage A — Planner (Databricks serverless)
Databricks → `fetch_planner` notebook (fresh serverless session) → widgets:
`symbols`=`SPY` (blank = all active), `dry_run`=`false`, `vendor`=`ibkr` → **Run all**.

**Expected summary:**
```
requests_emitted   11        (or 1 if incremental, 0 if up to date)
skipped_noop       []
skipped_inflight   []
skipped_unmapped   []        (non-empty = §7 not done for those symbols)
orphans_repaired   0
```
**Validate:**
```sql
SELECT status, COUNT(*) FROM tradeanalytics.control.fetch_request GROUP BY status;  -- PENDING = N
```
```bash
aws s3 ls s3://handh-trade-raw-use1/control/fetch/ibkr/pending/ | wc -l             -- = N
```

### Stage B — Agent (EC2, automatic)
No action — the always-on agent picks up manifests within ~60s. Watch it:
```bash
ibkrbox "journalctl -u fetch-agent-ibkr -f"
```
**Expected:** `N pending manifest(s)` → `... LANDED ~250 bars → s3://...` × N. Ctrl+C stops watching (not the service).
**Validate:**
```bash
aws s3 ls s3://handh-trade-raw-use1/control/fetch/ibkr/done/   | grep -c batch_daily   # = N
aws s3 ls s3://handh-trade-raw-use1/control/fetch/ibkr/failed/ | wc -l                  # = 0
aws s3 ls s3://handh-trade-raw-use1/ibkr/ohlcv_daily/ --recursive | grep -c json        # = N
```

### Stage C — Raw → Bronze (Databricks serverless)
Databricks → Workflows → **[dev] Raw to Bronze Ingestion (Two-Plane)** → Run now
(defaults: dry_run=false, vendor=ibkr).

**Expected summary (ZERO warnings/errors):**
```
landed              N
failed              0
requests_ingested   N
records_written     ~2600/instrument
records_rejected    0
receipts_archived   N
```

### Stage D — Final validation
```sql
SELECT status, COUNT(*) FROM tradeanalytics.control.fetch_request GROUP BY status;   -- INGESTED = N
SELECT source, COUNT(*) bars, MIN(bar_date) e, MAX(bar_date) l
  FROM tradeanalytics.bronze.market_data_daily GROUP BY source;                      -- ibkr, ~2639/instr
SELECT instrument_id, record_count, latest_date FROM tradeanalytics.control.ingestion_watermark;  -- count > 0
SELECT job_type, records_new, status FROM tradeanalytics.control.job_run_log
  ORDER BY run_started_at DESC LIMIT 20;                                             -- raw_to_bronze, success
```
**Success criteria:** all requests INGESTED · Bronze bar count matches · watermark
`record_count` > 0 (not 0) · one `job_run_log` row per request, `status=success` ·
`failed/` empty · contiguous chunk dates (`end_date+1 = next start_date`, no gaps).

---

## 9. Monitoring & observability

| Signal | Where | Healthy |
|---|---|---|
| Agent liveness | `ibkrbox "journalctl -u fetch-agent-ibkr -n 20"` | hourly `heartbeat — idle` lines |
| Agent service | `ibkrbox "systemctl status fetch-agent-ibkr"` | `active (running)` |
| Work state (primary dashboard) | SQL: `SELECT status, COUNT(*) FROM control.fetch_request GROUP BY status` | no long-lived PENDING/LANDED |
| Stuck work | SQL: `... WHERE status='PENDING' AND requested_at < now() - INTERVAL 2 HOURS` | 0 rows |
| Audit trail | `control.job_run_log` | one row per ingested request |
| Rejected data | `bronze.market_data_rejected` | reviewed if non-empty |
| Job failures | Databricks job email (`handh.stocks@gmail.com`) | none |
| Queue depth | `aws s3 ls .../pending/ \| wc -l` | 0 between runs |

---

## 10. Operational checklist (production run)

- [ ] `AWS_PROFILE=handh-trade` set; `databricks bundle validate` passes
- [ ] Latest code deployed (§5); agent restarted if its file changed
- [ ] Agent `active`; exactly one process; gateway `PORT-OPEN`
- [ ] Vendor IDs seeded for all target instruments (§7)
- [ ] Queue empty (`pending/` = 0) before starting
- [ ] Planner run → summary sane (no unexpected `skipped_unmapped`)
- [ ] Agent drained (`done/` = N, `failed/` = 0)
- [ ] `raw_to_bronze` run → zero warnings
- [ ] Stage-D validation all green
- [ ] `bronze.market_data_daily appendOnly` still `true` (never left off)

---

## 11. Troubleshooting

**11.1 Planner: `skipped_unmapped` non-empty** — instrument lacks a current
ibkr mapping. *Fix:* run §7 seeding, re-run planner. Never bypass with symbol
fetching (silent-wrong-data risk).

**11.2 Agent: `unsupported contract_version` / `unknown task_type` → failed/** —
agent code older than the manifest. *Fix:* redeploy agent (§5 step 2), then
re-emit (delete affected rows + failed manifests, re-run planner).

**11.3 Agent: `gateway is down — waiting`** — IB Gateway not reachable.
*Diagnose:* `ibkrbox "docker logs ibkr-gateway --tail 20"`; VNC `vnc://54.197.158.82:5900`.
*Fix:* `ibkrbox "cd ~/ibkr-gateway && docker compose restart"`; wait ~60s for `Login has completed`. Newly-created paper account "Application In Progress" → wait for IBKR (next business day).

**11.4 Multiple agents racing / stale-code errors** — `pgrep` shows >1 PID.
*Fix:* `ibkrbox "pkill -f 'venv/bin/python fetch_agent.py'"` then `systemctl start`.
Prevented now by flock guard + systemd, but foreground runs without `ssh -t` can still orphan (use `ssh -t` for any manual foreground run).

**11.5 raw_to_bronze: `UNRESOLVED_COLUMN interval`** — stale column ref;
fixed 2026-07-05 (`bar_interval`). If seen, code is behind — redeploy bundle.

**11.6 raw_to_bronze: `job_run_log` insert fails** — schema drift between INSERT
and live table. *Diagnose:* `DESCRIBE TABLE control.job_run_log`; align INSERT to
live columns (owned by `01_create_schemas_and_tables.py`, not `03`). Non-fatal
(ingestion still completes).

**11.7 `DELTA_CANNOT_MODIFY_APPEND_ONLY`** — tried to DELETE Bronze/job_run_log.
Expected — they're append-only. Only for deliberate teardown: `ALTER TABLE …
SET TBLPROPERTIES (delta.appendOnly=false)` → DELETE → **immediately re-set true**.

**11.8 Notebook runs old code** — one of the four caches stale (§5): bundle,
Git folder, warm serverless session, or agent process. Match the cache to the
artifact and refresh.

**11.9 `command not found: ibkrbox`** — alias not in this shell: `source ~/.zshrc`
or use the full `ssh -i … ` command.

---

## 12. Failure recovery (safe by design)

The system is idempotent end-to-end; recovery is almost always "re-run".

| Situation | Recovery |
|---|---|
| Planner crashed mid-write | Re-run planner — orphan-repair marks PENDING-without-manifest rows ORPHANED and re-plans them (self-healing) |
| Agent down during a run | Manifests wait in `pending/`; `systemctl start` → drains backlog |
| Fetch failed (transient) | No action — watermark unchanged, next planner run re-plans the gap |
| Fetch failed (persistent, e.g. delisted) | Triage `error_message` in `failed/`; pause ticker (`UPDATE ticker_feed_config SET is_active=false`) |
| raw_to_bronze crashed mid-run | Re-run — reconcile idempotent, Bronze dedup absorbs partial writes, INGESTED requests skipped |
| Wrong/suspect data | Reprocess from immutable raw payloads (never re-call IBKR) |
| EC2 rebooted | Auto-recovers (~2 min): systemd + docker restart cascade |
| EC2 destroyed | Rebuild ~30 min (runbook §6.8); zero data loss (state in S3/Delta) |

**Re-running a completed instrument** is safe: planner derives INCREMENTAL from
the watermark (not a re-backfill); overlapping bars dedup to zero new rows.

---

## 13. Rollback & cleanup (test/teardown only — NEVER production Bronze)

Full clean-room teardown for one symbol (used 2026-07-05):
```sql
-- control tables (not append-only): direct DELETE
DELETE FROM tradeanalytics.control.fetch_request       WHERE symbol='SPY';
DELETE FROM tradeanalytics.control.ingestion_watermark WHERE instrument_id=(
  SELECT instrument_id FROM tradeanalytics.reference.instrument_listing
  WHERE symbol='SPY' AND is_current=true);
-- append-only tables: toggle off, delete, RE-LOCK immediately
ALTER TABLE tradeanalytics.bronze.market_data_daily SET TBLPROPERTIES (delta.appendOnly=false);
DELETE FROM tradeanalytics.bronze.market_data_daily WHERE symbol='SPY';
ALTER TABLE tradeanalytics.bronze.market_data_daily SET TBLPROPERTIES (delta.appendOnly=true);
-- (repeat toggle pattern for market_data_rejected, job_run_log)
```
```bash
# S3 raw + queue
aws s3 rm s3://handh-trade-raw-use1/ibkr/ohlcv_daily/       --recursive --quiet
aws s3 rm s3://handh-trade-raw-use1/control/fetch/ibkr/     --recursive --quiet
```
**Always verify `appendOnly=true` is restored afterward:**
`SHOW TBLPROPERTIES tradeanalytics.bronze.market_data_daily (delta.appendOnly);`

---

## 14. Best practices, notes & known limitations

**Best practices**
- Control the WORK (SQL on control tables), not the PROCESS, for day-to-day ops.
- Never delete Bronze in production — reprocess from raw.
- Never bypass `require_vendor_id`; seed conIds first.
- After any code change, deploy the right cache layer(s) before running (§5).
- Run planner with `dry_run=true` first when changing scope/config.

**Operational notes**
- Schedules are OFF (manual runs) until intentionally enabled. Target rhythm:
  planner 7pm ET, raw_to_bronze 8pm ET, Mon–Fri; agent is always-on (no schedule).
- Backfill chunk = 365d, incremental = 30d (config `daily.yml`). IBKR pacing is
  per-request, so fewer/larger chunks = faster backfill.
- All audit timestamps are UTC; bar dates are US-exchange calendar dates. Run the
  planner with explicit `as_of_date` near the NZ/US date boundary.

**Known limitations**
- EC2 hardware failure is not self-healing (manual ~30-min rebuild).
- Single-worker per vendor queue (IBKR pacing is the ceiling; parallelism wouldn't help).
- Reference-data defect: instrument_id 45 has multiple listings (BF.B/BRK.B) —
  contained, not in active universe; surgery pending (CLAUDE.md §10).
- `bronze_daily_ingestion` job cannot run in cloud (no egress) — local dev tool only.

---

## 15. Managing instruments (operational reference)

> ⚠️ **Schema-drift warning.** Two DDL notebooks exist
> (`01_create_reference_tables.py` and `01_create_schemas_and_tables.py`) with
> slightly different column sets for some tables (a known issue — same class as
> the job_run_log drift). **Before running any INSERT/UPDATE below, confirm the
> live columns** with `DESCRIBE TABLE tradeanalytics.reference.<table>` and adjust.
> The examples use the columns verified present on 2026-07-05.

### 15.1 The control & reference tables — purpose and how they link

**Mental model — three questions:**
- **Reference tables = WHAT exists and WHAT we want** (instruments, listings, desired feed config)
- **Control tables = WHAT has happened and WHAT to do next** (watermarks, work queue, audit, commands)
- The planner DERIVES work by comparing *desired* (ticker_feed_config) vs *actual* (ingestion_watermark).

| Table | Plane | Purpose | Key | Links to |
|---|---|---|---|---|
| `reference.instrument` | ref | Permanent master record, one per financial instrument | `instrument_id` (BIGINT identity) | root of everything |
| `reference.instrument_listing` | ref | Symbol / exchange / currency (SCD-2, `is_current`) | `listing_id`; FK `instrument_id` | → instrument |
| `reference.instrument_vendor_id` | ref | Vendor IDs (IBKR conId etc.), SCD-2, append-only | `vendor_mapping_id`; FK `instrument_id` | → instrument |
| `reference.universe_membership` | ref | Which instruments belong to which universe (SP500…) | `membership_id`; FK `instrument_id` | → instrument |
| `reference.ticker_feed_config` | ref | **DESIRED STATE**: target dates, is_active, batch_group, priority | `config_id`; FK `instrument_id` | → instrument; read by planner |
| `control.ingestion_watermark` | ctl | **ACTUAL STATE**: earliest/latest date fetched, record_count | `instrument_id + stream` | ← written by raw_to_bronze |
| `control.fetch_request` | ctl | Work queue: one row per fetch chunk, PENDING→INGESTED | `request_id`; `request_key` | ← planner, → raw_to_bronze |
| `control.job_run_log` | ctl | Append-only audit: one row per ingested chunk | `log_id` | ← raw_to_bronze |
| `control.ingestion_command` | ctl | Operator one-offs (FORCE_RELOAD, PAUSE…) consumed once | `command_id` | read by planner |

**The join key everywhere is `instrument_id`.** Symbol is a display attribute in
`instrument_listing` only — never join on it.

```
instrument (id=505, SPY)
  ├── instrument_listing   (symbol=SPY, exchange_mic=ARCX, is_current=true)
  ├── instrument_vendor_id (vendor=ibkr, vendor_instrument_id=756733, is_current=true)
  ├── universe_membership  (universe_code=SP500)
  └── ticker_feed_config   (is_active=true, target_start_date=2016-01-01) ── desired
                                                                              ▲
control.ingestion_watermark (instrument_id=505, latest_date=2026-07-02) ── actual
        planner compares desired vs actual → control.fetch_request (work)
```

### 15.1a Readable views (use these for queries)

Base tables key on `instrument_id` (no stored `symbol` — symbols change, e.g.
FB→META, and live only in `instrument_listing`, SCD-2). For readable queries,
use the `v_*` views (created by `notebooks/reference/07_create_readable_views.py`)
— they join the CURRENT symbol/company/conId live, so they can never go stale:

| View | Over | Adds |
|---|---|---|
| `reference.v_ticker_feed_config` | ticker_feed_config | symbol, company_name, asset_class, ibkr_conid |
| `control.v_ingestion_watermark` | ingestion_watermark | symbol, company_name |
| `control.v_fetch_request` | fetch_request | symbol, company_name |
| `control.v_job_run_log` | job_run_log | symbol |

**Read via the views; WRITE to the base tables.** Never store symbol in these
tables — that would denormalize a value that changes, creating update anomalies.

### 15.2 Which instruments are active — and how to check

"Active for fetching" = a row in `ticker_feed_config` with `is_active = true`.
(An instrument can exist and be mapped but NOT be fetched if inactive.)

```sql
-- Currently ACTIVE instruments (what the planner will fetch) — via the readable view
SELECT symbol, company_name, asset_class, batch_group, priority, target_start_date
FROM tradeanalytics.reference.v_ticker_feed_config
WHERE is_active = true
ORDER BY symbol;
```
```sql
-- Full picture: config vs mapping vs actual watermark (one row per instrument)
SELECT l.symbol, fc.is_active, fc.target_start_date,
       v.vendor_instrument_id AS ibkr_conid,
       w.latest_date, w.record_count
FROM tradeanalytics.reference.instrument_listing l
LEFT JOIN tradeanalytics.reference.ticker_feed_config fc
       ON fc.instrument_id = l.instrument_id
LEFT JOIN tradeanalytics.reference.instrument_vendor_id v
       ON v.instrument_id = l.instrument_id AND v.vendor='ibkr' AND v.is_current=true
LEFT JOIN tradeanalytics.control.ingestion_watermark w
       ON w.instrument_id = l.instrument_id AND w.stream='daily'
WHERE l.is_current = true
ORDER BY l.symbol;
```

### 15.3 Activate / deactivate an instrument (single SQL)

```sql
-- PAUSE fetching (planner stops emitting for it; existing Bronze untouched)
UPDATE tradeanalytics.reference.ticker_feed_config
SET is_active = false, updated_at = current_timestamp()
WHERE instrument_id = (SELECT instrument_id FROM tradeanalytics.reference.instrument_listing
                       WHERE symbol = 'TSLA' AND is_current = true);

-- RESUME fetching
UPDATE tradeanalytics.reference.ticker_feed_config
SET is_active = true, updated_at = current_timestamp()
WHERE instrument_id = (SELECT instrument_id FROM tradeanalytics.reference.instrument_listing
                       WHERE symbol = 'TSLA' AND is_current = true);
```
**Propagation:** takes effect on the **next planner run** — no code change, no
restart. Deactivate does NOT delete anything; it just stops future fetches.

### 15.4 Add a NEW instrument (all tables, in order)

Four reference tables must get rows, then vendor-ID seeding, then it's fetchable.
`instrument_id` is auto-generated — capture it after the first insert.

```sql
-- STEP 1: master record (asset_class required). instrument_id auto-assigned.
INSERT INTO tradeanalytics.reference.instrument (isin, figi, asset_class, is_active, created_at, updated_at)
VALUES (NULL, NULL, 'equity', true, current_timestamp(), current_timestamp());

-- capture the new id (most recent for this asset_class, or look it up by a known field)
-- e.g.: SELECT MAX(instrument_id) AS new_id FROM tradeanalytics.reference.instrument;
-- Assume it returned 777 for the examples below.

-- STEP 2: listing (symbol/exchange — SCD-2, is_current=true)
INSERT INTO tradeanalytics.reference.instrument_listing
  (instrument_id, symbol, company_name, exchange, exchange_mic, currency,
   valid_from, is_current, created_at)
VALUES (777, 'AMD', 'Advanced Micro Devices', 'SMART', 'XNAS', 'USD',
        current_date(), true, current_timestamp());

-- STEP 3: universe membership (optional but recommended for grouping)
INSERT INTO tradeanalytics.reference.universe_membership
  (instrument_id, universe_code, source_etf, valid_from, is_current, created_at, updated_at)
VALUES (777, 'SP500', 'MANUAL', current_date(), true, current_timestamp(), current_timestamp());

-- STEP 4: feed config = DESIRED STATE (this is what makes it fetchable)
INSERT INTO tradeanalytics.reference.ticker_feed_config
  (instrument_id, stream, target_start_date, run_frequency, batch_group,
   priority, is_active, max_lookback_days, created_at, updated_at, created_by)
VALUES (777, 'daily', DATE'2016-01-01', 'daily', 'A',
        5, true, 3650, current_timestamp(), current_timestamp(), 'ops-manual');
```
```
-- STEP 5: seed the IBKR conId (agent must be running)
--   Databricks → notebooks/reference/06_seed_vendor_ids.py → Run all
--   (differential: qualifies only the new unmapped AMD; loads its conId)
```
```sql
-- STEP 6: verify readiness (all four should be populated)
SELECT
  (SELECT COUNT(*) FROM tradeanalytics.reference.instrument_listing   WHERE instrument_id=777 AND is_current=true) listing,
  (SELECT COUNT(*) FROM tradeanalytics.reference.ticker_feed_config   WHERE instrument_id=777 AND is_active=true)  feed,
  (SELECT COUNT(*) FROM tradeanalytics.reference.instrument_vendor_id WHERE instrument_id=777 AND vendor='ibkr' AND is_current=true) conid;
-- expect 1,1,1 → next planner run will INITIAL_LOAD it
```
**Then:** run the planner (blank symbols = all active, or `symbols=AMD`). It sees
no watermark → derives INITIAL_LOAD from `target_start_date` → agent fetches →
raw_to_bronze ingests. **Do not skip STEP 5** — the `require_vendor_id` policy
blocks any instrument without a current conId (never falls back to symbol).

### 15.5 Change history depth (e.g. 10 → 15 years)

History depth is **desired state** in `ticker_feed_config.target_start_date`.
Move it earlier and the planner derives a HISTORY_EXTENSION for the uncovered
older range on the next run.

```sql
-- Extend ALL active instruments back to 2011 (15y from 2026)
UPDATE tradeanalytics.reference.ticker_feed_config
SET target_start_date = DATE'2011-01-01', updated_at = current_timestamp()
WHERE is_active = true AND target_start_date > DATE'2011-01-01';

-- Or one instrument:
UPDATE tradeanalytics.reference.ticker_feed_config
SET target_start_date = DATE'2011-01-01', updated_at = current_timestamp()
WHERE instrument_id = (SELECT instrument_id FROM tradeanalytics.reference.instrument_listing
                       WHERE symbol='SPY' AND is_current=true);
```
**What happens next run:**
1. Planner reads watermark → `earliest_date = 2016-01-01`; desired start now 2011-01-01.
2. `earliest_date > target_start` → derives **HISTORY_EXTENSION** for 2011-01-01 → 2015-12-31.
3. Chunked at 365d (backfill size) → ~5 new fetch_request chunks per instrument.
4. Agent fetches; raw_to_bronze appends; watermark `earliest_date` moves back to 2011.

**Dependencies / impacts:**
- The instrument must already have a conId (existing instruments do).
- IBKR must actually *have* data that far back for the symbol (older or recently
  listed names may return fewer years — the gap simply won't fill, no error).
- Bronze is append-only; extension only ADDS older rows, never rewrites existing.
- `max_lookback_days` is a guardrail cap — if set (e.g. 3650 = 10y), it can
  clamp the derived start. To truly allow 15y, ensure `max_lookback_days` ≥ 5475
  (or NULL) as well:
  ```sql
  UPDATE tradeanalytics.reference.ticker_feed_config
  SET max_lookback_days = 5475 WHERE is_active = true;
  ```

### 15.6 Common operational scenarios (copy-paste SQL)

```sql
-- Force a full reload of one instrument (operator one-off; planner consumes it)
INSERT INTO tradeanalytics.control.ingestion_command
  (instrument_id, action, requested_at, status)
VALUES (505, 'FORCE_RELOAD', current_timestamp(), 'PENDING');
--   ⚠ verify column names first: DESCRIBE TABLE control.ingestion_command

-- Move an instrument to a different batch group (staging large backfills)
UPDATE tradeanalytics.reference.ticker_feed_config
SET batch_group = 'B', updated_at = current_timestamp()
WHERE instrument_id = 777;

-- Change fetch frequency
UPDATE tradeanalytics.reference.ticker_feed_config
SET run_frequency = 'weekly', updated_at = current_timestamp()
WHERE instrument_id = 777;

-- Rename a symbol correctly (SCD-2: close old, open new — conId unchanged!)
UPDATE tradeanalytics.reference.instrument_listing
SET is_current = false, valid_to = current_date(), change_reason = 'RENAME'
WHERE symbol = 'FB' AND is_current = true;
INSERT INTO tradeanalytics.reference.instrument_listing
  (instrument_id, symbol, company_name, exchange, exchange_mic, currency,
   valid_from, is_current, created_at)
VALUES (<same_instrument_id>, 'META', 'Meta Platforms', 'SMART', 'XNAS', 'USD',
        current_date(), true, current_timestamp());
--   Fetching is UNAFFECTED by renames — the agent fetches by conId, not symbol.
```

### 15.7 Instrument-config change → propagation flow

```
Operator SQL on reference tables (desired state)
        │  (no deploy, no restart — just data)
        ▼
Next fetch_planner run  ── compares desired vs actual watermark
        │   is_active=false → instrument skipped
        │   new instrument (no watermark) → INITIAL_LOAD
        │   target_start_date earlier → HISTORY_EXTENSION
        │   watermark behind today → INCREMENTAL
        ▼
control.fetch_request rows (PENDING) + S3 manifests
        ▼
EC2 agent fetches by conId → raw payloads
        ▼
raw_to_bronze → bronze.market_data_daily + watermark advances + job_run_log
```
**Golden rule:** you change **desired state** (reference tables) via single SQL;
the planner turns that into work automatically on its next run. You never touch
`control.fetch_request` or the watermark by hand — those are system-managed.

### 15.8 Instrument-management troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| New instrument never fetched | Missing feed_config row OR is_active=false | §15.4 STEP 4; check §15.2 query |
| New instrument in `skipped_unmapped` | conId not seeded | Run §7 seeding (STEP 5) |
| History extension did nothing | `max_lookback_days` clamping, or IBKR has no older data | Raise max_lookback_days (§15.5); check IBKR history availability |
| Deactivated instrument still fetched | Planner ran before the UPDATE committed | Re-run planner after confirming `is_active=false` |
| Two current listings for one instrument | universe_sync SCD-2 defect (see id 45) | Close the wrong listing (SCD-2 UPDATE); do NOT delete |
| INSERT fails on unknown column | DDL drift between the two 01_*.py notebooks | `DESCRIBE TABLE` the live table; use its actual columns |

### 15.9 Bulk-activate EXISTING instruments (INSERT … SELECT)

**Key insight:** the reference tables already hold ~500 instruments
(`instrument`, `instrument_listing`, `instrument_vendor_id` all populated), but
only a handful are in `ticker_feed_config`. An instrument that already exists and
is conId-mapped becomes fetchable by adding **only** a feed_config row — no need
to re-insert instrument/listing/vendor rows (§15.4 steps 1–3, 5 are for genuinely
NEW instruments not yet in the tables). This can be one bulk `INSERT … SELECT`
that resolves `instrument_id` automatically by joining on symbol.

**Always PREVIEW first** (count + which symbols would activate):
```sql
SELECT COUNT(*) AS would_activate,
       array_join(sort_array(collect_list(l.symbol)), ', ') AS symbols
FROM tradeanalytics.reference.instrument_listing l
JOIN tradeanalytics.reference.instrument_vendor_id v
     ON v.instrument_id = l.instrument_id AND v.vendor='ibkr' AND v.is_current=true
WHERE l.is_current = true
  AND l.symbol IN ('AAPL','MSFT','SPY','QQQ',  /* … your list … */ )
  AND NOT EXISTS (SELECT 1 FROM tradeanalytics.reference.ticker_feed_config fc
                  WHERE fc.instrument_id = l.instrument_id AND fc.stream='daily')
  AND l.instrument_id NOT IN (        -- exclude multi-listing pollution (e.g. id 45)
      SELECT instrument_id FROM tradeanalytics.reference.instrument_listing
      WHERE is_current=true GROUP BY instrument_id HAVING COUNT(*)>1);
```
A symbol MISSING from the preview means it either isn't in `instrument_listing`
(truly new → full §15.4) or has no conId (→ §7 seeding).

**The bulk INSERT** (idempotent, safe — three guards built in):
```sql
INSERT INTO tradeanalytics.reference.ticker_feed_config
  (instrument_id, stream, target_start_date, run_frequency, batch_group,
   priority, is_active, max_lookback_days, created_at, updated_at, created_by)
SELECT l.instrument_id, 'daily', DATE'2016-01-01', 'daily',
       'B',                       -- test batch_group, separate from prod 'A'
       5, true, 3650,
       current_timestamp(), current_timestamp(), 'ops-bulk-testing'
FROM tradeanalytics.reference.instrument_listing l
JOIN tradeanalytics.reference.instrument_vendor_id v        -- guard 1: must have conId
     ON v.instrument_id = l.instrument_id AND v.vendor='ibkr' AND v.is_current=true
WHERE l.is_current = true
  AND l.symbol IN ('AAPL','MSFT','SPY','QQQ',  /* … your list … */ )
  AND NOT EXISTS (                                          -- guard 2: no duplicates
      SELECT 1 FROM tradeanalytics.reference.ticker_feed_config fc
      WHERE fc.instrument_id = l.instrument_id AND fc.stream='daily')
  AND l.instrument_id NOT IN (                              -- guard 3: skip pollution
      SELECT instrument_id FROM tradeanalytics.reference.instrument_listing
      WHERE is_current=true GROUP BY instrument_id HAVING COUNT(*)>1);
```
Guards: (1) conId JOIN → nothing lands in `skipped_unmapped`; (2) `NOT EXISTS`
→ re-runnable, never duplicates; (3) multi-listing exclusion → the id-45 defect
can't sneak in. **After INSERT:** run planner (blank symbols) → all become
INITIAL_LOAD. Using `batch_group='B'` lets you later pause/stage the whole test
set with one `WHERE batch_group='B'`.

**Bulk deactivate the test set when done:**
```sql
UPDATE tradeanalytics.reference.ticker_feed_config
SET is_active = false, updated_at = current_timestamp()
WHERE batch_group = 'B';
```

### 15.10 Handy operational queries (health, coverage, discovery)

```sql
-- A. Coverage dashboard: mapped vs active vs actually-ingested, per instrument
SELECT l.symbol,
       (v.vendor_instrument_id IS NOT NULL)                         AS has_conid,
       COALESCE(fc.is_active, false)                                AS is_active,
       w.earliest_date, w.latest_date, w.record_count,
       datediff(current_date(), w.latest_date)                      AS days_stale
FROM tradeanalytics.reference.instrument_listing l
LEFT JOIN tradeanalytics.reference.instrument_vendor_id v
       ON v.instrument_id=l.instrument_id AND v.vendor='ibkr' AND v.is_current=true
LEFT JOIN tradeanalytics.reference.ticker_feed_config fc
       ON fc.instrument_id=l.instrument_id AND fc.stream='daily'
LEFT JOIN tradeanalytics.control.ingestion_watermark w
       ON w.instrument_id=l.instrument_id AND w.stream='daily'
WHERE l.is_current=true
ORDER BY is_active DESC, days_stale DESC NULLS LAST;

-- B. Active instruments with NO data yet (planned but never successfully ingested)
SELECT l.symbol, fc.target_start_date
FROM tradeanalytics.reference.ticker_feed_config fc
JOIN tradeanalytics.reference.instrument_listing l
     ON l.instrument_id=fc.instrument_id AND l.is_current=true
LEFT JOIN tradeanalytics.control.ingestion_watermark w
     ON w.instrument_id=fc.instrument_id AND w.stream='daily'
WHERE fc.is_active=true AND w.instrument_id IS NULL;

-- C. Active instruments MISSING a conId (would be blocked by require_vendor_id)
SELECT l.symbol
FROM tradeanalytics.reference.ticker_feed_config fc
JOIN tradeanalytics.reference.instrument_listing l
     ON l.instrument_id=fc.instrument_id AND l.is_current=true
LEFT JOIN tradeanalytics.reference.instrument_vendor_id v
     ON v.instrument_id=fc.instrument_id AND v.vendor='ibkr' AND v.is_current=true
WHERE fc.is_active=true AND v.vendor_instrument_id IS NULL;

-- D. Stale active instruments (ingested, but latest_date behind — needs a run)
SELECT l.symbol, w.latest_date, datediff(current_date(), w.latest_date) AS days_behind
FROM tradeanalytics.control.ingestion_watermark w
JOIN tradeanalytics.reference.instrument_listing l
     ON l.instrument_id=w.instrument_id AND l.is_current=true
JOIN tradeanalytics.reference.ticker_feed_config fc
     ON fc.instrument_id=w.instrument_id AND fc.is_active=true
WHERE datediff(current_date(), w.latest_date) > 3
ORDER BY days_behind DESC;

-- E0. Stage timings for a run (plan → fetch → ingest)
--   planner duration (durable, from planner_run_log):
SELECT batch_id, duration_seconds AS planner_secs, requests_emitted, run_started_at
FROM tradeanalytics.control.planner_run_log ORDER BY run_started_at DESC LIMIT 5;
--   agent drain (first→last landed; populated after raw_to_bronze reconciles):
SELECT timestampdiff(MINUTE, MIN(landed_at), MAX(landed_at)) AS agent_minutes
FROM tradeanalytics.control.fetch_request WHERE status IN ('LANDED','INGESTED');
--   ingest duration (from job_run_log):
SELECT MIN(run_started_at) AS ingest_start, MAX(run_completed_at) AS ingest_end,
       SUM(duration_seconds) AS ingest_secs
FROM tradeanalytics.control.job_run_log WHERE job_type='raw_to_bronze';

-- E. Bronze bar counts per active instrument (data volume overview)
SELECT b.symbol, COUNT(*) AS bars, MIN(b.bar_date) AS earliest, MAX(b.bar_date) AS latest
FROM tradeanalytics.bronze.market_data_daily b
GROUP BY b.symbol ORDER BY bars DESC;

-- F. Last run outcome per instrument (from the audit log)
SELECT l.symbol, j.job_type, j.records_new, j.status, j.run_started_at
FROM tradeanalytics.control.job_run_log j
JOIN tradeanalytics.reference.instrument_listing l
     ON l.instrument_id=j.instrument_id AND l.is_current=true
QUALIFY ROW_NUMBER() OVER (PARTITION BY j.instrument_id ORDER BY j.run_started_at DESC)=1
ORDER BY j.run_started_at DESC;

-- G. Reference-data integrity check (should return ZERO rows; >0 = pollution)
SELECT instrument_id, COUNT(*) AS current_listings
FROM tradeanalytics.reference.instrument_listing WHERE is_current=true
GROUP BY instrument_id HAVING COUNT(*)>1;

-- H. Batch-group roster (what runs together)
SELECT batch_group, COUNT(*) AS instruments,
       SUM(CASE WHEN is_active THEN 1 ELSE 0 END) AS active
FROM tradeanalytics.reference.ticker_feed_config GROUP BY batch_group ORDER BY batch_group;
```

**When to reach for which:**
- Onboarding a test set → **§15.9 preview + INSERT**, then **query B** (confirm they're queued), then **query C** (catch any unmapped before running the planner).
- Daily "is everything current?" → **query D** (stale) + **query F** (last outcome).
- After a big backfill → **query E** (volumes) + **query A** (full coverage).
- Suspect reference pollution → **query G** (must be empty).
</content>
