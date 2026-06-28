# CLAUDE.md — TradeAnalytics Session Context

> **HOW TO USE:** At the start of every new Claude session, upload this file.
> Claude will read it and pick up exactly where we left off — code patterns,
> decisions, current state, and known issues all included.
> Keep this file updated as the project evolves.

---

## 1. Project Identity

| Item | Value |
|------|-------|
| Project | TradeAnalytics — Signal-as-a-Service ML trading platform |
| GitHub | `hemachandra-menakuru/tradeanalytics` (private) |
| Claude Project | `handh_tradeanalytics` |
| AWS Account | `311925399625`, region `us-east-1` |
| Databricks | `dbc-bf0075e6-07aa.cloud.databricks.com`, workspace `handh-dev` |
| Unity Catalog | `tradeanalytics` |
| Bronze schema | `tradeanalytics.bronze` |
| DABs profile | `handh-trade-aws` |
| IBKR account | `U5498892`, gateway at `~/dev/tools/ibkr/clientportal.gw`, port `5055` |
| Conda env | `tradeanalytics` (Python 3.11, `databricks-connect==15.4.25`) |
| Repo path (local) | `/Users/hemachandra/projects/tradeanalytics` (shell alias: `~/pr/tradeanalytics`) |
| Active branch | `main` (Phase 2.5 merged 2026-06-28 via PR #7) |
| Git pager | Disabled — `git config --global core.pager cat` (already set) |

---

## 2. Architecture Principles (non-negotiable)

### Universal component pattern
Every component follows: `ABC → Registry → Factory → Config`
Adding a new component = implement ABC + register (one line) + update YAML. Zero other changes.

### Eight architectural principles
1. **Loose coupling** — ABCs only across boundaries, never concrete implementations
2. **Single responsibility** — each class does one thing, one config file per concern
3. **Plug-and-play** — new providers/models/strategies/brokers via registry pattern, not code changes
4. **Config-driven** — behaviour controlled by YAML, not if/else in code
5. **Abstraction layers** — ABC for every external dependency
6. **No vendor lock-in** — IBKR, Claude, Databricks all behind ABCs, swappable
7. **Horizontal scale** — stateless components, Spark-native, Delta for state
8. **Fail-safe** — health checks, fallbacks, circuit breakers at every external boundary

### Bronze layer rules (absolute)
- **Append-only** — NEVER UPDATE / DELETE / MERGE on bronze tables
- Amendments are new records with `record_version` incremented
- Three-layer deduplication:
  - Layer 1: `IngestionPlanner` — correct date range (skip if up to date)
  - Layer 2: `BronzeWriter._bulk_classify()` — bulk pre-fetch + in-memory classify
  - Layer 3: Silver `ROW_NUMBER` window — exactly one record per key downstream
- Rejected records go to `bronze.market_data_rejected` — queryable, reprocessable
- **Records flow as plain dicts throughout the pipeline** — `BronzeRecord` is NOT instantiated
  during ingestion. All audit fields stamped in `validator._enrich_record()`

### Data source (locked)
- IBKR Client Portal REST API = **primary for all Phases 1-4** (runs on `localhost:5055`)
- Yahoo Finance = **unit test fallback only**, never production
- IBKR gateway: start with `bin/run.sh root/conf.yaml` in `~/dev/tools/ibkr/clientportal.gw`
- To authenticate: `curl -sk -X POST https://localhost:5055/v1/api/iserver/reauthenticate`
- Phase 5 (live trading): IB Gateway on EC2 + `ib_insync`

### LLM placement (validated, non-negotiable)
- LLM is **never in the synchronous execution path**
- UPSTREAM (async, batch): Catalyst Sentiment Score → feature matrix input
- DOWNSTREAM (async, post-trade): reasoning briefs, attribution, daily reports

### ML sequencing (locked)
- XGBoost/LightGBM first for ALL cluster types
- TFT/LSTM only after XGBoost proven insufficient (out-of-sample evidence required)
- GARCH for volatility-based position sizing only — not entry signals

### Signal platform design
- Signals are portfolio-agnostic (Type 1 quality filters in `SignalQualityEngine`)
- `SignalLog` captures EVERY signal regardless of outcome (prevents survivorship bias)
- Build sequence locked: signal engine → validation → distribution → execution bot → monetisation

---

## 3. Phase Status

| Phase | Name | Status |
|-------|------|--------|
| 1 | Infrastructure | ✅ Complete |
| 2 | Bronze Ingestion | ✅ Merged to main — PR #6, 2026-06-25. IBKR smoke test passed, source=ibkr confirmed |
| 2.5 | Pre-Phase 3 Restructure | ✅ Complete — merged to main via PR #7, 2026-06-28. Reference/control tables built and seeded. DeltaWatermarkStore, DeltaUniverseReader, table-driven IngestionPlanner all wired. |
| 3A | Corporate Actions + Vendor-Agnostic Schema | ✅ Complete — DDL notebook run on cluster 2026-06-28. ibkr_con_id migrated to instrument_vendor_id and dropped. Corporate actions tables created. Python classes written and tested (93 tests). |
| 3 (pre) | Bronze pre-Silver fixes | ✅ Complete 2026-06-28 — instrument_id stamped on bronze records, ingested_by fixed (was NULL), vendor wired into watermark, schema_migrations notebook run. 8-instrument Yahoo ingestion verified on feature/phase3-silver. |
| 3 | Silver (Feature Engineering) | 🔜 Next — step-by-step teaching approach |
| 4 | Gold + Signal Platform | Not started |
| 4b | Signal sharing (Telegram/API) | Not started |
| 5a | HC's execution bot | Not started |
| 5b | Friends' execution bots | Not started |
| 6 | Monetisation | Not started (legal review required first) |

### Phase 3 teaching approach (locked)
- **Step by step — explain every concept before writing code**
- No assumptions about prior ML knowledge
- Explain what, why, and what the output means on real data
- HC says if something isn't clear — go back and re-explain
- Never rush ahead to code without concept explanation first

---

## 4. File Structure

Restructured on `feature/phase3-restructure` (2026-06-25). Old `src/ingestion/` and `src/config/` folders removed.

```
tradeanalytics/
├── config/
│   ├── dev.yml                  # infrastructure (S3, Databricks, cluster)
│   ├── sources.yml              # provider settings (IBKR base_url=https://localhost:5055/v1/api)
│   ├── risk.yml
│   ├── logging.yml
│   ├── quality/
│   │   └── data_quality_rules.yml  # moved here from src/reference/seed/ (canonical location)
│   └── streams/
│       ├── daily.yml            # Phase 2, active
│       ├── intraday.yml         # Phase 3, disabled
│       └── tick.yml             # Phase 5, disabled
├── src/
│   ├── shared/
│   │   ├── config/config_loader.py         # import: src.shared.config.config_loader
│   │   ├── base/
│   │   │   ├── data_provider.py            # HistoricalDataProvider, RealtimeProvider, OptionsProvider ABCs (AR-1 ✅)
│   │   │   ├── watermark_store.py          # WatermarkStore ABC (AR-4 ✅)
│   │   │   ├── universe_reader.py          # UniverseReader + InstrumentInfo (AR-3 ✅)
│   │   │   └── trading_calendar.py         # TradingCalendar ABC (AR-2 ✅)
│   │   ├── calendar/
│   │   │   └── us_equity_calendar.py       # US holiday calendar (AR-2 ✅)
│   │   └── models/
│   │       └── ohlcv_record.py             # OHLCVRecord TypedDict — enforced provider contract (AR-8 ✅)
│   ├── bronze/
│   │   ├── base/market_data_provider.py    # MarketDataProvider — composite ABC (inherits all 3 shared ABCs)
│   │   ├── factory/provider_factory.py     # MarketDataFactory — registry base: HistoricalDataProvider
│   │   ├── jobs/bronze_ingestion_job.py    # BronzeIngestionJob + JobRunSummary
│   │   ├── models/
│   │   │   ├── bronze_record.py            # BronzeRecord (59 fields, 8 groups)
│   │   │   ├── ingestion_mode.py           # IngestionMode, FetchPlan, IngestionWatermark
│   │   │   └── ingestion_planner.py        # IngestionPlanner (8 modes)
│   │   ├── providers/
│   │   │   ├── ibkr_provider.py            # IBKRProvider (primary, live-verified; conid cache persisted)
│   │   │   └── yahoo_provider.py           # YahooProvider (unit test fallback only)
│   │   ├── validation/
│   │   │   ├── models.py
│   │   │   ├── rule_engine.py              # RuleEngine (18 rules from YAML)
│   │   │   └── validator.py               # DataQualityValidator (provider_nullable_fields wired)
│   │   └── writers/
│   │       ├── bronze_writer.py            # BronzeWriter (Spark JOIN dedup, no stream_cfg param)
│   │       └── watermark_manager.py        # WatermarkManager — to move to src/control/ in Phase 2.5
│   ├── reference/
│   │   ├── managers/ticker_reader.py       # reads from seed CSV (replace with Delta in Phase 2.5)
│   │   ├── seed/
│   │   │   ├── tickers.csv                 # bootstrap seed only — NOT source of truth in production
│   │   │   ├── strategies.csv
│   │   │   └── data_quality_rules.yml      # kept for backward compat — canonical copy in config/quality/
│   │   └── sources/                        # reference data clients (NASDAQ FTP, OpenFIGI, etc.)
│   ├── control/                            # Phase 2.5 — watermark manager moves here
│   ├── silver/                             # Phase 3 placeholders
│   ├── gold/                               # Phase 4 placeholders
│   ├── api/                                # Phase 4b placeholders
│   ├── execution/                          # Phase 5 placeholders
│   └── llm/                               # Phase 4 placeholders
├── notebooks/
│   └── bronze/bronze_daily_ingestion.py   # Databricks entry point (path updated in databricks.yml)
├── databricks.yml                          # DABs bundle (job ID: 174217366433843)
├── pyproject.toml                          # pytest config — markers: unit/integration/smoke
├── requirements.txt                        # Phase 1-3 deps only
├── requirements-phase4.txt                 # API/signal publishing deps (fastapi, flask, etc.)
├── requirements-phase5.txt                 # Execution deps (alembic, SQLAlchemy, docker)
├── TradeAnalytics_Phase2_Data_Ingestion_Guide.docx
└── tests/
    ├── conftest.py                         # auto-applies @unit marker to unmarked tests
    ├── bronze/
    ├── reference/managers/
    └── shared/config/
```

**Import paths after restructure:**
- Config: `from src.shared.config.config_loader import ConfigLoader`
- Bronze: `from src.bronze.writers.bronze_writer import BronzeWriter`
- Reference: `from src.reference.managers.ticker_reader import TickerReader`

---

## 5. Key Component Patterns

### BronzeWriter — critical Spark patterns
```python
# write_batch() signature (changed 2026-06-26):
#   main_table: str, rejected_table: str  ← explicit strings, not stream_cfg object
#   (storage layer must not know about config structure — AR-5 ✅)
writer.write_batch(
    symbol=symbol, interval=interval, batch_id=batch_id,
    clean_records=..., rejected_records=...,
    main_table=self._stream_cfg.table,
    rejected_table=self._stream_cfg.rejected_table,
)

# _spark_bulk_fetch() — Spark DataFrame JOIN, not f-string IN-clause
# SQL injection fix: build keys_df via createDataFrame(), join with table_df
# _spark_append() — reads Delta schema FIRST, then aligns types
# Type alignment: DateType→datetime.date, Double→float, Long/Int→int, Bool→bool
# Prevents: CANNOT_DETERMINE_TYPE + FIELD_DATA_TYPE_UNACCEPTABLE
# get_record_count() — real count for watermark (not 0)
```

### WatermarkManager — Spark pattern
```python
# _spark_upsert_watermark() — explicit StructType with DateType
# Converts earliest_date/latest_date strings → datetime.date before createDataFrame()
```

### DataQualityValidator — audit field stamping + nullable fields
```python
# Create with provider_nullable_fields wired (2026-06-26):
validator = DataQualityValidator.for_stream(config, "daily")
# Reads daily.yml → validation.provider_nullable_fields
# Those fields excluded from data_completeness_pct denominator

# _enrich_record() stamps ALL audit fields:
# batch_id, pipeline_version, record_version(=1), ingested_at(UTC), ingestion_type,
# is_amended(False), amendment_reason(None), supersedes_batch(None),
# data_completeness_pct(computed), session_open(None), session_close(None),
# ingested_by, fetch_attempt_count
# data_completeness_pct=60-64 for IBKR (correct — IBKR supplies ~60-64% of optional fields)

# Rules file canonical location: config/quality/data_quality_rules.yml
```

### BronzeIngestionJob — key patterns
```python
# stream_interval defined BEFORE ticker loop (prevents NameError in error handler)
# run() accepts start_date/end_date override params
# Passes plan.ingestion_type to validator.validate_batch()
# _update_watermark_after_write() extracted as helper (2026-06-26)

# pipeline_version (2026-06-26):
# Reads PIPELINE_VERSION env var first (set by DABs via databricks.yml pipeline_version var)
# Falls back to git SHA for local development
# Deploy with: databricks bundle deploy --var "pipeline_version=$(git rev-parse --short HEAD)"
```

### ISP provider ABCs (added 2026-06-26)
```python
# src/shared/base/data_provider.py — three focused ABCs:
#   HistoricalDataProvider: get_historical, health_check, provider_name, supported_intervals
#   RealtimeProvider:        get_latest_quote, is_market_open
#   OptionsProvider:         get_options_chain
# MarketDataProvider (src/bronze/base/) inherits all 3 — backward compat composite
# MarketDataFactory registry type: HistoricalDataProvider (not MarketDataProvider)
# Future providers: inherit only the interfaces they actually support
```

### IBKRProvider — conid cache (updated 2026-06-26)
```python
# Persisted to ~/.tradeanalytics/ibkr_conid_cache.json
# Loaded on __init__, saved after each new lookup
# Survives job restarts — avoids repeated contract searches across runs
```

### Local execution via Databricks Connect (IBKR ingestion path)
```python
# conda env has databricks-connect==15.4.25 (replaces standalone pyspark + delta-spark)
# Tests still pass — BronzeWriter(mode="local") uses in-memory store, no Spark needed
from databricks.connect import DatabricksSession
spark = DatabricksSession.builder.remote(
    host="https://dbc-bf0075e6-07aa.cloud.databricks.com",
    token=TOKEN,          # from ~/.databrickscfg profile handh-trade-aws
    cluster_id=CLUSTER_ID # interactive cluster, DBR 15.4
).getOrCreate()
# Python (incl. IBKR HTTP calls) runs on local Mac; Spark writes go to Unity Catalog
```

### Notebook — Cell 7 (provider registration)
```python
# NO config._data override — IBKR used as primary per config
MarketDataFactory.register("ibkr",  IBKRProvider)
MarketDataFactory.register("yahoo", YahooProvider)
# Widgets: symbols, dry_run, as_of_date, start_date, end_date
```

---

## 6. Reference & Control Table Architecture (Designed 2026-06-25, not yet built)

Full design doc: `TradeAnalytics — ML Trading Pipeline/Ingestion_Architecture_Design.md`

### Core design decisions (locked)

**Never use ticker symbol as a primary key.** Symbols are fragile — they change (FB→META), get reused after delistings, and are ambiguous across exchanges. Every table uses `instrument_id` (internal BIGINT surrogate) as the join key. Symbol is a display attribute in `instrument_listing` only.

**Desired state vs actual state reconciliation.** The IngestionPlanner DERIVES load type by comparing what we want (`ticker_feed_config`) against what we have (`ingestion_watermark`). Load type is never configured — it is always computed. No watermark = initial load. Watermark behind target = incremental. Watermark ahead of target_start = history extension.

**Watermark is ACTUAL STATE — never manually touch it.** It is a system mirror. Manual inserts or updates cause phantom loads or silent skips. Control ingestion via `ticker_feed_config` (ongoing) or `ingestion_command` (one-off).

**Load type is derived, never configured.** You control desired state; the planner figures out what to fetch.

### Unity Catalog schemas

```
tradeanalytics.reference   — WHAT exists and WHAT we want
tradeanalytics.control     — WHAT has happened and WHAT to do next
tradeanalytics.bronze      — append-only OHLCV (existing)
tradeanalytics.silver      — Phase 3 (feature store)
tradeanalytics.gold        — Phase 4 (signals, ML)
```

### Reference tables

| Table | Purpose | Key |
|---|---|---|
| `reference.instrument` | Permanent master record per financial instrument | `instrument_id` (BIGINT, auto, permanent) |
| `reference.instrument_listing` | Symbol, exchange, MIC — SCD Type 2 | `listing_id`; FK `instrument_id` |
| `reference.instrument_vendor_id` | Vendor-specific IDs (IBKR conid, Polygon ticker, etc.) — SCD Type 2, append-only | `vendor_mapping_id`; FK `instrument_id` |
| `reference.universe_membership` | Which instruments are in which trading universe | FK `instrument_id` |
| `reference.ticker_feed_config` | DESIRED STATE — target dates, run_frequency, batch_group, priority, preferred_vendor | FK `instrument_id` |
| `reference.corporate_actions` | All corporate events (splits, dividends, spinoffs) — append-only audit log | `action_id`; FK `instrument_id` |
| `reference.adjustment_factors` | Cumulative price adjustment factors per date — deferred to Phase 4 | FK `instrument_id` |
| `reference.market_calendar` | Exchange holidays and half-days | `exchange_mic + date` |

**`instrument` key fields:** `instrument_id`, `isin` (ISO 6166), `figi` (OpenFIGI), `asset_class`
**NOTE: `ibkr_con_id` removed from `reference.instrument` in Phase 3A — migrated to `reference.instrument_vendor_id`**

**`instrument_listing` key fields:** `symbol`, `exchange`, `exchange_mic` (ISO 10383), `currency`, `min_tick_size`, `min_lot_size`, `valid_from`, `valid_to`, `is_current`, `change_reason`

**`ticker_feed_config` key fields:** `target_start_date`, `target_end_date` (NULL=ongoing), `run_frequency` (daily|weekly|monthly|on_demand), `batch_group` (A|B|C|D), `priority`, `is_active`, `max_lookback_days`

**`adjustment_factors`:** Bronze stores RAW prices. Silver views multiply by `cumulative_split_factor` at read time. Execution always uses raw prices. Bronze is never modified for corporate actions.

### Control tables

| Table | Purpose | Key |
|---|---|---|
| `control.ingestion_watermark` | ACTUAL STATE — earliest/latest date fetched, status, consecutive_failures, vendor | FK `instrument_id + stream` |
| `control.ingestion_batch_config` | Which batch_groups and run_frequencies execute per job type | `job_type` |
| `control.ingestion_command` | Explicit one-off instructions (FORCE_RELOAD, PAUSE, RESUME, SKIP_ONCE) | auto, consumed once |
| `control.job_run_log` | Full audit trail — records_new, records_rejected, load_type, status, vendor per run | auto |
| `control.corporate_action_candidates` | Saga state machine — DETECTED → CLASSIFIED → RELOAD_TRIGGERED → RESOLVED | `candidate_id`; FK `instrument_id` |

**Watermark status values:** `active` | `suspended` (after N consecutive failures, stops auto-retry) | `paused`

**ingestion_command action values:** `FORCE_RELOAD` | `HISTORY_LOAD` | `PAUSE` | `RESUME` | `SKIP_ONCE`

### IngestionPlanner load type derivation (new design)

```
For each instrument per run:
1. Check ingestion_command for pending commands → process first (PAUSE/RESUME/FORCE_RELOAD)
2. Read ticker_feed_config (desired state)
3. Read ingestion_watermark (actual state)
4. Derive:
   - No watermark           → INITIAL_LOAD (from target_start_date or today - max_lookback_days)
   - status = suspended     → SKIP
   - latest_date < target   → INCREMENTAL
   - earliest_date > target_start → HISTORY_EXTENSION
   - gaps detected          → GAP_FILL
   - FORCE_RELOAD command   → FORCE_RELOAD
   - otherwise              → NO_OP
```

### Day-to-day operator actions

| Task | Action |
|---|---|
| Add new ticker | INSERT into `instrument`, `instrument_listing`, `universe_membership`, `ticker_feed_config` |
| Pause a ticker | `UPDATE ticker_feed_config SET is_active = false` OR INSERT PAUSE command |
| Resume suspended ticker | INSERT RESUME into `ingestion_command` |
| Force reload date range | INSERT FORCE_RELOAD into `ingestion_command` with override dates |
| Extend history further back | `UPDATE ticker_feed_config SET target_start_date = '2010-01-01'` |
| Control daily batch scope | `UPDATE ingestion_batch_config` — change batch_groups_included |

### Table → Python class mapping (to be built in Phase 2.5)

| Table | Python class | Location |
|---|---|---|
| `reference.instrument` | `InstrumentManager` | `src/reference/managers/` |
| `reference.instrument_listing` | `InstrumentManager` | `src/reference/managers/` |
| `reference.ticker_feed_config` | `TickerFeedConfigManager` (replaces `TickerReader`) | `src/reference/managers/` |
| `reference.market_calendar` | `MarketCalendarManager` | `src/reference/managers/` |
| `reference.adjustment_factors` | `AdjustmentFactorManager` | `src/reference/managers/` |
| `control.ingestion_watermark` | `WatermarkManager` (move from `src/bronze/writers/`) | `src/control/watermark/` |
| `control.ingestion_command` | `IngestionCommandProcessor` | `src/control/jobs/` |
| `control.ingestion_batch_config` | Read directly by `BronzeIngestionJob.run()` | — |
| `control.job_run_log` | Written by `BronzeIngestionJob` per ticker | — |

---

## 7. Databricks Job

```yaml
job_name:   "[dev handh_stocks] [dev] Bronze Daily Ingestion"
job_id:     174217366433843
schedule:   "0 0 19 * * ?" (7pm Eastern Mon-Fri)
status:     PAUSED
policy_id:  001B2429FBD0E8AD
cluster:    m5.xlarge, 1 worker, SPOT_WITH_FALLBACK
smoke_test_params:
  symbols:    "SPY"
  start_date: "2026-06-16"
  end_date:   "2026-06-17"
  dry_run:    "false"
  as_of_date: ""
```

---

## 8. Smoke Test Results

### Run 1 — 2026-06-22 (Yahoo — stale cached notebook, invalid)
Wrote 2,636 SPY records with `source=yahoo`. Tables were truncated 2026-06-25.

### Run 3 — 2026-06-28 (IBKR — Phase 2.5 smoke test) ✅

| Check | Result |
|-------|--------|
| source | ✅ `ibkr` |
| instrument_id | ✅ 505 (SPY permanent surrogate key) |
| watermark table | ✅ `control.ingestion_watermark` (not bronze schema) |
| watermark key | ✅ `instrument_id=505 + stream=daily` |
| DeltaUniverseReader | ✅ read from `reference.ticker_feed_config` JOIN |
| Date range | 2026-06-16 → 2026-06-17 (2 records) |
| Records written | 2 |
| Records rejected | 0 |
| Execution | Local Databricks Connect, cluster `0624-230428-oxt7myog` |

### Run 2 — 2026-06-25 (IBKR — confirmed valid) ✅

| Check | Result |
|-------|--------|
| source | ✅ `ibkr` (all 29 records) |
| record_version | ✅ 1 |
| ingestion_type | ✅ backfill |
| data_quality_flag | ✅ false |
| data_completeness_pct | ✅ 64.0 |
| batch_id | ✅ `batch_daily_20260624_232449` |
| ingested_at | ✅ UTC timestamp |
| ingested_by | ⚠️ NULL (see Known Issues) |
| Date range | 2026-05-13 → 2026-06-24 (29 trading days) |
| Records written | 29 |
| Records rejected | 0 |
| Execution | Local Databricks Connect (Python on Mac → Unity Catalog) |

**Note on record count:** 29 records (not full 10yr history) because `_days_to_period()` in
IBKRProvider maps the INITIAL_LOAD request to ~1 month. This is OQ-1 — pre-existing known issue.

---

## 9. Architecture Improvement Backlog (from review 2026-06-26)

Implement one at a time. Do NOT do all at once.
Full review doc: `TradeAnalytics — ML Trading Pipeline/Architecture_Review_2026_06.md`

| # | Issue | Fix | Priority | Status |
|---|---|---|---|---|
| AR-1 | `MarketDataProvider` ABC forces all providers to implement options + realtime even if unsupported (ISP violation) | Split into `HistoricalDataProvider`, `RealtimeProvider`, `OptionsProvider` ABCs | Phase 2.5 | ✅ Done (2026-06-26) |
| AR-2 | Hardcoded US holiday calendar (150 lines) in `ingestion_planner.py` — breaks for any non-US market or crypto | `TradingCalendar` ABC + `USEquityCalendar` impl, inject into `IngestionPlanner` | Phase 2.5 | ✅ Done |
| AR-3 | No ABC behind `TickerReader` — Phase 2.5 CSV→Delta migration requires changing `BronzeIngestionJob` imports | `UniverseReader` ABC + `InstrumentInfo` model; `TickerReader` implements it | Phase 2.5 | ✅ Done |
| AR-4 | No ABC behind `WatermarkManager` — migration to `control` schema requires changing call sites | `WatermarkStore` ABC in `src/shared/base/`; both implementations implement it | Phase 2.5 | ✅ Done (2026-06-26) |
| AR-5 | `BronzeWriter.write_batch()` receives `stream_cfg` config object — storage layer knows about config structure | Pass resolved table names (`main_table`, `rejected_table`) not the config object | Phase 2.5 | ✅ Done (2026-06-26) |
| AR-6 | `DataQualityValidator._compute_completeness()` hardcodes equity OHLCV field lists — breaks for FX/futures/crypto | `CompletenessCalculator` ABC, inject asset-class-specific impl | Phase 3 | ⬜ Not started |
| AR-7 | `_get_pipeline_version()` uses `subprocess.run(git)` — returns "unknown" on Databricks clusters | Read `PIPELINE_VERSION` env var set by DABs; git fallback for local only | Phase 2.5 | ✅ Done (2026-06-26) |
| AR-8 | Provider output contract is a docstring, not enforced — field name typos surface as Spark errors | `OHLCVRecord` TypedDict; providers return `List[OHLCVRecord]` | Phase 3 | ✅ Done (2026-06-26) |
| AR-9 | `IngestionPlanner.plan()` if-cascade — adding ingestion modes from `ingestion_command` table will make it unmaintainable | Strategy pattern: one `PlanStrategy` class per mode, ordered list | Phase 3 | ⬜ Not started |

### src/shared/ layer (current state)
```
src/shared/
├── config/config_loader.py        ✅ exists
├── base/
│   ├── data_provider.py           ✅ HistoricalDataProvider, RealtimeProvider, OptionsProvider (AR-1)
│   ├── universe_reader.py         ✅ UniverseReader + InstrumentInfo (AR-3)
│   ├── watermark_store.py         ✅ WatermarkStore ABC (AR-4)
│   ├── trading_calendar.py        ✅ TradingCalendar ABC (AR-2)
│   └── completeness_calculator.py ← CompletenessCalculator (AR-6, Phase 3)
├── calendar/
│   ├── us_equity_calendar.py      ✅ US holiday calendar (AR-2)
│   ├── exchange_calendars_calendar.py ← Phase 3
│   └── always_open_calendar.py    ← Crypto/FX (Phase 3)
└── models/
    └── ohlcv_record.py            ✅ OHLCVRecord TypedDict (AR-8)
```

---

## 10. Known Issues & Technical Debt

### ⚠️ Date range override not working (investigate before next smoke test)
`start_date`/`end_date` params passed to `job.run()` but IBKR returned full 10yr
history instead of 2 days. Root cause: `_days_to_period()` in IBKRProvider may map
short ranges to minimum IBKR period, or plan override code path not being hit.

**Design alternatives to try next session:**
- **Option A:** Filter records post-fetch in Python — drop anything outside start/end window
  before validation. Safest — provider fetches what IBKR returns, Python filters.
- **Option B:** Pass date constraints directly to `IBKRProvider.get_historical()` and
  enforce strict date filtering inside the provider before returning records.
- **Option C:** Update `daily.yml` `date_range_override` for smoke tests — but requires
  updating tests that check `enabled: false` (feasible, just needs test update).
- **Recommendation: Option A** — post-fetch filter is simplest, most robust, no API changes.

### ⚠️ `ingested_by` is NULL in Delta
IBKR provider sets `ingested_by="ibkr_provider_v1"` on raw records but field arrives
as NULL in Delta. `validator._enrich_record()` uses `enriched.get("ingested_by", None)`
which should preserve the value. Investigate whether it's being overwritten somewhere.

### Phase 2.5 Technical Debt — ALL RESOLVED ✅
1. ~~`WatermarkManager` symbol key~~ → `DeltaWatermarkStore` with `instrument_id+stream` key ✅
2. ~~`TickerReader` CSV~~ → `DeltaUniverseReader` reading from `reference.ticker_feed_config` ✅
3. ~~`IngestionPlanner` YAML-driven~~ → table-driven with `InstrumentInfo` + `IngestionWatermarkRecord` ✅
4. `market_calendar` hardcoded holidays — still TODO for Phase 3 (low priority)

### Phase 3 Technical Debt (fix during Phase 3)
1. **BronzeRecord not instantiated** — records flow as dicts, all defaults bypassed.
   Real fix: `validator` creates `BronzeRecord.from_dict()` → typed `to_dict()` output.
   Removes need for manual type alignment in `_spark_append()` (560 lines → 3 lines).
2. **`additional_rules`** in config — verify RuleEngine handles list-of-dicts correctly.
3. **Spike/flash crash DQ rule missing** — add rolling volatility filter to `data_quality_rules.yml`: flag bars where |close_t - close_t-1| / close_t-1 > N × σ_rolling.
4. **`adjustment_factors` table needed before feature engineering** — adjusted close is a feature input; raw close on split dates will produce artificial jumps in features.
5. **`CompletenessCalculator` ABC** (AR-6) — `_compute_completeness()` still hardcodes equity OHLCV field lists; needs asset-class-specific impl when FX/futures/crypto added.

---

## 10. Config Key Reference

```python
config.databricks.catalog          # "tradeanalytics"
config.sources.primary             # "ibkr"
config.sources.fallback            # "yahoo"
config.sources.ibkr.base_url       # "https://localhost:5055/v1/api"
config.daily.enabled               # True
config.daily.table                 # "market_data_daily"
config.daily.rejected_table        # "market_data_rejected"
config.daily.watermark_table       # "ingestion_watermark_daily"
config.daily.intervals             # ["1d"]
```

---

## 11. Delta Tables

### Bronze
| Table | Full Name | Records |
|-------|-----------|---------|
| Main | `tradeanalytics.bronze.market_data_daily` | ibkr records (yahoo stale rows deleted 2026-06-28) |
| Rejected | `tradeanalytics.bronze.market_data_rejected` | 0 |

S3: `s3://handh-trade-refined-use1/bronze/`

### Reference (built Phase 2.5 + Phase 3A)
| Table | Full Name | Records | Notes |
|-------|-----------|---------|-------|
| Instruments | `tradeanalytics.reference.instrument` | 8 | `ibkr_con_id` removed in Phase 3A |
| Listings | `tradeanalytics.reference.instrument_listing` | 8 | |
| Vendor IDs | `tradeanalytics.reference.instrument_vendor_id` | 8 | IBKR conids migrated from `instrument` |
| Universe | `tradeanalytics.reference.universe_membership` | 8 | |
| Feed config | `tradeanalytics.reference.ticker_feed_config` | 8 | Added `preferred_vendor`, `fallback_vendor` |
| Market calendar | `tradeanalytics.reference.market_calendar` | seeded | |
| Corporate actions | `tradeanalytics.reference.corporate_actions` | 0 | New in Phase 3A |

### Control (built Phase 2.5 + Phase 3A)
| Table | Full Name | Records | Notes |
|-------|-----------|---------|-------|
| Watermark | `tradeanalytics.control.ingestion_watermark` | 1 (SPY) | Added `vendor` column |
| Batch config | `tradeanalytics.control.ingestion_batch_config` | 3 | |
| Commands | `tradeanalytics.control.ingestion_command` | 0 | |
| Job run log | `tradeanalytics.control.job_run_log` | 0 | Added `vendor` column |
| Corp action candidates | `tradeanalytics.control.corporate_action_candidates` | 0 | New in Phase 3A — saga state machine |

### Key instrument_ids (permanent — verified 2026-06-28)
| Symbol | instrument_id |
|--------|--------------|
| NVDA | 1 |
| AAPL | 5 |
| MSFT | 9 |
| AMZN | 13 |
| GOOGL | 17 |
| SPY | TBC — scroll reference.instrument_listing |
| QQQ | TBC — scroll reference.instrument_listing |
| TSLA | TBC — scroll reference.instrument_listing |

Note: reference table was seeded with a larger universe (20+ instruments). IDs 1-19+ visible.
Old Phase 2.5 IDs (SPY=505) are stale — tables were truncated and re-seeded 2026-06-28.

---

## 12. Phase 3 Preview (Silver — Feature Engineering)

### Gap analysis performed 2026-06-28
External AI feature/label catalog validated against our architecture. Full findings in memory: `phase3_external_catalog_gap_analysis.md`. Key locked decisions:

**Violations to fix before any code:**
- Feature store primary key = `instrument_id + timestamp` (NOT `symbol`) — symbols change/get reused
- Hurst exponent is NOT a bar-level microstructure feature — it is a weekly Meta-Router input only (Phase 4)
- FinBERT/transformer sentiment gated on XGBoost baseline proof first (ML sequencing rule)
- News sentiment = async batch (daily/4H), never a 5-minute live feed (LLM path rule)

**What to adopt from catalog:** point-in-time correctness, stationarity (raw prices forbidden in feature store), scale invariance, feature metadata structure (formula + intuition + windows + output type + `min_warmup_bars`), continuous/binary/triple-barrier label designs

**Data feasibility for Phase 3:** only daily OHLCV Bronze data is available now. Intraday microstructure, order book, options, and sentiment features → ABCs + placeholder only until data pipelines exist.

### Phase 3A — Corporate Actions + Vendor-Agnostic Schema (complete design)

**Problem:** IBKR retroactively adjusts all historical prices on every split. Our Bronze
stores old prices. After a 4-for-1 split, stored $500 bars become wrong — IBKR now
returns $125 for the same date. Bronze must be reloaded (full history, not targeted).

**Detection mechanism:** Daily Spark bulk query compares latest Bronze price per instrument
against what IBKR currently returns for the same bar. Ratio deviation > 40% = corporate action candidate.

**Saga state machine (`control.corporate_action_candidates`):**
```
DETECTED → CLASSIFIED → RELOAD_TRIGGERED → RESOLVED
                                          → FALSE_POSITIVE (earnings gap, not a split)
                                          → FAILED (retries exhausted)
```
Recovery step runs on every job — picks up any row not in a terminal state.

**Python files (new Phase 3A):**
| File | Purpose |
|---|---|
| `src/shared/base/corporate_actions_provider.py` | `CorporateActionsProvider ABC` — vendor-agnostic interface |
| `src/reference/providers/ibkr_corporate_actions_provider.py` | IBKR implementation (stub endpoint, conid-based) |
| `src/reference/providers/yahoo_corporate_actions_provider.py` | Yahoo implementation — seed/verify only, never production |
| `src/control/corporate_actions/detector.py` | `CorporateActionDetector` — Spark bulk detection |
| `src/control/corporate_actions/classifier.py` | `CorporateActionClassifier` — ratio-to-event-type classification + saga step |

**IBKRCorporateActionsProvider.from_spark()** loads conid map from `reference.instrument_vendor_id`.
**YahooCorporateActionsProvider.from_spark()** loads symbol map from `reference.instrument_listing`.

**Vendor-agnostic design:** `ibkr_con_id` removed from `reference.instrument`. All vendor IDs
in `reference.instrument_vendor_id` (vendor=ibkr|polygon|bloomberg). Adding a new vendor =
INSERT rows for all instruments, zero schema changes.

**Notebook to run:** `notebooks/reference/04_phase3a_schema_additions.py`
Run Parts 1–3 first, verify all rows match, then run Part 4 (drop column). Safe to re-run.

### Phase 3 build sequence (locked)

| Part | What | Prerequisite |
|---|---|---|
| 3A | Vendor-agnostic schema + corporate actions tables + Python classes | ✅ Written, notebook pending run |
| B | ABCs: `IndicatorEngine`, `FeatureEngineer`, `LabelEngine` (+ `min_warmup_bars` contract) | Part 3A complete |
| C | 60+ features — daily OHLCV only (see feature plan below) | Parts 3A + B |
| D | Label calibration (histogram analysis first) + binary/continuous labels | Part C |
| E | Enable intraday stream → intraday microstructure features | After stream enabled |

### Target: 60+ features — planned feature groups (Phase 3C)

**Group 1 — Trend (10 features)**
| Feature | Formula | Notes |
|---|---|---|
| SMA 10, 20, 50, 200 | Rolling mean of close | Raw values — use as ratio to close, not raw price |
| EMA 10, 20, 50 | Exponentially weighted close | More weight on recent prices |
| SMA crossover binary | `sma_20 > sma_50` → 1/0 | Encodes trend direction |
| SMA crossover distance | `(sma_20 - sma_50) / close` | Normalised — captures how deep into trend |
| Price vs SMA200 | `close / sma_200 - 1` | Long-term mean reversion gauge |

**Group 2 — Momentum (12 features)**
| Feature | Formula | Notes |
|---|---|---|
| RSI 14 | Classic Wilder RSI | Bounded 0–100, overbought/oversold |
| RSI 7 | Faster RSI | Captures short-term exhaustion |
| ROC 5, 10, 21 | `(close_t / close_t-n) - 1` | Unbounded momentum, different to RSI |
| MACD line | EMA(12) - EMA(26) | Trend momentum convergence |
| MACD signal | EMA(9) of MACD | Trigger line |
| MACD histogram | MACD - signal | Direction of momentum change |
| Momentum lags | 1d, 5d, 10d, 21d returns | Explicit price change over N days |

**Group 3 — Volatility (8 features)**
| Feature | Formula | Notes |
|---|---|---|
| ATR 14 | Average True Range | Normalised volatility measure |
| ATR % | `atr_14 / close` | Scale-invariant version |
| Bollinger upper/lower | SMA ± 2σ | Volatility envelope |
| Bollinger %B | `(close - bb_lower) / (bb_upper - bb_lower)` | 0=at lower band, 1=at upper band |
| Bollinger width | `(bb_upper - bb_lower) / bb_mid` | Volatility expansion/contraction |
| Realised vol 10d | Rolling 10d std of daily returns | Short-term volatility regime |
| Realised vol 21d | Rolling 21d std of daily returns | Medium-term volatility regime |

**Group 4 — Volume (8 features)**
| Feature | Formula | Notes |
|---|---|---|
| Volume SMA ratio | `volume / volume.rolling(20).mean()` | RVOL — relative volume vs average |
| OBV | Cumulative `±volume` by direction | On-Balance Volume — trend confirmation |
| OBV slope | `(obv_t - obv_t-5) / 5` | Direction of OBV trend |
| Volume ROC 5 | `(vol_t / vol_t-5) - 1` | Volume momentum |
| Price × Volume | `close × volume` | Dollar volume — liquidity proxy |
| CMF 20 | Chaikin Money Flow | Buying/selling pressure |
| Volume z-score | `(volume - vol_mean) / vol_std` | Standardised volume shock |
| High volume flag | `volume > 2 × vol_sma20` | Binary — unusual volume day |

**Group 5 — Price structure (8 features)**
| Feature | Formula | Notes |
|---|---|---|
| Overnight gap | `(open - prev_close) / prev_close` | Gap up/down at open |
| Intraday range | `(high - low) / close` | Bar range as % of price |
| Upper shadow | `(high - max(open,close)) / close` | Rejection wick above |
| Lower shadow | `(min(open,close) - low) / close` | Rejection wick below |
| Body size | `abs(close - open) / close` | Candle body % |
| Close position | `(close - low) / (high - low)` | Where close sits in bar range |
| Log return | `log(close / prev_close)` | Stationary, normally distributed |
| Garman-Klass vol | OHLC volatility estimator | More efficient than close-to-close |

**Group 6 — Cross-sectional / relative (6 features)**
| Feature | Formula | Notes |
|---|---|---|
| Rel strength vs SPY | `close / spy_close - 1` | Alpha vs broad market |
| Rel strength vs QQQ | `close / qqq_close - 1` | Alpha vs tech |
| Beta 60d | Rolling 60d regression vs SPY | Market sensitivity |
| Correlation vs SPY 20d | Rolling 20d correlation | Market co-movement |
| Sector rank (return) | Percentile rank of 21d return in universe | Cross-sectional momentum |
| Z-score of 21d return | `(ret_21d - mean) / std` across universe | Standardised relative momentum |

**Group 7 — Regime indicators (4 features)**
| Feature | Formula | Notes |
|---|---|---|
| VIX proxy | Realised vol of SPY last 21d | Fear gauge approximation (until VIX data added) |
| Market breadth | SPY above SMA200 → 1 else 0 | Bull/bear regime binary |
| Vol regime | `atr_pct > 80th percentile rolling` | High/low vol binary |
| Trend strength | `abs(sma_20 - sma_50) / atr_14` | How strong is the trend vs noise |

**Group 8 — Lagged features (4 features)**
| Feature | Notes |
|---|---|
| RSI t-1, t-2 | RSI from prior bars — gives model memory of recent signal |
| ROC(10) t-1 | Prior momentum reading |
| ATR% t-5 | Prior volatility state |

**Total planned: ~60–65 features.** More can be added by varying windows (RSI-7 vs RSI-14 already counts as 2).

### ML implementation checklist (from notebook review 2026-06-29 — MUST implement)

| # | Item | Phase | Status |
|---|---|---|---|
| 1 | ROC (Rate of Change) as a feature — unbounded momentum | 3C | ⬜ |
| 2 | Historical swing distribution analysis before picking label threshold | 3D | ⬜ |
| 3 | Binary label: `return >= threshold within N days` with calibrated N and threshold | 3D | ⬜ |
| 4 | `scale_pos_weight = count(0) / count(1)` — handle class imbalance in XGBoost | 4 | ⬜ |
| 5 | Probability threshold tuning 0.30→0.90 — default 0.5 is wrong for rare events | 4 | ⬜ |
| 6 | Stateful walk-forward — portfolio state carries across folds, not reset | 4 | ⬜ |
| 7 | Nested exit optimisation per fold — SL%, TP%, holding period, MA exit | 4 | ⬜ |
| 8 | Sortino Ratio as primary optimisation metric (not Sharpe) | 4 | ⬜ |
| 9 | SHAP values logged to MLflow after each training run | 4 | ⬜ |
| 10 | Out-of-sample holdout set — completely withheld from all development | 4 | ⬜ |

### Step-by-step teaching order (concept first, always)
1. What is a feature? (EMA, RSI, MACD on real SPY data — visualise first)
2. Why raw prices are forbidden (stationarity, split events)
3. What is `adjustment_factors` and why it must come first
4. What is a label? (forward returns — what are we predicting and why?)
5. What is the feature matrix? (rows=dates, cols=features — read before training)
6. Train first XGBoost on SPY
7. Walk-forward validation — out-of-sample IC
8. Does it actually have edge?

Planned ABCs: `IndicatorEngine`, `FeatureEngineer`, `FeatureScaler`, `RegimeDetector`, `LabelEngine`
Feature store: `tradeanalytics.feature_store.*`

---

## 13. Session Start Checklist

1. Upload this `CLAUDE.md` file first
2. Confirm: any changes since last session?
3. Run `/Users/hemachandra/anaconda3/envs/tradeanalytics/bin/python -m pytest tests/ -q` — confirm 405 tests passing (402 + 3 new corporate actions tests) on branch `feature/phase3-silver`
4. State the immediate task
5. **Next up: Phase 3 (Silver — feature engineering, step-by-step teaching)**

## 14. Ops Runbook — Known Gotchas

### After `databricks bundle deploy` — cluster module cache
Databricks clusters cache Python `.pyc` bytecode in memory. Deploying new code via
`databricks bundle deploy` updates files on the workspace filesystem but the running
cluster does not reload them automatically.

**Fix:** After every deploy, either:
- Restart the cluster (Compute → Restart), OR
- Detach and re-attach the notebook to the cluster

Serverless compute does not have this problem — it spins a fresh environment per run.
But serverless has **no outbound internet access** in this workspace — Yahoo Finance and
IBKR are both unreachable. Always use the regular cluster for notebook-based ingestion.

### Ingestion execution paths
| Path | IBKR reachable | Yahoo reachable | Module cache issue |
|---|---|---|---|
| Regular cluster (notebook) | ❌ | ✅ | ✅ restart after deploy |
| Serverless (notebook) | ❌ | ❌ | ✅ always fresh |
| Databricks Connect (local Mac) | ✅ | ✅ | ✅ always fresh |

**Production ingestion** must use Databricks Connect from local Mac — only path with IBKR access.
## Project State — June 2026

---

### 1. Project Purpose & Goals

**What TradeAnalytics Is**

TradeAnalytics is a Signal-as-a-Service (SaaS) quantitative trading platform built by a single developer (HC). It is designed as a centralised, systematic ML-driven signal engine that ingests market data, performs feature engineering, runs quant/ML strategies, fuses signals, assigns risk scores, and publishes portfolio-agnostic trading signals. It is not a portfolio manager and it is not an execution bot — those are separate concerns.

**Two Operating Models**

**Model 1 — Signal Generator (Producer):**
A centralised engine owned and operated by HC. It ingests market data, builds features, runs models, fuses signals, and publishes a stream of portfolio-agnostic trading signals (ticker, direction, score, regime, confidence, risk metadata). It has no knowledge of who is consuming the signals or what they do with them.

**Model 2 — Signal Consumer (Per-User Portfolio Engine):**
A decentralised model where each consumer independently applies their own portfolio logic, position sizing, execution rules, and risk overlays to the published signals. Portfolio risk is each consumer's own responsibility. The Signal Generator is completely decoupled from consumer portfolio state.

**Why It Exists**

- Primary: improve engineering skills across quant research, ML, data engineering, and cloud architecture simultaneously
- Secondary: gain systematic, evidence-based edge in financial markets
- Tertiary: build a verified track record to share or monetise signals (validate → share → charge, in that order)

**What Success Looks Like**

1. A live signal engine running daily with real IBKR market data
2. Signals that survive walk-forward out-of-sample validation with positive risk-adjusted returns
3. A paper trading track record of ≥6 months before live execution
4. Signals shareable with friends (Phase 4b) and eventually monetisable (Phase 6) if the track record justifies it

---

### 2. Architecture & Design Decisions

#### 2.1 Platform Layer Assignment

| Concern | Tool | Reason |
|---|---|---|
| Primary compute and storage engine | Databricks (handh-dev workspace, us-east-1) | Unified Delta Lake, Unity Catalog, MLflow, Workflows, DABs in one platform |
| Networking, storage, IAM, secrets | AWS (account 311925399625, us-east-1) | S3 for Delta storage, Secrets Manager for credentials, VPC for Databricks workspace |
| Market data (Phases 1–4) | IBKR Client Portal REST API | Free with existing live account (U5498892, NZD base currency) |
| Market data fallback / unit tests | Yahoo Finance | Free, no API key, used only in test fixtures — never in production |
| Execution (Phase 5+) | IB Gateway on EC2 t3.small + ib_insync | Programmatic order submission; paper trading first |
| ML experiment tracking | MLflow (Databricks-managed) | Native integration, no separate infrastructure |
| Orchestration | Databricks Workflows (DABs) | Co-located with compute, avoids Airflow overhead for a solo project |

**Rejected alternatives:**
- Polygon.io: excluded as unnecessary given IBKR covers all data needs for free
- Apache Kafka / Kinesis: not decided; deferred to Phase 4 signal publishing design
- Airflow: excluded as orchestration overhead not justified at this scale
- Standalone MLflow server: excluded in favour of Databricks-managed MLflow
- NAT Gateway: deferred to Phase 5 (live trading with real capital)

#### 2.2 Universal Code Pattern (Mandatory — Zero Exceptions)

Every new component in the system must follow this exact pattern:

```
ABC (contract) → Registry (catalogue) → Factory (creator) → Config (YAML selector)
```

Adding a new data provider, feature calculator, ML model, or signal publisher means:
1. Implement the ABC
2. Register it in the Registry
3. Add a config YAML entry to select it

There must be zero if/else chains for component selection anywhere in the codebase. Config YAML is the only selector. This pattern is validated across the entire codebase and must not be violated.

#### 2.3 Medallion Architecture (Bronze / Silver / Gold)

**Bronze — Append-Only Ingestion Layer**
- `delta.appendOnly=true` — no UPDATE, DELETE, or MERGE on the main table ever
- Amendments are appended as new records with `record_version` incremented
- Three-layer deduplication:
  - Layer 1: IngestionPlanner date range (job-level idempotency)
  - Layer 2: BronzeWriter classify (new / amend / skip per record)
  - Layer 3: Silver reads Bronze through a ROW_NUMBER deduplication window (partitioned by unique key, ordered by `record_version DESC`)
- All audit fields stamped in `validator._enrich_record()` — records flow as plain Python dicts; `BronzeRecord` dataclass is never instantiated during ingestion
- Silver always reads Bronze through the dedup window — guarantees exactly one record per unique key downstream

**Silver — Feature Engineering Layer** (Phase 3, not yet built)
- IndicatorEngine, FeatureEngineer, FeatureScaler, RegimeDetector ABCs
- Feature store: `tradeanalytics.feature_store.*`
- Batch and streaming feature paths will be separated (slow: Hurst/HMM/Sentiment; fast: RSI/VWAP/price momentum)

**Gold — Signal and ML Layer** (Phase 4, not yet built)
- MLModel, LabelingEngine, SignalFusion, BacktestValidator ABCs
- XGBoost/LightGBM first for all model clusters; deep learning only after XGBoost proven insufficient with out-of-sample evidence
- SignalQualityEngine (Type 1 filters), SignalPublisher, SignalLog (unbiased retraining), PaperTracker

#### 2.4 LLM Placement Rule (Locked — Validated by 4 Independent AI Reviews)

LLM is **never** in the synchronous execution path. It operates only in two async modes:

- **Upstream (async, batch):** Catalyst Sentiment Score — reads news/earnings, outputs a numeric sentiment score that enters the feature matrix as a feature. Not a decision gate.
- **Downstream (async, post-trade):** Reasoning briefs, attribution reports, and signal explanations generated after position resolution. Not a decision gate.

**Why:** LLM in the synchronous path introduces 200–800ms latency, non-determinism that makes backtesting impossible, and a single point of failure. All four independent AI reviews (ChatGPT, Gemini, Qwen, CoPilot) flagged this unanimously.

#### 2.5 Risk Architecture

- **Type 1 risk (Signal Generator):** Signal quality filters — volatility-adjusted confidence, liquidity check, regime suitability. Applied by SignalQualityEngine before publishing. Centralised.
- **Type 2 risk (Consumer):** Portfolio-level risk — position sizing, drawdown limits, exposure caps, stop-loss. Applied by each consumer independently. External to the Signal Generator.
- Hard risk constraints are checked **before** SignalFusion and MLModel evaluation — never after.

#### 2.6 Signal Log Rule

Every signal generated is logged to `gold.signal_log` regardless of outcome, including signals rejected by risk. Fields include: `signal_score`, `regime`, `was_rejected_by_risk`, `rejection_reason`, `actual_outcome` (backfilled after hold period). This prevents survivorship bias in training data and enables unbiased model retraining.

#### 2.7 Stateless vs Stateful Components

- **Stateless (scale horizontally):** IngestionJob, IndicatorEngine, FeatureEngineer, MLModel.predict(), LLMAgent
- **Stateful (single coordinated instance):** WatermarkManager, KillSwitch, OrderManager, PortfolioState
- All state lives in Delta Lake — never in memory between runs

#### 2.8 No-Hardcoding Rule (Permanent)

All environment-specific values (catalog name, schema names, credentials, thresholds) flow through ConfigLoader. They are never hardcoded in source files. All thresholds are injected from stream configs at runtime — `data_quality_rules.yml` has no hardcoded values.

#### 2.9 Hurst Exponent + HMM — Complementary, Not Alternatives

- **Hurst Exponent:** measures long-term memory of the price series; requires 60–100 bars to be statistically reliable; used as weekly Meta-Router input to route symbols to momentum vs mean-reversion clusters. Correct use: weekly routing decision.
- **HMM:** identifies current regime state (bull/bear/sideways/high-vol) at bar level; used as a bar-level feature in the feature matrix.
- These are complementary. Do not replace one with the other. Qwen's suggestion to remove Hurst in favour of HMM was rejected because it misunderstood the distinct roles.

#### 2.10 ML Sequencing Rule

XGBoost/LightGBM/Random Forest are mandatory first choices for all model clusters. LSTM, Transformers, TFT, and Autoencoders are only permitted after XGBoost has been proven insufficient with out-of-sample evidence. This is a hard gate — not a suggestion.

---

### 3. Current State

#### Phase 1 — Infrastructure (COMPLETE)

- AWS account secured: root MFA enabled, billing budget ($50/month), CloudTrail logging
- IAM admin user: `hc-admin` with `AdministratorAccess`
- VPC: `vpc-08931caf43fb6812e` (CIDR `10.0.0.0/16`), two public subnets (us-east-1a, us-east-1b), internet gateway, route table, security group
- S3 buckets: `handh-trade-raw-use1`, `handh-trade-refined-use1`, `handh-trade-mlflow-use1`, `handh-trade-dbx-root-use1` — all versioned, public access blocked
- Databricks workspace: `handh-dev` (Premium, us-east-1) at `dbc-bf0075e6-07aa.cloud.databricks.com`
- Unity Catalog: `tradeanalytics` (NOT `handh_trade` — corrected early in Phase 2)
- Schemas: `tradeanalytics.bronze`, `tradeanalytics.silver`, `tradeanalytics.gold`
- Cluster policy: `handh-trade-job-policy` (Job Compute, m5.xlarge, 1–4 workers, SPOT_WITH_FALLBACK)
- Storage credential: `handh-trade-storage-credential`
- External locations: raw / refined / mlflow
- DABs skeleton merged to main (`feature/phase1-dabs-skeleton`)
- Databricks CLI v1.4.0, profile `handh-trade-aws`

#### Phase 2 — Bronze Data Ingestion ✅ COMPLETE

**Branch:** Merged to `main` via PR #6 on 2026-06-25
**Tests:** 249 passing
**Status:** Complete. IBKR smoke test confirmed — 29 SPY records, source=ibkr, 2026-05-13→2026-06-24.

**What is built:**
- `IBKRProvider` and `YahooProvider` implementing `MarketDataProvider` ABC
- `IngestionPlanner` — 8 ingestion modes (see Section 5)
- `DataQualityValidator` — enriches and validates all records; stamps all 11 audit fields via `_enrich_record()`
- `BronzeWriter` — bulk pre-fetch deduplication (N+1 bug fixed), classifies records as new/amend/skip
- `WatermarkManager` — tracks `earliest_date` and `latest_date` per `symbol+interval`
- Delta tables created: `market_data_daily` (59 fields), `market_data_rejected` (18 fields), `ingestion_watermark_daily` (10 fields)
- Databricks notebook and DABs job definition in `databricks.yml`

**Critical bugs fixed during Phase 2 (do not re-introduce):**
- N+1 Spark query storm in `BronzeWriter`: fixed with bulk pre-fetch (single SQL per batch)
- `CANNOT_DETERMINE_TYPE`: all-None columns cause Spark schema inference failure — fixed by reading schema via `spark.table().schema` with `DESCRIBE TABLE` fallback
- `FIELD_DATA_TYPE_UNACCEPTABLE`: `DateType` columns require `datetime.date` objects, not strings
- `WatermarkManager.createDataFrame`: must use explicit `StructType` schema — no inferred schema
- Catalog name mismatch (`handh_trade` → `tradeanalytics`) corrected across 8 files
- Holiday calendar expanded from 2026-only to 2010–2040
- Yahoo provider override in notebook (`config._data` mutation) — removed in current notebook but stale cached version caused the smoke test issue

#### Phase 2.5 — Pre-Phase 3 Restructure 🔄 IN PROGRESS

**Branch:** `feature/phase3-restructure`
**Status:** Folder restructure done, all imports updated, 249 tests passing. Table architecture designed. Code not yet written.

**What is done:**
- `src/ingestion/` → `src/bronze/` (all imports updated)
- `src/config/` → `src/shared/config/` (all imports updated)
- `src/ingestion/readers/` → `src/reference/managers/`
- `src/reference/seed/` — CSV/YAML bootstrap files (not source of truth)
- Placeholder `__init__.py` structure for silver/, gold/, control/, api/, execution/, llm/
- `tests/ingestion/` → `tests/bronze/`, `tests/config/` → `tests/shared/config/`
- `notebooks/` → `notebooks/bronze/` (path updated in `databricks.yml`)
- Full reference & control table architecture designed (see Section 6)

**What is NOT yet done (next Claude Code session):**
- Create `tradeanalytics.reference` and `tradeanalytics.control` Unity Catalog schemas
- Build and seed all reference and control Delta tables
- Migrate `WatermarkManager` to `src/control/watermark/` with `instrument_id` as key
- Replace `TickerReader` (CSV) with `TickerFeedConfigManager` (Delta)
- Update `IngestionPlanner` to table-driven desired/actual state reconciliation
- PR and merge to `main`

**Open design issue OQ-1 (carried forward):**
`start_date`/`end_date` widget params do not constrain IBKR fetch. Option A (post-fetch Python filter in IBKRProvider) still recommended. Will be naturally resolved when IngestionPlanner is rewritten to use `ticker_feed_config` table — the desired date range comes from the table, not widget params.

Git pager permanently disabled: `git config --global core.pager cat`

---

### 4. Future State / Roadmap

**Build sequence (locked):**

```
Phase 2  → Bronze ingestion            (code complete, closing out)
Phase 3  → Silver: feature engineering (next)
Phase 4  → Gold + Signal Platform      (ML models, signal engine, publishing)
Phase 4b → Share signals with friends  (Telegram / REST API)
Phase 5a → Execution bot (HC's portfolio, consumes signal API)
Phase 5b → Friends build their own bots (same signal API, their portfolio)
Phase 6  → Monetise if track record justifies (legal review first)
```

#### Phase 3 — Silver (Feature Engineering) — NEXT

**HC's explicit requirement:** Teach step by step. Explain every concept before writing code. No assumptions about prior ML knowledge.

Planned ABCs: `IndicatorEngine`, `FeatureEngineer`, `FeatureScaler`, `RegimeDetector`

Key components:
- Price/volume indicators: EMA, RSI, MACD, ATR, VWAP, Bollinger Bands
- Volatility features: realised vol, volatility clustering
- Regime features: Hurst Exponent (weekly routing), HMM state (bar-level feature)
- Rolling PCA and cross-sectional features
- Microstructure features (where data supports)
- Feature scaler layer (standard / robust / min-max, regime-aware)
- Feature store: `tradeanalytics.feature_store.*`
- Intraday stream activation: 1h / 4h intervals (config exists, currently disabled)
- Batch/streaming feature path separation: slow-moving (Hurst, HMM, Catalyst Sentiment) vs fast-moving (RSI, VWAP, price momentum)

#### Phase 4 — Gold + Signal Platform

ABCs: `MLModel`, `LabelingEngine`, `SignalFusion`, `BacktestValidator`

Key components:
- Meta-Router: Hurst-based weekly routing to momentum vs mean-reversion clusters
- ML model clusters: momentum, mean-reversion, mega-cap (XGBoost/LightGBM first)
- HMM + GARCH position sizing for mean-reverting regime
- Ensemble/fusion layer: stacking, blending, regime-switching
- SignalQualityEngine (Type 1 filters)
- SignalPublisher (Delta tables → REST / WebSocket / Telegram)
- LLMAgent: Catalyst Sentiment Score (upstream, async) + reasoning briefs (downstream, async)
- SignalLog: every signal logged regardless of outcome
- PaperTracker: paper trading baseline evidence before live

MLflow experiment naming convention (to be confirmed in Phase 4):
- Experiments: `tradeanalytics/{cluster}/{model_type}` (e.g. `tradeanalytics/momentum/xgboost`)
- Model registry: `tradeanalytics.{cluster}.{model_name}` in Unity Catalog

#### Phase 5 — Execution

- IB Gateway on EC2 t3.small + `ib_insync`
- Gateway location: `~/dev/tools/ibkr/clientportal.gw`, port `5055`
- Paper trading before live
- NAT Gateway provisioned at this phase
- ABCs: `BrokerProvider`, `ExecutionEngine`, `ExecutionAlgo`
- Execution algorithms: VWAP, TWAP, POV

#### Phase 6 — Monetisation

- Legal review before any paid offering
- Track record requirement: ≥6 months paper, then live validation
- Sequencing: validate → share → charge

---

### 5. Key Conventions & Patterns

#### 5.1 Naming Conventions

| Resource type | Convention | Example |
|---|---|---|
| AWS resources | `handh-trade-{resource}-{env}` | `handh-trade-raw-use1` |
| S3 buckets | `handh-trade-{purpose}-use1` | `handh-trade-refined-use1` |
| Databricks workspace | `handh-dev` | — |
| Unity Catalog | `tradeanalytics` | — |
| Schemas | `tradeanalytics.{layer}` | `tradeanalytics.bronze` |
| Tables | `tradeanalytics.{layer}.{table_name}` | `tradeanalytics.bronze.market_data_daily` |
| Feature store | `tradeanalytics.feature_store.*` | — |
| Conda environment | `tradeanalytics` | Python 3.11 |
| Git branches | `feature/phase{N}-{description}` | `feature/phase2-data-ingestion` |

**Do not use** `handh_trade` as catalog name — this was an early error corrected in Phase 2. The catalog is always `tradeanalytics`.

#### 5.2 AWS Configuration

- Region: `us-east-1`
- Account ID: `311925399625`
- IAM admin user: `hc-admin`
- CLI profile: `handh-trade-aws`
- Databricks CLI profile: `handh-trade-aws`
- Short prefix: `handh-trade`

#### 5.3 IBKR Configuration

- Account ID: `U5498892` (NZD base currency)
- Gateway: `~/dev/tools/ibkr/clientportal.gw`
- Port: `5055`
- Base URL: `https://localhost:5055/v1/api`
- `account_id` stored in `.env` (never committed)
- IBKR is unreachable from Databricks clusters — gateway runs on local Mac only. For Databricks jobs, Yahoo Finance is the test fallback. IBKR ingestion runs via local execution or a future EC2 gateway.

#### 5.4 Config Architecture

All configuration flows through `ConfigLoader`. Never hardcode environment values.

```
config/dev.yml                          — infrastructure (catalog, schemas, env)
config/sources.yml                      — provider settings (IBKR, Yahoo)
config/streams/daily.yml                — Phase 2 active stream
config/streams/intraday.yml             — Phase 3, currently disabled
config/streams/tick.yml                 — Phase 5, currently disabled
config/risk.yml                         — risk parameters
config/logging.yml                      — log levels
src/reference/data_quality_rules.yml    — shared quality rules, no hardcoded thresholds
config/models/meta_router.yml           — Phase 4 (not yet built)
config/models/momentum.yml              — Phase 4 (not yet built)
config/models/mean_reversion.yml        — Phase 4 (not yet built)
config/models/mega_cap.yml              — Phase 4 (not yet built)
config/models/regime_detector.yml       — Phase 4 (not yet built)
config/signals/fusion.yml               — Phase 4 (not yet built)
config/signals/validation.yml           — Phase 4 (not yet built)
```

Key config accessor patterns (do not hardcode these values):
```python
config.databricks.catalog          # "tradeanalytics"
config.databricks.schemas.bronze   # "bronze"
config.sources.primary             # "ibkr"
config.sources.fallback            # "yahoo"
config.sources.ibkr.base_url       # "https://localhost:5055/v1/api"
config.sources.ibkr.account_id     # from env IBKR_ACCOUNT_ID
config.daily.table                 # "market_data_daily"
config.daily.rejected_table        # "market_data_rejected"
config.daily.watermark_table       # "ingestion_watermark_daily"
```

#### 5.5 Ingestion Modes (8, Priority Order)

```
force_reload        (requires confirm=true)
restatement
explicit_date_range
history_extension
initial_load
gap_fill
incremental
no_op
```

All modes support `symbols:[]` scoping. Watermark tracks `earliest_date` + `latest_date` per `symbol+interval`. Per-ticker `history_start` in `tickers.csv` overrides global `lookback_years`.

#### 5.6 Python Engineering Standards

- Python 3.11, conda env `tradeanalytics`
- Type hints on all public methods
- ABC for every new component category — no concrete classes without an ABC parent
- Dataclasses for value objects
- Enums for all categorical constants
- pytest fixtures and mocks — no real network calls in unit tests
- Yahoo Finance is the only permitted test data source (never IBKR in tests)
- All record flows as plain Python dicts through the pipeline — no ORM-style instantiation during ingestion
- Spark schema: always read via `spark.table().schema` or explicit `StructType` — never infer from dicts
- Date columns: always pass `datetime.date` objects, never strings, to Delta writes

#### 5.7 Repository Structure (Local)

See Section 4 for full structure. Active branch: `feature/phase3-restructure`.

```
/Users/hemachandra/projects/tradeanalytics   — local repo root
├── CLAUDE.md                                — canonical session-start context
├── databricks.yml                           — DABs pipeline (job ID: 174217366433843)
├── src/
│   ├── shared/config/config_loader.py       — ConfigLoader (was src/config/)
│   ├── bronze/                              — Phase 2 ingestion (was src/ingestion/)
│   ├── reference/managers/ + seed/          — TickerReader + CSV seeds
│   ├── control/                             — Phase 2.5 (watermark, command processor)
│   ├── silver/                              — Phase 3 (empty placeholder)
│   └── gold/                               — Phase 4 (empty placeholder)
├── config/                                  — YAML config files
├── tests/bronze/ + shared/config/ + reference/  — pytest suite (249 passing)
└── notebooks/bronze/                        — Databricks notebooks
```

GitHub: `hemachandra-menakuru/tradeanalytics` (public)

#### 5.8 Delta Table Rules

| Rule | Detail |
|---|---|
| Bronze tables | `delta.appendOnly=true` — no exceptions |
| Watermark table | MERGE allowed (not append-only) |
| Silver reads | Always via ROW_NUMBER dedup window over Bronze |
| Amendments | New record, `record_version` incremented, never overwrite |
| Schema changes | Coordinate with Claude Project before implementing |

---

### 6. Open Questions & Risks

#### 6.1 Immediate (Phase 2 Closure)

| # | Issue | Status |
|---|---|---|
| OQ-1 | `start_date`/`end_date` widget parameters do not prevent `INITIAL_LOAD` mode — IBKR returns full 10-year history. Option A (post-fetch Python filtering) designed but not implemented. | Designed, not coded |
| OQ-2 | Smoke test ran with `source=yahoo` due to stale cached notebook. Tables must be truncated and smoke test re-run with IBKR. | Blocked on IBKR gateway being reachable |
| OQ-3 | IBKR Client Portal REST API is only reachable from local Mac (gateway runs on `localhost:5055`). For Databricks jobs to ingest live data, either: (a) gateway must be tunnelled, or (b) ingestion runs locally and results are written to S3/Delta. Architecture for production ingestion path not finalised. **Serverless compute also has no outbound internet access in this workspace — Yahoo Finance unreachable from serverless. Use regular cluster for notebook-based ingestion, or Databricks Connect for local execution.** | Open |

#### 6.2 Phase 3 Design (Not Yet Started)

| # | Issue | Status |
|---|---|---|
| OQ-4 | Feature store table schema not yet designed. Partitioning strategy (by symbol? by date? by interval?) not decided. | Open |
| OQ-5 | Batch vs streaming feature path split: which features go on which path, and how the point-in-time join is implemented. | Open |
| OQ-6 | FeatureScaler: which scaling strategy per feature type (standard / robust / min-max), whether scaling is regime-aware, and how scaler state is persisted for inference. | Open |
| OQ-7 | Intraday stream activation (1h/4h): config exists but disabled. When to enable and what infrastructure changes are needed. | Open |

#### 6.3 Phase 4 Design (Future)

| # | Issue | Status |
|---|---|---|
| OQ-8 | MLflow experiment naming convention not finalised. | Open |
| OQ-9 | Signal publishing transport not decided: Delta CDC, REST polling, WebSocket, or Telegram bot. Will depend on consumer latency requirements. | Open |
| OQ-10 | Catalyst Sentiment Score LLM prompts and Loughran-McDonald sentiment lexicon integration not designed. | Open |
| OQ-11 | PaperTracker performance threshold: what Sharpe / Sortino / MDD must be demonstrated before moving to live execution? Not defined. | Open |

#### 6.4 Known Technical Debt

| # | Debt | Priority |
|---|---|---|
| TD-1 | `tickers.csv` per-ticker `history_start` override: implemented but not tested at scale with many symbols. | Medium |
| TD-2 | Holiday calendar currently static (hardcoded 2010–2040). Should be driven by exchange calendar library. | Low |
| TD-3 | No circuit breaker on IBKR rate limits — if gateway is under load, ingestion retries are not throttled. | Medium |
| TD-4 | `data_quality_rules.yml` threshold values are environment-specific but currently shared — no dev vs prod split for thresholds. | Low |

---

### 7. Handoff Protocol

#### Responsibility Split

- **Claude Project (TradeAnalytics — ML Trading Pipeline):** Design authority.
  All architectural decisions, ML strategy, LLM agent behaviour, pipeline design,
  and high-level planning are owned here.
- **Claude Code (CLI):** Implementation authority.
  All code writing, debugging, refactoring, file edits, and testing happen here.

#### Claude Code Standing Instructions

This file (`CLAUDE.md`) is the shared source of truth between Claude Code and the Claude Project.

**Claude Code MUST:**
- Read this file fully at the start of every session
- Update the relevant section of this file after any implementation decision, architectural change, or significant refactor
- Never let this file become stale — if something planned is now built, update it
- If an architectural decision was changed during implementation, document it here with the reason so the Claude Project is informed on next review
- If a new open question or risk emerges during coding, add it to Section 6

**Claude Code must NOT:**
- Make architectural decisions unilaterally — flag them here and defer to the Claude Project
- Silently change patterns or conventions without updating Section 5
- Introduce hardcoded catalog names, schema names, credentials, or thresholds anywhere
- Instantiate `BronzeRecord` dataclass during ingestion — records flow as plain Python dicts
- Infer Spark schema from Python dicts — always read from `spark.table().schema` or use explicit `StructType`
- Use Yahoo Finance as a data source in any non-test code path
- Place LLM calls in any synchronous execution path
- Introduce any new component without a corresponding ABC, Registry entry, and YAML config entry
- Use UPDATE, DELETE, or MERGE on any append-only Bronze Delta table
