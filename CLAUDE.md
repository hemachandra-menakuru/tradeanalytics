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
| Conda env | `tradeanalytics` (Python 3.11) |
| Repo path (local) | `/Users/hemachandra/projects/tradeanalytics` (shell alias: `~/pr/tradeanalytics`) |
| Active branch | `feature/phase2-data-ingestion` |
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
| 2 | Bronze Ingestion | ✅ Complete — smoke test passed, 2,636 SPY records in Delta |
| 3 | Silver (Feature Engineering) | Not started |
| 4 | Gold + Signal Platform | Not started |
| 4b | Signal sharing (Telegram/API) | Not started |
| 5a | HC's execution bot | Not started |
| 5b | Friends' execution bots | Not started |
| 6 | Monetisation | Not started (legal review required first) |

### Phase 2 remaining tasks
1. ✅ All code + infrastructure complete
2. ✅ Smoke test passed — 2,636 SPY records (2016-01-04 → 2026-06-18)
3. ⬜ Phase 2 docs: `TradeAnalytics_Phase2_Data_Ingestion_Guide.docx`
4. ⬜ PR: merge `feature/phase2-data-ingestion` → main

### Phase 3 teaching approach (locked)
- **Step by step — explain every concept before writing code**
- No assumptions about prior ML knowledge
- Explain what, why, and what the output means on real data
- HC says if something isn't clear — go back and re-explain
- Never rush ahead to code without concept explanation first

---

## 4. File Structure

```
tradeanalytics/
├── config/
│   ├── dev.yml                  # infrastructure (S3, Databricks, cluster)
│   ├── sources.yml              # provider settings (IBKR base_url=https://localhost:5055/v1/api)
│   ├── risk.yml
│   ├── logging.yml
│   └── streams/
│       ├── daily.yml            # Phase 2, active
│       ├── intraday.yml         # Phase 3, disabled
│       └── tick.yml             # Phase 5, disabled
├── src/
│   ├── config/config_loader.py
│   ├── ingestion/
│   │   ├── base/market_data_provider.py    # MarketDataProvider ABC
│   │   ├── factory/provider_factory.py     # MarketDataFactory (registry)
│   │   ├── jobs/bronze_ingestion_job.py    # BronzeIngestionJob + JobRunSummary
│   │   ├── models/
│   │   │   ├── bronze_record.py            # BronzeRecord (59 fields, 8 groups)
│   │   │   ├── ingestion_mode.py           # IngestionMode, FetchPlan, IngestionWatermark
│   │   │   └── ingestion_planner.py        # IngestionPlanner (8 modes)
│   │   ├── providers/
│   │   │   ├── ibkr_provider.py            # IBKRProvider (primary, live-verified)
│   │   │   └── yahoo_provider.py           # YahooProvider (unit test fallback only)
│   │   ├── readers/ticker_reader.py
│   │   ├── validation/
│   │   │   ├── models.py
│   │   │   ├── rule_engine.py              # RuleEngine (18 rules from YAML)
│   │   │   └── validator.py               # DataQualityValidator
│   │   └── writers/
│   │       ├── bronze_writer.py            # BronzeWriter (bulk pre-fetch, explicit schema)
│   │       └── watermark_manager.py        # WatermarkManager (explicit StructType schema)
│   └── reference/
│       ├── tickers.csv
│       └── data_quality_rules.yml
├── notebooks/bronze_daily_ingestion.py     # Databricks entry point
├── databricks.yml                          # DABs bundle (job ID: 174217366433843)
└── tests/                                  # 249 tests passing
```

---

## 5. Key Component Patterns

### BronzeWriter — critical Spark patterns
```python
# _spark_append() — reads Delta schema FIRST, then aligns types
table_schema = self._spark.table(full_table).schema  # primary
# Fallback: DESCRIBE TABLE if schema empty (empty table edge case)
# Type alignment: DateType→datetime.date, Double→float, Long/Int→int, Bool→bool
# Prevents: CANNOT_DETERMINE_TYPE + FIELD_DATA_TYPE_UNACCEPTABLE

# _bulk_classify() — ONE query per write_batch() call
# _spark_bulk_fetch() — ROW_NUMBER window in single SQL
# get_record_count() — real count for watermark (not 0)
```

### WatermarkManager — Spark pattern
```python
# _spark_upsert_watermark() — explicit StructType with DateType
# Converts earliest_date/latest_date strings → datetime.date before createDataFrame()
```

### DataQualityValidator — audit field stamping
```python
# _enrich_record() stamps ALL audit fields:
# batch_id, pipeline_version, record_version(=1), ingested_at(UTC), ingestion_type,
# is_amended(False), amendment_reason(None), supersedes_batch(None),
# data_completeness_pct(computed), session_open(None), session_close(None),
# ingested_by, fetch_attempt_count
# data_completeness_pct=60 for IBKR (correct — IBKR supplies ~60% of optional fields)
```

### BronzeIngestionJob — key patterns
```python
# stream_interval defined BEFORE ticker loop (prevents NameError in error handler)
# run() accepts start_date/end_date override params
# Passes plan.ingestion_type to validator.validate_batch()
```

### Notebook — Cell 7 (provider registration)
```python
# NO config._data override — IBKR used as primary per config
MarketDataFactory.register("ibkr",  IBKRProvider)
MarketDataFactory.register("yahoo", YahooProvider)
# Widgets: symbols, dry_run, as_of_date, start_date, end_date
```

---

## 6. Databricks Job

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

## 7. Smoke Test Results (2026-06-22)

| Check | Result |
|-------|--------|
| Job succeeded | ✅ 10m 16s |
| Records written | ✅ 2,636 SPY (2016-01-04 → 2026-06-18) |
| record_version | ✅ 1 |
| ingestion_type | ✅ backfill |
| data_quality_flag | ✅ false |
| data_completeness_pct | ✅ 60 |
| batch_id | ✅ populated |
| ingested_at | ✅ UTC timestamp |
| ingested_by | ⚠️ NULL (see Known Issues) |
| Delta lineage | ✅ 2 upstream, 2 downstream tables |

---

## 8. Known Issues & Technical Debt

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

### ⚠️ Duplicate records from failed smoke test runs
Multiple rows for some dates from repeated failed runs. Bronze is append-only by design.
Silver ROW_NUMBER dedup handles this. Do not DELETE from Bronze.

### Phase 3 Technical Debt (fix during Phase 3)
1. **BronzeRecord not instantiated** — records flow as dicts, all defaults bypassed.
   Real fix: `validator` creates `BronzeRecord.from_dict()` → typed `to_dict()` output.
   Removes need for manual type alignment in `_spark_append()` (560 lines → 3 lines).
2. **`_process_ticker()` too long** (~150 lines) — split before adding Phase 4 hooks.
3. **`provider_nullable_fields`** in `daily.yml` not consumed by validator — wire up.
4. **Holiday calendar** — migrate to `tradeanalytics.reference.market_holidays` Delta table.
5. **`additional_rules`** in config — verify RuleEngine handles list-of-dicts correctly.

---

## 9. Config Key Reference

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

## 10. Delta Tables

| Table | Full Name | Records |
|-------|-----------|---------|
| Main bronze | `tradeanalytics.bronze.market_data_daily` | 2,636 SPY |
| Rejected | `tradeanalytics.bronze.market_data_rejected` | 0 |
| Watermark | `tradeanalytics.bronze.ingestion_watermark_daily` | 1 (SPY) |

S3: `s3://handh-trade-refined-use1/bronze/`

---

## 11. Phase 3 Preview (Silver — Feature Engineering)

Step-by-step teaching order:
1. What is a feature? (EMA, RSI, MACD on real SPY data — visualise first)
2. What is a label? (forward returns — what are we predicting and why?)
3. What is the feature matrix? (rows=dates, cols=features — read before training)
4. Train first XGBoost on SPY
5. Walk-forward validation — out-of-sample IC
6. Does it actually have edge?

Planned ABCs: `IndicatorEngine`, `FeatureEngineer`, `FeatureScaler`, `RegimeDetector`
Feature store: `tradeanalytics.feature_store.*`

---

## 12. Session Start Checklist

1. Upload this `CLAUDE.md` file first
2. Confirm: any changes since last session?
3. Run `python -m pytest tests/ -q` — confirm 249 tests passing
4. State the immediate task
5. **Next up:** Phase 2 docs → PR → merge → Phase 3
