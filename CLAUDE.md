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
| 2 | Bronze Ingestion | ✅ Complete — IBKR smoke test passed 2026-06-25, source=ibkr confirmed |
| 3 | Silver (Feature Engineering) | Not started |
| 4 | Gold + Signal Platform | Not started |
| 4b | Signal sharing (Telegram/API) | Not started |
| 5a | HC's execution bot | Not started |
| 5b | Friends' execution bots | Not started |
| 6 | Monetisation | Not started (legal review required first) |

### Phase 2 remaining tasks
1. ✅ All code + infrastructure complete
2. ✅ Smoke test passed — IBKR confirmed, source=ibkr, 29 SPY records (2026-05-13 → 2026-06-24)
3. ✅ Phase 2 docs: `TradeAnalytics_Phase2_Data_Ingestion_Guide.docx` — created 2026-06-25
4. ✅ PR: merge `feature/phase2-data-ingestion` → main

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
├── TradeAnalytics_Phase2_Data_Ingestion_Guide.docx  # Phase 2 operational guide
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
# data_completeness_pct=60-64 for IBKR (correct — IBKR supplies ~60-64% of optional fields)
```

### BronzeIngestionJob — key patterns
```python
# stream_interval defined BEFORE ticker loop (prevents NameError in error handler)
# run() accepts start_date/end_date override params
# Passes plan.ingestion_type to validator.validate_batch()
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

## 7. Smoke Test Results

### Run 1 — 2026-06-22 (Yahoo — stale cached notebook, invalid)
Wrote 2,636 SPY records with `source=yahoo`. Tables were truncated 2026-06-25.

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
| Main bronze | `tradeanalytics.bronze.market_data_daily` | 29 SPY (source=ibkr, 2026-05-13→2026-06-24) |
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

#### Phase 2 — Bronze Data Ingestion (Code complete; smoke test run with wrong source)

**Branch:** `feature/phase2-data-ingestion`
**Tests:** 249 passing
**Status:** Code complete. Deployment done. Smoke test succeeded but with wrong data source — a stale cached notebook with a Yahoo override ran instead of the current notebook, writing 2,636 SPY records with `source=yahoo` and `pipeline_version=unknown` to `tradeanalytics.bronze.market_data_daily`.

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

**Immediate next steps before Phase 2 can be closed (in order):**
1. TRUNCATE `tradeanalytics.bronze.market_data_daily` and `ingestion_watermark_daily` (Yahoo data is not production)
2. Verify deployed notebook in Databricks has no Yahoo override
3. Re-run smoke test with IBKR as primary — confirm `source=ibkr` in results
4. Create `TradeAnalytics_Phase2_Data_Ingestion_Guide.docx`
5. Merge `feature/phase2-data-ingestion` → `main` via PR

**Known open design issue — `start_date`/`end_date` widget parameters:**
Widget parameters passed to `job.run()` did not prevent `INITIAL_LOAD` mode — IBKR returned full 10-year history instead of 2 days. Recommended fix is Option A: post-fetch Python filtering in `IBKRProvider` before data is passed to the pipeline. This has been designed but not yet implemented.

#### CLAUDE.md

`CLAUDE.md` exists in the repo root on `feature/phase2-data-ingestion`. It is the canonical session-start context document for Claude Code. Raw URL:
`https://raw.githubusercontent.com/hemachandra-menakuru/tradeanalytics/feature/phase2-data-ingestion/CLAUDE.md`

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

```
/Users/hemachandra/projects/tradeanalytics   — local repo root
├── CLAUDE.md                                — canonical session-start context (Claude Code reads this first)
├── databricks.yml                           — DABs pipeline definition
├── src/                                     — all Python source
│   ├── ingestion/                           — Phase 2 components
│   ├── reference/                           — data_quality_rules.yml
│   └── ...
├── config/                                  — all YAML config
├── tests/                                   — pytest suite (249 passing)
└── notebooks/                              — Databricks notebooks
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
| OQ-3 | IBKR Client Portal REST API is only reachable from local Mac (gateway runs on `localhost:5055`). For Databricks jobs to ingest live data, either: (a) gateway must be tunnelled, or (b) ingestion runs locally and results are written to S3/Delta. Architecture for production ingestion path not finalised. | Open |

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
