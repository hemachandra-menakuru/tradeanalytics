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
  - Layer 2: `BronzeWriter._bulk_classify()` — bulk pre-fetch + in-memory classify (new / amend / skip)
  - Layer 3: Silver `ROW_NUMBER` window — exactly one record per key downstream
- Rejected records go to `bronze.market_data_rejected` — queryable, reprocessable
- **Records flow as plain dicts throughout the pipeline** — `BronzeRecord` is NOT instantiated
  during ingestion. All audit fields are stamped in `validator._enrich_record()`

### Data source (locked)
- IBKR Client Portal REST API = **primary for all Phases 1–4** (runs on `localhost:5055`)
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
| 2 | Bronze Ingestion | ⏳ Code complete (249 tests passing), Databricks deployment pending |
| 3 | Silver (Feature Engineering) | Not started |
| 4 | Gold + Signal Platform | Not started |
| 4b | Signal sharing (Telegram/API) | Not started |
| 5a | HC's execution bot | Not started |
| 5b | Friends' execution bots | Not started |
| 6 | Monetisation | Not started (legal review required first) |

### Phase 2 remaining tasks (in order)
1. ✅ Bronze Delta tables created and schema-verified in Unity Catalog
2. ✅ All code fixes applied (see Section 7)
3. ⬜ Add DABs job definition to `databricks.yml` — job `bronze_daily_ingestion`, schedule 7pm Mon–Fri Eastern, policy ID `001B2429FBD0E8AD`
4. ⬜ Upload IBKR credentials to Databricks secret scope (`tradeanalytics`)
5. ⬜ Deploy bundle to Databricks workspace
6. ⬜ End-to-end smoke test on Databricks
7. ⬜ Phase 2 docs: `TradeAnalytics_Phase2_Data_Ingestion_Guide.docx`
8. ⬜ PR: merge `feature/phase2-data-ingestion` → main

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
│   ├── config/
│   │   └── config_loader.py     # ConfigLoader + ConfigNode
│   ├── ingestion/
│   │   ├── base/
│   │   │   └── market_data_provider.py   # MarketDataProvider ABC
│   │   ├── factory/
│   │   │   └── provider_factory.py       # MarketDataFactory (registry)
│   │   ├── jobs/
│   │   │   └── bronze_ingestion_job.py   # BronzeIngestionJob + JobRunSummary
│   │   ├── models/
│   │   │   ├── bronze_record.py          # BronzeRecord (59 fields, 8 groups)
│   │   │   ├── ingestion_mode.py         # IngestionMode enum, FetchPlan, IngestionWatermark
│   │   │   └── ingestion_planner.py      # IngestionPlanner (8 priority modes)
│   │   ├── providers/
│   │   │   ├── ibkr_provider.py          # IBKRProvider (primary, live-verified)
│   │   │   └── yahoo_provider.py         # YahooProvider (unit test fallback only)
│   │   ├── readers/
│   │   │   └── ticker_reader.py          # TickerReader + TickerInfo
│   │   ├── validation/
│   │   │   ├── models.py                 # ValidationSummary, ValidationResult
│   │   │   ├── rule_engine.py            # RuleEngine (18 rules from YAML)
│   │   │   └── validator.py              # DataQualityValidator
│   │   └── writers/
│   │       ├── bronze_writer.py          # BronzeWriter (bulk pre-fetch, local + Spark)
│   │       └── watermark_manager.py      # WatermarkManager (local + Spark)
│   └── reference/
│       ├── tickers.csv                   # active ticker list
│       └── data_quality_rules.yml        # shared rules (thresholds from stream config)
├── notebooks/
│   └── bronze_daily_ingestion.py         # Databricks entry point
├── databricks.yml                        # DABs bundle definition
└── tests/                                # 249 tests passing
```

---

## 5. Component Details & Key Patterns

### ConfigLoader (`src/config/config_loader.py`)
- Singleton, cached per environment
- Load order: `dev.yml` → `sources.yml` → stream YAMLs (auto-discovered) → `risk.yml` → `logging.yml` → `.env` → env vars
- Returns `ConfigNode` with dot-notation access: `config.sources.primary`, `config.daily.table`
- Key method: `ConfigLoader.load(environment="dev")`, `ConfigLoader.reset()` (tests only)
- Catalog read from: `config.databricks.catalog` (value: `"tradeanalytics"`)

### MarketDataProvider ABC (`src/ingestion/base/market_data_provider.py`)
- Abstract methods: `get_historical()`, `get_latest_quote()`, `get_options_chain()`, `is_market_open()`, `health_check()`
- Abstract properties: `provider_name`, `supports_options`, `supports_realtime`, `supported_intervals`
- All providers return standard OHLCV dict (see file docstring for full schema)
- Exceptions: `ProviderConnectionError`, `ProviderAuthError`, `ProviderDataError`, `ProviderNotSupportedError`
- `get_historical()` catches `ProviderAuthError` and `ProviderConnectionError` → returns `[]` gracefully

### MarketDataFactory (`src/ingestion/factory/provider_factory.py`)
- Registry pattern: `MarketDataFactory.register("ibkr", IBKRProvider)`
- Both providers self-register at import time (bottom of their files)
- Key methods: `get_provider(config)`, `get_fallback_provider(config)`, `get_provider_by_name(name, config)`
- Config keys: `config.sources.primary` and `config.sources.fallback`

### IBKRProvider (`src/ingestion/providers/ibkr_provider.py`)
- Uses Client Portal REST API (no IB Gateway, no TWS)
- Base URL: `https://localhost:5055/v1/api`
- Self-signed SSL cert → `ssl.CERT_NONE`
- `conid` cache avoids repeated contract searches
- IBKR bar format: `o/h/l/c/v` + timestamp in milliseconds UTC
- Period mapping: `_days_to_period()` converts days → IBKR period string (`{N}d`, `{N}w`, `{N}m`)
- Live-verified: SPY + AAPL, 3 records returned correctly (week of 2026-06-16)
- Graceful degradation: `get_historical()` returns `[]` on auth/connection errors
- Stamps `ingested_by="ibkr_provider_v1"`, `fetch_attempt_count=1` on every record
- Stamps `session_open=None`, `session_close=None` (not available from history endpoint)
- Live tests guarded by `@requires_live_gateway` — skipped when gateway unauthenticated

### YahooProvider (`src/ingestion/providers/yahoo_provider.py`)
- **Unit test fallback only — never production**
- Stamps `session_open=None`, `session_close=None` (not available from Yahoo)
- `get_historical()` returns `[]` on errors, never raises

### BronzeIngestionJob (`src/ingestion/jobs/bronze_ingestion_job.py`)
- Orchestrates: IngestionPlanner → provider fetch → DataQualityValidator → BronzeWriter → WatermarkManager
- `stream_interval = self._stream_cfg.intervals[0]` defined BEFORE ticker loop (prevents NameError in error handler)
- Passes `plan.ingestion_type` to `validator.validate_batch()` so it flows to every record
- `get_record_count()` used for accurate watermark `record_count` in both local and Spark modes
- Error handler uses `stream_interval` (not undefined `interval`) for watermark failure updates

### DataQualityValidator (`src/ingestion/validation/validator.py`)
- `validate_batch(symbol, interval, batch_id, raw_records, pipeline_version, ingestion_type)`
- `_enrich_record()` stamps ALL missing audit fields onto every clean/flagged record:
  - **Group 7 (Data Quality):** `data_quality_flag`, `data_quality_reasons` (JSON string), `data_completeness_pct`
  - **Group 8 (Pipeline Audit):** `batch_id`, `pipeline_version`, `record_version` (default 1), `ingested_at` (UTC ISO), `ingestion_type`, `is_amended` (False), `amendment_reason` (None), `supersedes_batch` (None), `ingested_by`, `fetch_attempt_count`
  - **Group 4 (Session):** `session_open` (None), `session_close` (None)
- `_compute_completeness()` — core fields weighted 80%, optional 20%
- `provider_nullable_fields` in `daily.yml` is documented but not yet wired in (Phase 3 TODO)

### RuleEngine (`src/ingestion/validation/rule_engine.py`)
- 18 rules loaded from `src/reference/data_quality_rules.yml`
- Thresholds injected from stream config at runtime (never hardcoded)
- YAML-driven: adding a rule = 3 steps (rule definition + register + config), zero code changes

### BronzeWriter (`src/ingestion/writers/bronze_writer.py`)
- **Bulk pre-fetch deduplication** — `_bulk_classify()` fires ONE Spark query per `write_batch()` call
- `_spark_bulk_fetch()` — single SQL with ROW_NUMBER window, fetches all keys in one round-trip
- `_local_bulk_fetch()` — one pass over in-memory store (O(N))
- Amendment detection compares: `open`, `high`, `low`, `close`, `volume`, `adj_close` (tolerance 0.001)
  - `adj_close` included to catch retroactive split/dividend adjustments
- `get_record_count(symbol, interval, table_name)` — real count for watermark (not hardcoded 0)
- Catalog default: `"tradeanalytics"`

### WatermarkManager (`src/ingestion/writers/watermark_manager.py`)
- Uses MERGE (not append-only) — tracks current state per symbol+interval
- `min(earliest, existing)` / `max(latest, existing)` — never moves dates in wrong direction
- Watermark table: `tradeanalytics.bronze.ingestion_watermark_daily`

### IngestionPlanner (`src/ingestion/models/ingestion_planner.py`)
- 8 modes in priority order: `FORCE_RELOAD > RESTATEMENT > EXPLICIT_DATE_RANGE > HISTORY_EXTENSION > INITIAL_LOAD > GAP_FILL > INCREMENTAL > NO_OP`
- Holiday calendar: `_US_MARKET_HOLIDAYS` covers **2010–2040** (NYSE schedule)
  - TODO Phase 3: migrate to `tradeanalytics.reference.market_holidays` Unity Catalog table
- Per-ticker `history_start` in `tickers.csv` overrides global `lookback_years`

### BronzeRecord (`src/ingestion/models/bronze_record.py`)
- **59 fields across 8 groups** — matches `tradeanalytics.bronze.market_data_daily` exactly
- **NOT instantiated during ingestion** — records flow as plain dicts, all fields stamped by validator
- `BronzeRecord.amend()` used only for explicit restatement runs
- Groups: Identity · Raw Price · Adjusted Price · Session Context · Corporate Actions · Market Conditions · Data Quality · Pipeline Audit
- Amendment fields (`is_amended`, `amendment_reason`, `supersedes_batch`) are in Group 8 (Pipeline Audit), not Group 7

---

## 6. Databricks Notebook (`notebooks/bronze_daily_ingestion.py`)

Key structure (cells in order):
```python
# Cell 1: Install deps BEFORE any imports
subprocess.run(["pip", "install", "python-dotenv", "yfinance", "--quiet"])

# Cell 2: Logging setup

# Cell 3: Add bundle path to sys.path
bundle_path = "/Workspace/Users/handh.stocks@gmail.com/.bundle/tradeanalytics/dev/files"
sys.path.insert(0, bundle_path)

# Cell 4: Read widget parameters
symbols    = [s.strip() for s in dbutils.widgets.get("symbols").split(",") if s.strip()] or None
dry_run    = dbutils.widgets.get("dry_run").lower() == "true"
as_of_date = date.fromisoformat(dbutils.widgets.get("as_of_date")) if as_of_date_param else None

# Cell 5: Load secrets
os.environ["IBKR_ACCOUNT_ID"] = dbutils.secrets.get("tradeanalytics", "IBKR_ACCOUNT_ID")

# Cell 6: Load config
ConfigLoader.reset()
config = ConfigLoader.load(environment=os.getenv("ENVIRONMENT", "dev"))

# Cell 7: Register providers — use config (IBKR primary, Yahoo fallback)
# NOTE: Yahoo override REMOVED — notebook now uses IBKR as configured
MarketDataFactory.register("ibkr",  IBKRProvider)
MarketDataFactory.register("yahoo", YahooProvider)

# Cell 8: Run job
job = BronzeIngestionJob(config=config, stream_name="daily", spark=spark)
summary = job.run(symbols=symbols, as_of_date=as_of_date, dry_run=dry_run)

# Cell 9: Log results + display DataFrame

# Cell 10: Raise on failure
```

### DABs job (to be added to `databricks.yml`)
```yaml
job_name: bronze_daily_ingestion
schedule: "0 0 19 * * ?"   # 7pm Eastern Mon–Fri
cluster_policy_id: 001B2429FBD0E8AD
status: PAUSED              # manual trigger until smoke test passes
```

---

## 7. All Fixes Applied This Session

| Fix | File | Description |
|-----|------|-------------|
| N+1 Spark storm | `bronze_writer.py` | `_bulk_classify()` replaces per-record `_spark_get_existing()` |
| `adj_close` amendment detection | `bronze_writer.py` | Retroactive split adjustments now detected |
| `record_count=0` in Spark | `bronze_ingestion_job.py` | Uses `writer.get_record_count()` |
| `pipeline_version` param removed | `bronze_writer.py` | Removed unused param from `write_batch()` |
| Notebook Yahoo override | `bronze_daily_ingestion.py` | `config._data` mutation removed — IBKR used as configured |
| IBKR graceful degradation | `ibkr_provider.py` | Returns `[]` on auth/connection errors |
| Live gateway tests guarded | `test_provider_abstraction.py` | `@requires_live_gateway` skips when unauthenticated |
| `ingested_by` + `fetch_attempt_count` | `ibkr_provider.py` | Stamped on every record |
| `session_open/session_close` | `ibkr_provider.py`, `yahoo_provider.py` | Both providers stamp `None` |
| 11 missing audit fields | `validator.py` | All stamped in `_enrich_record()` |
| `ingestion_type` flow | `bronze_ingestion_job.py`, `validator.py` | Flows job → validator → every record |
| `interval` NameError | `bronze_ingestion_job.py` | `stream_interval` defined before ticker loop |
| Catalog rename | 8 files | `handh_trade` → `tradeanalytics` everywhere |
| Holiday calendar | `ingestion_planner.py` | `_US_MARKET_HOLIDAYS` covers 2010–2040 |
| Amendment fields group | `bronze_record.py` | Moved from Group 7 → Group 8 (Pipeline Audit) |
| Dead config documented | `config/streams/daily.yml` | `provider_nullable_fields` marked Phase 3 TODO |

---

## 8. Config Key Reference

```python
config.databricks.catalog          # "tradeanalytics"
config.databricks.schemas.bronze   # "bronze"
config.sources.primary             # "ibkr"
config.sources.fallback            # "yahoo"
config.sources.ibkr.base_url       # "https://localhost:5055/v1/api"
config.sources.ibkr.account_id     # from env IBKR_ACCOUNT_ID
config.daily.enabled               # True
config.daily.table                 # "market_data_daily"
config.daily.rejected_table        # "market_data_rejected"
config.daily.watermark_table       # "ingestion_watermark_daily"
config.daily.intervals             # ["1d"]
config.daily.validation.thresholds.*   # thresholds for daily stream
```

---

## 9. Delta Tables

| Table | Full Name | Fields | Type |
|-------|-----------|--------|------|
| Main bronze | `tradeanalytics.bronze.market_data_daily` | 59 | Append-only Delta |
| Rejected | `tradeanalytics.bronze.market_data_rejected` | 18 | Append-only Delta |
| Watermark | `tradeanalytics.bronze.ingestion_watermark_daily` | 10 | MERGE Delta |

S3 location: `s3://handh-trade-refined-use1/bronze/`
All 3 tables schema-verified — perfect field match with BronzeRecord.to_dict() and RejectedRecord.to_dict()

---

## 10. S3 Buckets

| Bucket | Purpose |
|--------|---------|
| `handh-trade-raw-use1` | Raw source data |
| `handh-trade-refined-use1` | Delta tables (bronze/silver/gold) |
| `handh-trade-mlflow-use1` | MLflow artifacts |
| `handh-trade-dbx-root-use1` | Databricks workspace root |

---

## 11. Known Issues & Deferred Items

### Phase 3 TODO: `provider_nullable_fields`
`config/streams/daily.yml` documents which fields each provider cannot supply (e.g. Yahoo cannot supply `vwap`, `trade_count`). This config is not yet consumed by `DataQualityValidator`. Wire it into `_enrich_record()` in Phase 3 to prevent false DQ flags for provider-declared nulls.

### Phase 3 TODO: Holiday calendar reference table
`_US_MARKET_HOLIDAYS` (2010–2040) is hardcoded in `ingestion_planner.py`. Migrate to `tradeanalytics.reference.market_holidays` Unity Catalog table in Phase 3.

### Phase 3 TODO: BronzeRecord instantiation
Records flow as plain dicts in Phase 2. In Phase 3, refactor validator to instantiate `BronzeRecord` from raw provider dicts → `to_dict()` for Silver compatibility. All audit fields currently stamped manually in `_enrich_record()`.

### Phase 3 TODO: `additional_rules` parsing
`daily.yml` defines `additional_rules` as a list of dicts. Verify `RuleEngine` processes them correctly (not as plain strings).

---

## 12. Phase 3 Preview (Silver — Feature Engineering)

Planned ABCs: `IndicatorEngine`, `FeatureEngineer`, `FeatureScaler`, `RegimeDetector`
Key components: Hurst Exponent (weekly routing), HMM (bar-level feature), feature store (`tradeanalytics.feature_store.*`)
Intraday stream activation: 1h/4h

---

## 13. Session Start Checklist

When starting a new session:
1. Upload this `CLAUDE.md` file first
2. Confirm with HC: any changes since last session? (new commits, config changes, test failures)
3. State the immediate task — next up: DABs job definition in `databricks.yml`
4. Run `python -m pytest tests/ -q` to confirm 249 tests still passing before any changes
