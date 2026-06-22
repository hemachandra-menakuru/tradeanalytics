# CLAUDE.md — TradeAnalytics Session Context

> **HOW TO USE:** At the start of every new Claude session, paste this file.
> Claude will read it and pick up exactly where we left off — code patterns,
> decisions, current state, and known issues all included.
> Keep this file updated as the project evolves.

---

## 1. Project Identity

| Item | Value |
|------|-------|
| Project | TradeAnalytics — Signal-as-a-Service ML trading platform |
| GitHub | `hemachandra-menakuru/tradeanalytics` (private) |
| Claude Project | `tradeanalyticsanalytics` |
| AWS Account | `311925399625` (tradeanalyticsanalytics), region `us-east-1` |
| Databricks | `dbc-bf0075e6-07aa.cloud.databricks.com`, workspace `handh-dev` |
| Unity Catalog | `tradeanalytics` (NOT `tradeanalytics` — this was corrected) |
| Bronze schema | `tradeanalytics.bronze` |
| DABs profile | `handh-trade-aws` |
| IBKR account | `U5498892`, gateway at `~/dev/tools/ibkr/clientportal.gw`, port `5055` |
| Conda env | `tradeanalytics` (Python 3.11) |
| Repo path (local) | `/Users/hemachandra/projects/tradeanalytics` |
| Active branch | `feature/phase2-data-ingestion` |

---

## 2. Architecture Principles (non-negotiable)

### Universal component pattern
Every component follows: `ABC → Registry → Factory → Config`
Adding a new component = implement ABC + register (one line) + update YAML. Zero other changes.

### Eight architectural principles
Loose coupling (ABCs only across boundaries) · Single responsibility · Plug-and-play via registry · Config-driven (YAML not if/else) · Abstraction layers for every external dependency · No vendor lock-in · Horizontal scale (stateless + Spark-native + Delta for state) · Fail-safe (health checks, fallbacks, circuit breakers at every external boundary)

### Bronze layer rules (absolute)
- **Append-only** — NEVER UPDATE / DELETE / MERGE on bronze tables
- Amendments are new records with `record_version` incremented
- Three-layer deduplication:
  - Layer 1: `IngestionPlanner` — correct date range (skip if up to date)
  - Layer 2: `BronzeWriter._classify_record()` — new / amend / skip per record
  - Layer 3: Silver `ROW_NUMBER` window — exactly one record per key downstream
- Rejected records go to `bronze.market_data_rejected` — queryable, reprocessable

### Data source (locked)
- IBKR Client Portal REST API = **primary for all Phases 1–4** (runs on `localhost:5055`)
- Yahoo Finance = **unit test fallback only**, never production
- Databricks clusters cannot reach `localhost` → Yahoo is used for smoke tests on Databricks
- Phase 5 (live trading): IB Gateway on EC2 + `ib_async`

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
| 2 | Bronze Ingestion | ⏳ Code complete (249 tests passing), Databricks deployment in progress |
| 3 | Silver (Feature Engineering) | Not started |
| 4 | Gold + Signal Platform | Not started |
| 4b | Signal sharing (Telegram/API) | Not started |
| 5a | HC's execution bot | Not started |
| 5b | Friends' execution bots | Not started |
| 6 | Monetisation | Not started (legal review required first) |

### Phase 2 remaining tasks (in order)
1. ✅ Bronze Delta tables created in Unity Catalog
2. ✅ DABs job definition added to `databricks.yml` (job ID `174217366433843`)
3. ✅ IBKR credentials uploaded to Databricks secret scope (`tradeanalytics` scope)
4. ✅ Bundle deployed to Databricks
5. ⏳ **End-to-end smoke test** — IN PROGRESS, blocked (see Known Issues)
6. ⬜ Phase 2 docs: `TradeAnalytics_Phase2_Data_Ingestion_Guide.docx`
7. ⬜ PR: merge `feature/phase2-data-ingestion` → main

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
│   │   │   ├── bronze_record.py          # BronzeRecord, RecordInterval
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
│   │       ├── bronze_writer.py          # BronzeWriter (local + Spark)
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
- **IMPORTANT:** In the notebook, `config._data["sources"]["primary"] = "yahoo"` is used to override provider for Databricks smoke testing

### MarketDataProvider ABC (`src/ingestion/base/market_data_provider.py`)
- Abstract methods: `get_historical()`, `get_latest_quote()`, `get_options_chain()`, `is_market_open()`, `health_check()`
- Abstract properties: `provider_name`, `supports_options`, `supports_realtime`, `supported_intervals`
- All providers return standard OHLCV dict (see file docstring for full schema — ~40 fields)
- Exceptions: `ProviderConnectionError`, `ProviderAuthError`, `ProviderDataError`, `ProviderNotSupportedError`

### MarketDataFactory (`src/ingestion/factory/provider_factory.py`)
- Registry pattern: `MarketDataFactory.register("ibkr", IBKRProvider)`
- Both providers self-register at import time (bottom of their files)
- **In the notebook**, providers are registered manually after import to ensure they're available:
  ```python
  MarketDataFactory.register("ibkr",  IBKRProvider)
  MarketDataFactory.register("yahoo", YahooProvider)
  ```
- Key methods: `get_provider(config)`, `get_fallback_provider(config)`, `get_provider_by_name(name, config)`
- Config keys: `config.sources.primary` and `config.sources.fallback`

### IBKRProvider (`src/ingestion/providers/ibkr_provider.py`)
- Uses Client Portal REST API (no IB Gateway, no TWS)
- Base URL: `https://localhost:5055/v1/api`
- Self-signed SSL cert → `ssl.CERT_NONE`
- `conid` cache avoids repeated contract searches
- IBKR bar format: `o/h/l/c/v` + timestamp in milliseconds UTC
- Period mapping: `_days_to_period()` converts days → IBKR period string (`{N}d`, `{N}w`, `{N}m`)
- IBKR returns adjusted prices by default (`adj_factor=1.0`)
- Live-verified: SPY conid=756733, account U5498892, 4 bars returned correctly
- **Cannot be reached from Databricks clusters** (localhost-only)

### YahooProvider (`src/ingestion/providers/yahoo_provider.py`)
- Uses `yfinance` library
- `auto_adjust=True` so Close = adjusted close, `adj_factor=None` (not exposed by Yahoo)
- yfinance end date is exclusive → adds 1 day internally
- `get_options_chain()` raises `ProviderNotSupportedError`
- Used in Databricks smoke tests as Yahoo is internet-accessible from clusters

### BronzeRecord (`src/ingestion/models/bronze_record.py`)
- Dataclass with 8 field groups: Identity, Raw Price, Adjusted Price, Session Context, Corporate Actions, Market Conditions, Data Quality, Pipeline Audit
- `RecordInterval` string enum: `"1d"`, `"1h"`, `"4h"`, `"15m"`, `"5m"`, `"1m"`
- Unique key: daily = `symbol + date + interval`; intraday = `symbol + date + bar_time_utc + interval`
- `has_corporate_action` is always derived (never stale)
- `__post_init__` enforces all invariants immediately (fail fast)

### IngestionPlanner (`src/ingestion/models/ingestion_planner.py`)
- 8 priority modes (checked in order):
  1. `FORCE_RELOAD` — explicit config override
  2. `MARKET_CLOSED` — skip if market closed and not override
  3. `EXPLICIT_DATE_RANGE` — if `as_of_date` parameter passed
  4. `HISTORY_EXTENSION` — extend backwards if configured
  5. `INITIAL_LOAD` — no watermark exists (first run)
  6. `GAP_FILL` — gap > `max_gap_days`
  7. `NO_OP` — already up to date
  8. `INCREMENTAL` — normal daily update with `amendment_buffer_days` lookback
- Returns `FetchPlan` with `start_date`, `end_date`, `mode`, `batch_size_days`, `estimated_batches`

### BronzeIngestionJob (`src/ingestion/jobs/bronze_ingestion_job.py`)
- Orchestrates: watermark → plan → fetch → validate → write → update watermark
- Constructor: `BronzeIngestionJob(config, stream_name="daily", spark=spark)`
- `run(symbols=["SPY"], as_of_date=date(2026,6,20), dry_run=False)` → `JobRunSummary`
- Fetches in batches (`_fetch_in_batches`) to avoid API rate limits
- On provider health check failure → auto-switches to fallback provider
- `_process_ticker()` processes one symbol through full pipeline
- `JobRunSummary` tracks: results, skipped (NO_OP), failed, errors

### BronzeWriter (`src/ingestion/writers/bronze_writer.py`)
- `BronzeWriter(mode="local"|"spark", spark=spark, catalog="tradeanalytics", schema="bronze")`
- `write_batch(symbol, interval, batch_id, clean_records, rejected_records, stream_cfg)` → `BronzeWriteResult`
- Layer 2 dedup: `_classify_record()` → "new" / "amend" / "skip"
- Local mode: in-memory dict keyed by `table_name`
- Spark mode: `_spark_get_existing()` + `_spark_append()`
- **⚠️ KNOWN BUG (unfixed):** In Spark mode, `_classify_record()` calls `_spark_get_existing()` once per record, triggering 1 Spark job per record. With N records this creates N Spark jobs. Fix = `_bulk_get_existing()` (see Section 7).

### WatermarkManager (`src/ingestion/writers/watermark_manager.py`)
- `WatermarkManager(mode, spark, catalog, schema, watermark_table="ingestion_watermark_daily")`
- Tracks `earliest_date` and `latest_date` per `symbol + interval`
- Local mode: in-memory dict keyed by `(symbol, interval)`
- Spark mode: uses MERGE (not append-only — watermark tracks current state, unlike bronze tables)
- `update_watermark()` never moves `earliest_date` forward or `latest_date` backward (preserves widest range)

### DataQualityValidator (`src/ingestion/validation/validator.py`)
- `DataQualityValidator.for_stream(config, stream_name)` → creates validator with stream thresholds
- Tiered rules: thresholds injected at runtime from `data_quality_rules.yml`
- Returns `ValidationSummary` with `.writable_records` (passed + flagged) and `.rejected_records`

### RuleEngine (`src/ingestion/validation/rule_engine.py`)
- 18 rules loaded from `src/reference/data_quality_rules.yml`

### TickerReader (`src/ingestion/readers/ticker_reader.py`)
- Reads `src/reference/tickers.csv`
- `get_active_tickers(symbols=None, asset_classes=None, sectors=None)` → `List[TickerInfo]`
- `TickerInfo.effective_history_start` = `history_start or ipo_date or None` (falls back to global lookback)
- Cached after first load

---

## 6. Databricks Notebook (`notebooks/bronze_daily_ingestion.py`)

Key structure (cells in order):
```python
# Cell 1: Install deps BEFORE any imports (cluster libs install after cluster starts)
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

# Cell 7: Override provider to Yahoo (IBKR unreachable from Databricks clusters)
MarketDataFactory.register("ibkr",  IBKRProvider)
MarketDataFactory.register("yahoo", YahooProvider)
config._data["sources"]["primary"]  = "yahoo"
config._data["sources"]["fallback"] = "yahoo"

# Cell 8: Run job
job = BronzeIngestionJob(config=config, stream_name="daily", spark=spark)
summary = job.run(symbols=symbols, as_of_date=as_of_date, dry_run=dry_run)

# Cell 9: Log results + display DataFrame

# Cell 10: Raise on failure
```

### DABs job parameters (smoke test values in `databricks.yml`)
```yaml
symbols:    "SPY"
dry_run:    "false"
as_of_date: "2026-06-20"
```
Schedule: `0 0 19 * * ?` (7pm Eastern, Mon–Fri), currently `PAUSED`.

---

## 7. Known Issues & Bugs

### 🔴 BronzeWriter Spark N+1 problem (not yet fixed)
**Symptom:** Smoke test generates ~100 Spark jobs for a single symbol on a single date.

**Root cause:** `_classify_record()` calls `_spark_get_existing()` once per record. Each call runs `spark.sql()` = 1 Spark job. SPY daily data for one `as_of_date` returns ~100 bars (Yahoo returning more data than expected — possibly returning intraday bars or multiple dates).

**Fix required in `BronzeWriter`:**
Replace per-record `_spark_get_existing()` with a single bulk query upfront:

```python
def _bulk_get_existing(self, records, table_name) -> dict:
    """ONE Delta query for all incoming records. Returns dict keyed by (symbol, date, interval, bar_time_utc)."""
    full_table = f"{self._catalog}.{self._schema}.{table_name}"
    symbol   = records[0].get("symbol")
    interval = records[0].get("interval")
    dates    = list({r.get("date") for r in records if r.get("date")})
    dates_in = ", ".join(f"'{d}'" for d in dates)
    
    df = self._spark.sql(f"""
        SELECT * FROM (
            SELECT *, ROW_NUMBER() OVER (
                PARTITION BY symbol, date, interval, bar_time_utc
                ORDER BY record_version DESC
            ) AS rn
            FROM {full_table}
            WHERE symbol = '{symbol}' AND interval = '{interval}' AND date IN ({dates_in})
        ) WHERE rn = 1
    """)
    result = {}
    for row in df.collect():
        r = row.asDict()
        key = (r.get("symbol"), str(r.get("date")), r.get("interval"), str(r.get("bar_time_utc")))
        result[key] = r
    return result
```

Then in `write_batch()`:
```python
# Before the for loop:
existing_map = self._bulk_get_existing(clean_records, main_table) if self._mode == "spark" and clean_records else None

# In the for loop, pass existing_map to _classify_record()
action = self._classify_record(record, main_table, existing_map)
```

And merge new + amended into a single write call.

**Net result:** 2–3 Spark jobs total (1 bulk read + 1 write clean + 1 write rejected) instead of N+1.

**Tests:** 249 tests must still pass after this change. Run `pytest tests/ -x` before committing.

### 🟡 Yahoo returning unexpected record count
When smoke test ran with `as_of_date=2026-06-20` and `symbols=SPY`, ~100 Spark jobs fired.
SPY daily for a single date should return 1 record. Investigate whether Yahoo is returning:
- Multiple dates (date range instead of single date)
- Intraday bars
- Something else in the provider → IngestionPlanner interaction

Check `IngestionPlanner` mode for `as_of_date` parameter: it maps to `EXPLICIT_DATE_RANGE` (Priority 3) with `start_date = end_date = as_of_date`. So it should be 1 day. Check `_fetch_in_batches()` — it iterates from `plan.start_date` to `plan.end_date` in `batch_size_days` chunks. If `batch_size_days=1`, only 1 iteration. Verify in `daily.yml` what `batch_size_days` is set to.

---

## 8. Config Key Reference

```python
config.databricks.catalog          # "tradeanalytics"
config.databricks.schemas.bronze   # "bronze"
config.sources.primary             # "ibkr" (overridden to "yahoo" in notebook)
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

## 9. Delta Tables Created

| Table | Full Name | Type |
|-------|-----------|------|
| Main bronze | `tradeanalytics.bronze.market_data_daily` | Append-only Delta |
| Rejected | `tradeanalytics.bronze.market_data_rejected` | Append-only Delta |
| Watermark | `tradeanalytics.bronze.ingestion_watermark_daily` | MERGE Delta (not append-only) |

S3 location: `s3://handh-trade-refined-use1/bronze/`

---

## 10. S3 Buckets

| Bucket | Purpose |
|--------|---------|
| `handh-trade-raw-use1` | Raw source data |
| `handh-trade-refined-use1` | Delta tables (bronze/silver/gold) |
| `handh-trade-mlflow-use1` | MLflow artifacts |
| `handh-trade-dbx-root-use1` | Databricks workspace root |

---

## 11. Phase 3 Preview (Silver — Feature Engineering)

Planned ABCs: `IndicatorEngine`, `FeatureEngineer`, `FeatureScaler`, `RegimeDetector`
Key components: Hurst Exponent (weekly routing), HMM (bar-level feature), feature store (`tradeanalytics.feature_store.*`)
Intraday stream activation: 1h/4h

---

## 12. Session Start Checklist

When starting a new session, confirm with HC:
1. Any changes since last session? (new commits, config changes, test failures)
2. What's the immediate task? (current = fix BronzeWriter N+1 + complete smoke test)
3. Any new decisions made outside Claude that should update this file?
