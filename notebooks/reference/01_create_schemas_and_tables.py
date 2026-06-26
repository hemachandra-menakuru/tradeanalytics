# Databricks notebook source
# MAGIC %md
# MAGIC # TradeAnalytics — Reference & Control Schema Setup
# MAGIC
# MAGIC **Run:** Once only (or to rebuild after a full teardown).
# MAGIC **Never schedule this notebook.** It is a one-time setup script.
# MAGIC
# MAGIC ## What this notebook creates
# MAGIC
# MAGIC | Schema | Tables | Purpose |
# MAGIC |---|---|---|
# MAGIC | `tradeanalytics.reference` | 7 tables | Catalogue of what exists (instruments, listings, universes) |
# MAGIC | `tradeanalytics.control` | 4 tables | Pipeline state (watermarks, commands, job log) |
# MAGIC
# MAGIC ## Key design decisions
# MAGIC - `instrument_id` (BIGINT GENERATED ALWAYS AS IDENTITY) is the permanent surrogate key
# MAGIC - Symbol is never a primary key — it lives in `instrument_listing` only
# MAGIC - `reference.instrument` is append-only — instruments are never deleted
# MAGIC - `control.ingestion_watermark` is NOT append-only — it is upserted after every run

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 0 — Install dependencies
# MAGIC
# MAGIC `pyyaml` is required by `ConfigLoader` to read YAML config files.
# MAGIC `%pip install` restarts the Python kernel automatically — this cell must run first.

# COMMAND ----------

# MAGIC %pip install pyyaml openpyxl requests --quiet

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 1 — Widgets

# COMMAND ----------

dbutils.widgets.dropdown("dry_run", "true", ["true", "false"], "Dry Run (true = print SQL only)")

dry_run = dbutils.widgets.get("dry_run") == "true"
print(f"dry_run={dry_run}")
if dry_run:
    print("DRY RUN MODE — SQL will be printed but not executed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 2 — Setup

# COMMAND ----------

import logging
import sys
import os

# Add repo root to path so src.* imports work
sys.path.insert(0, "/Workspace/Repos/hemachandra-menakuru/tradeanalytics")

from src.shared.config.config_loader import ConfigLoader

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("schema_setup")

config  = ConfigLoader.load()
CATALOG = config.databricks.catalog   # "tradeanalytics"

logger.info(f"Catalog: {CATALOG}")
logger.info(f"Spark version: {spark.version}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 3 — Helper: execute or print

# COMMAND ----------

def run_sql(sql: str, description: str = "") -> None:
    """
    Execute SQL or print it (if dry_run=true).

    Why a helper function?
    We want every CREATE statement to be reviewable before execution.
    In dry_run mode, this notebook becomes a documentation tool — you can
    read exactly what SQL will run without touching the catalog.
    """
    if description:
        print(f"\n{'─' * 60}")
        print(f"  {description}")
        print(f"{'─' * 60}")
    print(sql.strip())

    if not dry_run:
        spark.sql(sql)
        print("  ✓ executed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 4 — Create schemas
# MAGIC
# MAGIC A **schema** in Unity Catalog is a namespace — a container for tables.
# MAGIC We need two new schemas alongside the existing `tradeanalytics.bronze`:
# MAGIC
# MAGIC - `tradeanalytics.reference` — the catalogue (what instruments exist)
# MAGIC - `tradeanalytics.control`   — the cockpit (what the pipeline has done and should do)

# COMMAND ----------

run_sql(f"""
CREATE SCHEMA IF NOT EXISTS {CATALOG}.reference
COMMENT 'Reference data — instrument master, listings, universe membership, feed config'
""", "Create tradeanalytics.reference schema")

run_sql(f"""
CREATE SCHEMA IF NOT EXISTS {CATALOG}.control
COMMENT 'Pipeline control — watermarks, batch config, commands, job run log'
""", "Create tradeanalytics.control schema")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 5 — reference.instrument
# MAGIC
# MAGIC **Purpose:** Permanent master record for every financial instrument we have ever seen.
# MAGIC
# MAGIC **Key design decisions:**
# MAGIC - `instrument_id` is a BIGINT GENERATED ALWAYS AS IDENTITY — the database auto-assigns it.
# MAGIC   We never set it manually. It is permanent — even if Apple changes its ticker to something
# MAGIC   else, instrument_id=1 (or whatever AAPL gets) never changes.
# MAGIC - `isin` is the international standard identifier (ISO 6166) — the safest cross-vendor join key.
# MAGIC - `figi` is Bloomberg's Financial Instrument Global Identifier — globally unique per instrument
# MAGIC   per exchange. More granular than ISIN (ISIN covers all exchanges; FIGI is per-exchange).
# MAGIC - `ibkr_con_id` is IBKR's internal contract ID — required for every IBKR API call.
# MAGIC - `is_active` = true means we want to ingest data for this instrument.
# MAGIC   Delisting does NOT set is_active=false — we still want historical data.
# MAGIC   is_active=false means we deliberately excluded it from ingestion.
# MAGIC
# MAGIC **Why append-only?**
# MAGIC Once an instrument is registered, its identity never changes. If metadata needs updating
# MAGIC (e.g. we discover the ISIN was wrong), we UPDATE that specific field — but we never DELETE
# MAGIC the row. Hence NOT append-only (we do allow UPDATEs for metadata corrections).

# COMMAND ----------

run_sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.reference.instrument (

    -- Surrogate key — generated by database, permanent, never manually set
    instrument_id       BIGINT      GENERATED ALWAYS AS IDENTITY
                                    COMMENT 'Internal permanent surrogate key',

    -- International identifiers (populated by OpenFIGI)
    isin                STRING      COMMENT 'ISO 6166 international identifier (e.g. US0378331005)',
    figi                STRING      COMMENT 'Bloomberg OpenFIGI identifier (e.g. BBG000B9XRY4)',
    composite_figi      STRING      COMMENT 'Bloomberg composite FIGI (across all exchanges)',

    -- Broker identifiers
    ibkr_con_id         BIGINT      COMMENT 'IBKR contract ID — required for all IBKR API calls',

    -- Classification
    asset_class         STRING      NOT NULL
                                    COMMENT 'equity | etf | fx | crypto | future | option',
    sector              STRING      COMMENT 'GICS sector (e.g. Information Technology)',
    industry            STRING      COMMENT 'GICS industry group',

    -- Status
    is_active           BOOLEAN     NOT NULL DEFAULT true
                                    COMMENT 'false = excluded from ingestion (not delisted)',
    is_delisted         BOOLEAN     NOT NULL DEFAULT false
                                    COMMENT 'true = no longer trading, but row is kept',
    delisted_date       DATE        COMMENT 'Date instrument stopped trading (NULL if still active)',

    -- Audit
    created_at          TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP()
                                    COMMENT 'When this row was first inserted',
    updated_at          TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP()
                                    COMMENT 'When any field on this row last changed',
    source              STRING      COMMENT 'Where we first learned about this instrument (NASDAQ_FTP | MANUAL | ETF_HOLDINGS)'
)
USING DELTA
COMMENT 'Permanent master record for every financial instrument. One row per instrument, ever.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true'
)
""", "Create reference.instrument")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 6 — reference.instrument_listing
# MAGIC
# MAGIC **Purpose:** Symbol + exchange details for an instrument. This is SCD Type 2
# MAGIC (Slowly Changing Dimension) — when a symbol changes (e.g. FB → META), we don't
# MAGIC UPDATE the row. We close the old one (`valid_to = today, is_current = false`)
# MAGIC and INSERT a new one with the new symbol.
# MAGIC
# MAGIC **Why separate from instrument?**
# MAGIC One instrument can be listed on multiple exchanges (e.g. AAPL on NASDAQ and on
# MAGIC a European exchange). Each listing is a separate row with its own symbol, currency,
# MAGIC and tick size. The `instrument_id` FK links them all back to the same AAPL.
# MAGIC
# MAGIC **Why SCD Type 2?**
# MAGIC If we just UPDATE when a symbol changes, we lose history. "What was the symbol
# MAGIC for instrument_id=5 on 2021-10-28?" becomes unanswerable. With SCD Type 2, we
# MAGIC keep every symbol a ticker ever had, with the date range it was valid.

# COMMAND ----------

run_sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.reference.instrument_listing (

    listing_id          BIGINT      GENERATED ALWAYS AS IDENTITY
                                    COMMENT 'Surrogate key for this listing row',

    -- FK to instrument master
    instrument_id       BIGINT      NOT NULL
                                    COMMENT 'FK → reference.instrument.instrument_id',

    -- Symbol details (what changes over time — SCD Type 2)
    symbol              STRING      NOT NULL
                                    COMMENT 'Ticker symbol as used on this exchange (e.g. AAPL, META)',
    exchange            STRING      COMMENT 'Human-readable exchange name (e.g. NASDAQ, NYSE)',
    exchange_mic        STRING      COMMENT 'ISO 10383 Market Identifier Code (e.g. XNAS, XNYS)',
    currency            STRING      NOT NULL DEFAULT 'USD'
                                    COMMENT 'Trading currency (ISO 4217)',
    company_name        STRING      COMMENT 'Full legal company name at time of listing',

    -- Tick and lot sizes
    min_tick_size       DOUBLE      COMMENT 'Minimum price increment (e.g. 0.01)',
    min_lot_size        INT         COMMENT 'Minimum order quantity (e.g. 1)',

    -- SCD Type 2 validity window
    valid_from          DATE        NOT NULL
                                    COMMENT 'Date this symbol/listing became active',
    valid_to            DATE        COMMENT 'Date this symbol/listing was superseded (NULL = current)',
    is_current          BOOLEAN     NOT NULL DEFAULT true
                                    COMMENT 'true = this is the current active listing for this instrument',
    change_reason       STRING      COMMENT 'Why this listing was closed (e.g. SYMBOL_CHANGE, DELISTING, EXCHANGE_TRANSFER)',

    -- Audit
    created_at          TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP()
)
USING DELTA
COMMENT 'Symbol + exchange details per instrument. SCD Type 2 — full symbol history preserved.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true'
)
""", "Create reference.instrument_listing")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 7 — reference.universe_membership
# MAGIC
# MAGIC **Purpose:** Tracks which instruments belong to which index or trading universe.
# MAGIC
# MAGIC An instrument can belong to multiple universes simultaneously:
# MAGIC - AAPL → SP500 (weight 7.2%) AND NDX100 (weight 9.1%)
# MAGIC
# MAGIC This table is re-synced from ETF holdings files weekly because index constituents
# MAGIC change (rebalancing quarterly, plus additions/removals between rebalances).
# MAGIC
# MAGIC `is_active` here means "currently in this index". When NASDAQ removes a stock
# MAGIC from NDX100, we UPDATE is_active=false and set removed_date — we don't DELETE.

# COMMAND ----------

run_sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.reference.universe_membership (

    membership_id       BIGINT      GENERATED ALWAYS AS IDENTITY,

    instrument_id       BIGINT      NOT NULL
                                    COMMENT 'FK → reference.instrument.instrument_id',

    universe_code       STRING      NOT NULL
                                    COMMENT 'SP500 | NDX100 | RUSSELL2000 | CUSTOM',

    -- Index weight (from ETF holdings file)
    index_weight        DOUBLE      COMMENT 'Weight in this universe (0.0 to 1.0)',
    source_etf          STRING      COMMENT 'Which ETF holding file sourced this (SPY | QQQ | IWM)',

    -- Active window
    is_active           BOOLEAN     NOT NULL DEFAULT true
                                    COMMENT 'true = currently in this universe',
    added_date          DATE        NOT NULL
                                    COMMENT 'Date first observed in this universe',
    removed_date        DATE        COMMENT 'Date removed from this universe (NULL = still in)',

    -- Audit
    created_at          TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    updated_at          TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP()
)
USING DELTA
COMMENT 'Which instruments belong to which index universe. Re-synced weekly from ETF holdings.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true'
)
""", "Create reference.universe_membership")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 8 — reference.ticker_feed_config
# MAGIC
# MAGIC **Purpose:** The DESIRED STATE — what we want the ingestion pipeline to do.
# MAGIC
# MAGIC This replaces `tickers.csv`. Instead of a flat file with 8 rows, this table
# MAGIC controls every aspect of ingestion for every instrument:
# MAGIC - How far back we want history (`target_start_date`)
# MAGIC - Whether ingestion is currently active (`is_active`)
# MAGIC - Which batch group runs this instrument (`batch_group`)
# MAGIC - How often to check for new data (`run_frequency`)
# MAGIC
# MAGIC The IngestionPlanner reads this table (desired state) and compares it against
# MAGIC `control.ingestion_watermark` (actual state) to decide what to fetch.
# MAGIC
# MAGIC **Load type is DERIVED, never configured here.**
# MAGIC You control desired state. The planner figures out INITIAL_LOAD vs INCREMENTAL vs GAP_FILL.

# COMMAND ----------

run_sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.reference.ticker_feed_config (

    config_id           BIGINT      GENERATED ALWAYS AS IDENTITY,

    instrument_id       BIGINT      NOT NULL
                                    COMMENT 'FK → reference.instrument.instrument_id',

    stream              STRING      NOT NULL DEFAULT 'daily'
                                    COMMENT 'Which data stream (daily | intraday | tick)',

    -- Desired date range (what we WANT)
    target_start_date   DATE        NOT NULL
                                    COMMENT 'Earliest date we want data for (e.g. 2016-01-01)',
    target_end_date     DATE        COMMENT 'Latest date we want (NULL = ongoing, ingest forever)',

    -- Scheduling
    run_frequency       STRING      NOT NULL DEFAULT 'daily'
                                    COMMENT 'daily | weekly | monthly | on_demand',
    batch_group         STRING      NOT NULL DEFAULT 'A'
                                    COMMENT 'A | B | C | D — controls which job batch runs this',
    priority            INT         NOT NULL DEFAULT 5
                                    COMMENT '1=highest, 10=lowest — within a batch group',

    -- Control
    is_active           BOOLEAN     NOT NULL DEFAULT true
                                    COMMENT 'false = skip this instrument in all runs',
    max_lookback_days   INT         NOT NULL DEFAULT 365
                                    COMMENT 'Max days to fetch in one INITIAL_LOAD run',

    -- Audit
    created_at          TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    updated_at          TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    created_by          STRING      COMMENT 'Who added this config row'
)
USING DELTA
COMMENT 'Desired ingestion state per instrument. Replaces tickers.csv. IngestionPlanner reads this.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true'
)
""", "Create reference.ticker_feed_config")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 9 — reference.market_calendar
# MAGIC
# MAGIC **Purpose:** Exchange holidays and half-days, driven by the `exchange_calendars`
# MAGIC Python library. Replaces the hardcoded 2010–2040 holiday list in IngestionPlanner.
# MAGIC
# MAGIC One row per exchange per date where the market is closed or has modified hours.
# MAGIC Normal trading days are NOT stored — only exceptions.
# MAGIC
# MAGIC `session_type` values:
# MAGIC - `HOLIDAY` — market closed all day
# MAGIC - `EARLY_CLOSE` — market closes early (e.g. day before Thanksgiving: 1pm close)
# MAGIC - `LATE_OPEN` — market opens late (rare)

# COMMAND ----------

run_sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.reference.market_calendar (

    calendar_id         BIGINT      GENERATED ALWAYS AS IDENTITY,

    exchange_mic        STRING      NOT NULL
                                    COMMENT 'ISO 10383 MIC code (e.g. XNAS, XNYS)',
    calendar_date       DATE        NOT NULL
                                    COMMENT 'The date of this calendar event',

    session_type        STRING      NOT NULL
                                    COMMENT 'HOLIDAY | EARLY_CLOSE | LATE_OPEN',
    description         STRING      COMMENT 'Human-readable name (e.g. Thanksgiving, Christmas)',

    -- Modified hours (populated for EARLY_CLOSE and LATE_OPEN, NULL for HOLIDAY)
    open_time_utc       STRING      COMMENT 'Market open time in UTC (HH:MM) — NULL for holidays',
    close_time_utc      STRING      COMMENT 'Market close time in UTC (HH:MM) — NULL for holidays',

    -- Source and validity
    valid_year          INT         NOT NULL
                                    COMMENT 'Calendar year this row covers',
    source              STRING      NOT NULL DEFAULT 'exchange_calendars'
                                    COMMENT 'Data source (exchange_calendars | MANUAL)',
    created_at          TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP()
)
USING DELTA
COMMENT 'Exchange holidays and half-days. Normal trading days not stored — exceptions only.'
TBLPROPERTIES (
    'delta.appendOnly' = 'true'
)
""", "Create reference.market_calendar")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 10 — control.ingestion_watermark
# MAGIC
# MAGIC **Purpose:** The ACTUAL STATE — what the pipeline has actually fetched.
# MAGIC
# MAGIC One row per (instrument_id, stream, interval). Updated after every successful run.
# MAGIC The IngestionPlanner reads this alongside `ticker_feed_config` (desired state)
# MAGIC to derive what to fetch next.
# MAGIC
# MAGIC **NEVER manually INSERT or UPDATE this table.** It is a system mirror.
# MAGIC Manual edits cause phantom loads (re-fetching data we already have) or
# MAGIC silent skips (not fetching data we're missing). Control ingestion via
# MAGIC `ticker_feed_config` (desired state) or `ingestion_command` (one-off commands).
# MAGIC
# MAGIC **NOT append-only** — this table is UPSERTed after every successful ingestion run.

# COMMAND ----------

run_sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.control.ingestion_watermark (

    watermark_id        BIGINT      GENERATED ALWAYS AS IDENTITY,

    instrument_id       BIGINT      NOT NULL
                                    COMMENT 'FK → reference.instrument.instrument_id',
    stream              STRING      NOT NULL DEFAULT 'daily'
                                    COMMENT 'daily | intraday | tick',
    interval            STRING      NOT NULL
                                    COMMENT '1d | 1h | 4h | 1m (bar interval)',

    -- Actual state — what we have fetched
    earliest_date       DATE        COMMENT 'Earliest date fetched for this instrument+interval',
    latest_date         DATE        COMMENT 'Latest date fetched (most recent bar date)',
    record_count        BIGINT      NOT NULL DEFAULT 0
                                    COMMENT 'Total records written to bronze for this instrument',

    -- Run tracking
    last_batch_id       STRING      COMMENT 'batch_id of the last successful run',
    last_run_at         TIMESTAMP   COMMENT 'When the last successful run completed',
    last_load_type      STRING      COMMENT 'INITIAL_LOAD | INCREMENTAL | GAP_FILL | FORCE_RELOAD',

    -- Health
    status              STRING      NOT NULL DEFAULT 'active'
                                    COMMENT 'active | suspended | paused',
    consecutive_failures INT        NOT NULL DEFAULT 0
                                    COMMENT 'Resets to 0 on success. Triggers suspend after N failures.',
    last_error          STRING      COMMENT 'Error message from last failed run (NULL if last run succeeded)',
    suspended_at        TIMESTAMP   COMMENT 'When status changed to suspended',
    suspended_reason    STRING      COMMENT 'Why this instrument was suspended',

    -- Audit
    created_at          TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    updated_at          TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP()
)
USING DELTA
COMMENT 'Actual ingestion state per instrument. NEVER manually edit — managed by pipeline only.'
""", "Create control.ingestion_watermark")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 11 — control.ingestion_batch_config
# MAGIC
# MAGIC **Purpose:** Controls which batch groups and run frequencies a given job type processes.
# MAGIC
# MAGIC The bronze ingestion job runs daily. But not all instruments run every day — some
# MAGIC are weekly or monthly. This table tells the job: "for a daily run, process batch
# MAGIC groups A and B, and only instruments with run_frequency=daily."
# MAGIC
# MAGIC Changing which instruments a job processes = UPDATE one row here.
# MAGIC No code changes needed.

# COMMAND ----------

run_sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.control.ingestion_batch_config (

    batch_config_id     BIGINT      GENERATED ALWAYS AS IDENTITY,

    job_type            STRING      NOT NULL
                                    COMMENT 'bronze_daily | bronze_intraday | universe_sync',
    stream              STRING      NOT NULL
                                    COMMENT 'daily | intraday | tick',

    -- Which instruments this job processes
    batch_groups_included   STRING  NOT NULL
                                    COMMENT 'Comma-separated batch groups (e.g. A,B or A,B,C,D)',
    run_frequencies_included STRING NOT NULL
                                    COMMENT 'Comma-separated run frequencies (e.g. daily or daily,weekly)',

    -- Limits
    max_symbols_per_run     INT     COMMENT 'Cap on instruments per run (NULL = no cap)',
    max_lookback_days       INT     NOT NULL DEFAULT 30
                                    COMMENT 'Max history days to fetch per instrument per run',

    -- Control
    is_active           BOOLEAN     NOT NULL DEFAULT true,
    updated_at          TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    updated_by          STRING
)
USING DELTA
COMMENT 'Controls which batch groups and frequencies each job type processes.'
""", "Create control.ingestion_batch_config")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 12 — control.ingestion_command
# MAGIC
# MAGIC **Purpose:** One-off operator instructions for the pipeline.
# MAGIC
# MAGIC Instead of editing code to force-reload one ticker, you INSERT one row here.
# MAGIC The pipeline checks for pending commands before processing each instrument.
# MAGIC After executing the command, it marks it `consumed=true`.
# MAGIC
# MAGIC **action values:**
# MAGIC - `FORCE_RELOAD` — delete existing watermark and re-ingest from override dates
# MAGIC - `HISTORY_LOAD` — extend history backwards (target_start_date override)
# MAGIC - `PAUSE` — stop ingesting this instrument (sets watermark status=paused)
# MAGIC - `RESUME` — re-activate a paused or suspended instrument
# MAGIC - `SKIP_ONCE` — skip the next scheduled run for this instrument

# COMMAND ----------

run_sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.control.ingestion_command (

    command_id          BIGINT      GENERATED ALWAYS AS IDENTITY,

    instrument_id       BIGINT      NOT NULL
                                    COMMENT 'FK → reference.instrument.instrument_id',
    stream              STRING      NOT NULL DEFAULT 'daily',

    -- What to do
    action              STRING      NOT NULL
                                    COMMENT 'FORCE_RELOAD | HISTORY_LOAD | PAUSE | RESUME | SKIP_ONCE',

    -- Optional date overrides (used by FORCE_RELOAD and HISTORY_LOAD)
    override_start_date DATE        COMMENT 'Fetch from this date (overrides watermark)',
    override_end_date   DATE        COMMENT 'Fetch up to this date (overrides today)',

    -- Execution state
    consumed            BOOLEAN     NOT NULL DEFAULT false
                                    COMMENT 'true = command has been executed',
    consumed_at         TIMESTAMP   COMMENT 'When the pipeline executed this command',
    consumed_by_batch   STRING      COMMENT 'batch_id that executed this command',

    -- Metadata
    reason              STRING      COMMENT 'Why this command was issued (for audit trail)',
    created_at          TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    created_by          STRING      COMMENT 'Who issued this command'
)
USING DELTA
COMMENT 'One-off operator instructions. INSERT a row to control one instrument without code changes.'
""", "Create control.ingestion_command")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 13 — control.job_run_log
# MAGIC
# MAGIC **Purpose:** Full audit trail — one row per instrument per job run.
# MAGIC
# MAGIC This is append-only. Every run writes a row here regardless of outcome.
# MAGIC Gives you complete visibility into: what ran, when, how long it took,
# MAGIC how many records were written, and what happened if it failed.
# MAGIC
# MAGIC **Why per-instrument, not per-job?**
# MAGIC A daily job processes 8 (or eventually 500) instruments. If one fails,
# MAGIC you need to know WHICH one failed and why — not just "the job had an error".
# MAGIC One row per instrument per run gives precise failure attribution.

# COMMAND ----------

run_sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.control.job_run_log (

    log_id              BIGINT      GENERATED ALWAYS AS IDENTITY,

    -- Run identity
    batch_id            STRING      NOT NULL
                                    COMMENT 'Unique batch identifier (e.g. batch_daily_20260627_190000)',
    job_type            STRING      NOT NULL
                                    COMMENT 'bronze_daily | bronze_intraday | universe_sync',
    run_started_at      TIMESTAMP   NOT NULL,
    run_completed_at    TIMESTAMP,
    duration_seconds    DOUBLE,

    -- What was processed
    instrument_id       BIGINT      COMMENT 'FK → reference.instrument.instrument_id (NULL for job-level rows)',
    stream              STRING,
    interval            STRING,
    load_type           STRING      COMMENT 'INITIAL_LOAD | INCREMENTAL | GAP_FILL | FORCE_RELOAD | NO_OP | SKIP',

    -- Results
    status              STRING      NOT NULL
                                    COMMENT 'SUCCESS | FAILED | SKIPPED | PARTIAL',
    records_fetched     BIGINT      NOT NULL DEFAULT 0,
    records_new         BIGINT      NOT NULL DEFAULT 0,
    records_amended     BIGINT      NOT NULL DEFAULT 0,
    records_skipped     BIGINT      NOT NULL DEFAULT 0,
    records_rejected    BIGINT      NOT NULL DEFAULT 0,

    -- Date range processed
    fetch_start_date    DATE,
    fetch_end_date      DATE,

    -- Error details
    error_message       STRING      COMMENT 'Exception message if status=FAILED',
    error_type          STRING      COMMENT 'Exception class name',

    -- Pipeline metadata
    pipeline_version    STRING      COMMENT 'Git SHA or PIPELINE_VERSION env var',
    cluster_id          STRING      COMMENT 'Databricks cluster that ran this job'
)
USING DELTA
COMMENT 'Append-only audit log. One row per instrument per run. Never update or delete.'
TBLPROPERTIES (
    'delta.appendOnly' = 'true'
)
""", "Create control.job_run_log")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 14 — Seed control.ingestion_batch_config
# MAGIC
# MAGIC This table needs one row of initial data — the configuration for the bronze daily job.
# MAGIC Unlike other tables, this has no data to "sync" from an external source.
# MAGIC It is seeded here and updated via SQL if the configuration changes.

# COMMAND ----------

if not dry_run:
    existing = spark.sql(f"""
        SELECT COUNT(*) as cnt
        FROM {CATALOG}.control.ingestion_batch_config
        WHERE job_type = 'bronze_daily'
    """).collect()[0]["cnt"]

    if existing == 0:
        spark.sql(f"""
            INSERT INTO {CATALOG}.control.ingestion_batch_config
                (job_type, stream, batch_groups_included, run_frequencies_included,
                 max_symbols_per_run, max_lookback_days, is_active)
            VALUES
                ('bronze_daily', 'daily', 'A,B,C,D', 'daily', NULL, 30, true)
        """)
        print("✓ Seeded bronze_daily batch config")
    else:
        print(f"  Skipped — bronze_daily batch config already exists ({existing} rows)")
else:
    print("DRY RUN — would seed bronze_daily row into control.ingestion_batch_config")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 15 — Summary

# COMMAND ----------

if not dry_run:
    print("=" * 60)
    print("  Schema & Table Setup — Complete")
    print("=" * 60)
    print()
    print("Schemas created:")
    for schema in ["reference", "control"]:
        count = spark.sql(f"SHOW TABLES IN {CATALOG}.{schema}").count()
        print(f"  {CATALOG}.{schema}  ({count} tables)")

    print()
    print("Reference tables:")
    for tbl in ["instrument", "instrument_listing", "universe_membership",
                "ticker_feed_config", "market_calendar"]:
        print(f"  {CATALOG}.reference.{tbl}")

    print()
    print("Control tables:")
    for tbl in ["ingestion_watermark", "ingestion_batch_config",
                "ingestion_command", "job_run_log"]:
        print(f"  {CATALOG}.control.{tbl}")

    print()
    print("Next step: run notebooks/reference/02_universe_sync.py")
else:
    print("DRY RUN complete — review SQL above, then set dry_run=false to execute")
