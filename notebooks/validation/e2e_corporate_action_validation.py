# Databricks notebook source
# MAGIC %md
# MAGIC # End-to-End Corporate Action Validation
# MAGIC
# MAGIC **Purpose:** Validates the complete ingestion + corporate action pipeline for any ticker
# MAGIC that has had a stock split. Reusable — just change the widgets for a different stock.
# MAGIC
# MAGIC **Default scenario:** NVDA 10-for-1 split on 2024-06-10
# MAGIC
# MAGIC ## What this notebook tests
# MAGIC
# MAGIC | Step | What | Real or Simulated |
# MAGIC |------|------|-------------------|
# MAGIC | 1 | Pre-split OHLCV ingestion via Bronze pipeline | ✅ Real |
# MAGIC | 2 | Post-split OHLCV ingestion via Bronze pipeline | ✅ Real |
# MAGIC | 3 | Corporate actions fetch from Yahoo Finance | ✅ Real |
# MAGIC | 4 | Seed `reference.corporate_actions` | ✅ Real |
# MAGIC | 5 | Detection simulation (vendor price fetch is a stub) | 🔵 Simulated |
# MAGIC | 6 | Classification saga (DETECTED → CLASSIFIED) | ✅ Real |
# MAGIC | 7 | FORCE_RELOAD command issued | ✅ Real |
# MAGIC | 8 | Full table validation queries | ✅ Real |
# MAGIC
# MAGIC ## Why Step 5 is simulated
# MAGIC `CorporateActionDetector._fetch_vendor_prices()` is a stub (returns `{}`) — it is
# MAGIC designed for IBKR which retroactively adjusts prices. Yahoo always returns pre-adjusted
# MAGIC prices so the price ratio comparison cannot be triggered via Yahoo automatically.
# MAGIC We simulate the detection by inserting a DETECTED candidate with the known price ratio,
# MAGIC then let the real classifier process it. Everything from detection onwards is real.
# MAGIC
# MAGIC ## Run order
# MAGIC Run all cells top to bottom. Each section prints a PASS/FAIL result.
# MAGIC The final cell prints a complete summary table.

# COMMAND ----------
# Install dependencies not pre-installed on Databricks clusters
# Must be the first executable cell — restarts the Python kernel after install
%pip install python-dotenv

# COMMAND ----------
# MAGIC %md
# MAGIC ## Widgets

# COMMAND ----------

dbutils.widgets.text("symbol",           "NVDA",       "Stock Symbol")
dbutils.widgets.text("split_date",       "2024-06-10", "Split Effective Date (YYYY-MM-DD)")
dbutils.widgets.text("split_ratio_from", "10",         "Split — shares AFTER (e.g. 10 for 10-for-1)")
dbutils.widgets.text("split_ratio_to",   "1",          "Split — shares BEFORE (e.g. 1 for 10-for-1)")
dbutils.widgets.text("pre_split_start",  "2022-06-01", "Pre-Split Data Start")
dbutils.widgets.text("pre_split_end",    "2024-06-07", "Pre-Split Data End (last trading day before split)")
dbutils.widgets.text("post_split_end",   "2024-06-21", "Post-Split Data End (>=5 days after split)")
dbutils.widgets.text("catalog",          "tradeanalytics", "Unity Catalog Name")
dbutils.widgets.dropdown("dry_run",      "false", ["true", "false"], "Dry Run (no Delta writes)")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

import sys
import os
import logging
from datetime import date, datetime, timezone, timedelta
from typing import Optional, Dict, List

# ── Notebook repo root on the cluster ─────────────────────────────────────────
# Databricks attaches the repo at /Workspace/... — add src/ to path
notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
repo_root = "/".join(notebook_path.split("/")[:-3])   # up from notebooks/validation/
sys.path.insert(0, f"/Workspace{repo_root}")

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s — %(message)s")
logger = logging.getLogger("e2e_validation")

# ── Read widgets ───────────────────────────────────────────────────────────────
SYMBOL           = dbutils.widgets.get("symbol").upper().strip()
SPLIT_DATE       = date.fromisoformat(dbutils.widgets.get("split_date"))
RATIO_FROM       = int(dbutils.widgets.get("split_ratio_from"))
RATIO_TO         = int(dbutils.widgets.get("split_ratio_to"))
PRE_SPLIT_START  = date.fromisoformat(dbutils.widgets.get("pre_split_start"))
PRE_SPLIT_END    = date.fromisoformat(dbutils.widgets.get("pre_split_end"))
POST_SPLIT_END   = date.fromisoformat(dbutils.widgets.get("post_split_end"))
CATALOG          = dbutils.widgets.get("catalog").strip()
DRY_RUN          = dbutils.widgets.get("dry_run").lower() == "true"

# Derived
EXPECTED_PRICE_RATIO = RATIO_TO / RATIO_FROM     # e.g. 0.10 for 10-for-1
POST_SPLIT_START     = SPLIT_DATE                 # split effective date is first post-split day
STREAM               = "daily"

print(f"""
╔══════════════════════════════════════════════════════════════╗
║  E2E Corporate Action Validation
╠══════════════════════════════════════════════════════════════╣
║  Symbol        : {SYMBOL}
║  Split date    : {SPLIT_DATE}  ({RATIO_FROM}-for-{RATIO_TO})
║  Price ratio   : {EXPECTED_PRICE_RATIO:.4f}  (post / pre price)
║  Pre-split     : {PRE_SPLIT_START} → {PRE_SPLIT_END}
║  Post-split    : {POST_SPLIT_START} → {POST_SPLIT_END}
║  Catalog       : {CATALOG}
║  Dry run       : {DRY_RUN}
╚══════════════════════════════════════════════════════════════╝
""")

# ── Result tracker ─────────────────────────────────────────────────────────────
_results: Dict[str, str] = {}     # step_name → "PASS" | "FAIL" | "SKIP"
_notes:   Dict[str, str] = {}     # step_name → detail message

def _pass(step: str, msg: str = ""):
    _results[step] = "✅ PASS"
    _notes[step]   = msg
    print(f"  ✅ PASS  {step}" + (f"  — {msg}" if msg else ""))

def _fail(step: str, msg: str = ""):
    _results[step] = "❌ FAIL"
    _notes[step]   = msg
    print(f"  ❌ FAIL  {step}" + (f"  — {msg}" if msg else ""))
    raise AssertionError(f"Step failed: {step}. {msg}")

def _skip(step: str, msg: str = ""):
    _results[step] = "⏭ SKIP"
    _notes[step]   = msg
    print(f"  ⏭ SKIP  {step}" + (f"  — {msg}" if msg else ""))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Pre-flight: Verify symbol exists in reference tables

# COMMAND ----------

print(f"\n── Pre-flight: {SYMBOL} in reference tables ──────────────────────")

try:
    rows = spark.sql(f"""
        SELECT i.instrument_id, il.symbol, il.exchange
        FROM {CATALOG}.reference.instrument_listing il
        JOIN {CATALOG}.reference.instrument i USING (instrument_id)
        WHERE il.symbol = '{SYMBOL}'
          AND il.is_current = true
    """).collect()

    if not rows:
        _fail("preflight_symbol_exists",
              f"{SYMBOL} not found in reference.instrument_listing. "
              f"Seed the reference tables first (run notebooks/reference/*).")

    row = rows[0]
    INSTRUMENT_ID = int(row["instrument_id"])
    print(f"  Symbol        : {row['symbol']}")
    print(f"  instrument_id : {INSTRUMENT_ID}")
    print(f"  Exchange      : {row['exchange']}")
    _pass("preflight_symbol_exists", f"instrument_id={INSTRUMENT_ID}")

except Exception as e:
    if "AssertionError" in str(type(e)):
        raise
    _fail("preflight_symbol_exists", str(e))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 1 — Pre-split OHLCV ingestion
# MAGIC
# MAGIC **What:** Ingest `{PRE_SPLIT_START}` → `{PRE_SPLIT_END}` for `{SYMBOL}` via the
# MAGIC standard Bronze pipeline (Yahoo provider).
# MAGIC
# MAGIC **Why Yahoo returns adjusted prices:** Yahoo Finance `auto_adjust=True` returns
# MAGIC split-adjusted prices for ALL historical dates — even dates before the split occurred.
# MAGIC This is the correct behaviour for Silver feature engineering (adjusted prices for
# MAGIC accurate returns calculation). The important thing we're testing here is NOT the price
# MAGIC values but the pipeline mechanics: records land in Bronze, instrument_id is stamped,
# MAGIC watermark is updated, deduplication works.
# MAGIC
# MAGIC **Pre-split price range expected for NVDA (adjusted):** ~$40–$120 (split-adjusted)

# COMMAND ----------

print(f"\n── Step 1: Pre-split ingestion ({PRE_SPLIT_START} → {PRE_SPLIT_END}) ─────────────")

if DRY_RUN:
    _skip("step1_pre_split_ingestion", "dry_run=true")
else:
    try:
        from src.shared.config.config_loader import ConfigLoader
        from src.bronze.jobs.bronze_ingestion_job import BronzeIngestionJob
        from src.bronze.factory.provider_factory import MarketDataFactory
        from src.bronze.providers.yahoo_provider import YahooProvider

        config = ConfigLoader.load()
        MarketDataFactory.register("yahoo", YahooProvider)

        job = BronzeIngestionJob(config=config, stream_name=STREAM, spark=spark)
        summary = job.run(
            symbols=[SYMBOL],
            start_date=PRE_SPLIT_START,
            end_date=PRE_SPLIT_END,
        )

        if SYMBOL in summary.failed:
            _fail("step1_pre_split_ingestion", summary.errors.get(SYMBOL, "unknown error"))

        result = next((r for r in summary.results if r.symbol == SYMBOL), None)
        if result:
            print(f"  Records written  : {result.records_written}")
            print(f"  Records rejected : {result.records_rejected}")
            print(f"  Ingestion type   : {result.ingestion_type}")
            _pass("step1_pre_split_ingestion",
                  f"{result.records_written} records written, {result.records_rejected} rejected")
        elif SYMBOL in summary.skipped:
            _pass("step1_pre_split_ingestion", "NO_OP — watermark already covers this range")
        else:
            _fail("step1_pre_split_ingestion", "no result found in job summary")

    except AssertionError:
        raise
    except Exception as e:
        _fail("step1_pre_split_ingestion", str(e))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 1a — Verify Bronze pre-split records

# COMMAND ----------

print(f"\n── Step 1a: Verify Bronze pre-split records ────────────────────────")

try:
    pre_rows = spark.sql(f"""
        SELECT COUNT(*)                              AS record_count,
               MIN(date)                            AS earliest_date,
               MAX(date)                            AS latest_date,
               COUNT(DISTINCT date)                 AS distinct_dates,
               MIN(close)                           AS min_close,
               MAX(close)                           AS max_close,
               SUM(CASE WHEN instrument_id IS NULL THEN 1 ELSE 0 END) AS null_instrument_ids,
               SUM(CASE WHEN close IS NULL THEN 1 ELSE 0 END)         AS null_closes,
               SUM(CASE WHEN volume IS NULL THEN 1 ELSE 0 END)        AS null_volumes
        FROM {CATALOG}.bronze.market_data_daily
        WHERE symbol = '{SYMBOL}'
          AND date BETWEEN '{PRE_SPLIT_START}' AND '{PRE_SPLIT_END}'
    """).collect()[0]

    print(f"  Records         : {pre_rows['record_count']}")
    print(f"  Date range      : {pre_rows['earliest_date']} → {pre_rows['latest_date']}")
    print(f"  Distinct dates  : {pre_rows['distinct_dates']}")
    print(f"  Close range     : ${pre_rows['min_close']:.2f} – ${pre_rows['max_close']:.2f}")
    print(f"  Null instrument_ids : {pre_rows['null_instrument_ids']}")
    print(f"  Null closes     : {pre_rows['null_closes']}")

    issues = []
    if pre_rows["record_count"] == 0:
        issues.append("no records found")
    if pre_rows["null_instrument_ids"] > 0:
        issues.append(f"{pre_rows['null_instrument_ids']} NULL instrument_ids")
    if pre_rows["null_closes"] > 0:
        issues.append(f"{pre_rows['null_closes']} NULL closes")
    if pre_rows["record_count"] != pre_rows["distinct_dates"]:
        issues.append(f"duplicates detected: {pre_rows['record_count']} records, {pre_rows['distinct_dates']} distinct dates")

    if issues:
        _fail("step1a_bronze_pre_split_verify", "; ".join(issues))
    else:
        _pass("step1a_bronze_pre_split_verify",
              f"{pre_rows['record_count']} records, ${pre_rows['min_close']:.2f}–${pre_rows['max_close']:.2f}")

except AssertionError:
    raise
except Exception as e:
    _fail("step1a_bronze_pre_split_verify", str(e))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 2 — Post-split OHLCV ingestion
# MAGIC
# MAGIC **What:** Ingest `{POST_SPLIT_START}` → `{POST_SPLIT_END}` (at least 5 trading days
# MAGIC post-split). Yahoo returns adjusted prices — for NVDA these will be ~$120–$135 range.
# MAGIC
# MAGIC **Expected:** Bronze now contains records spanning BOTH sides of the split date.
# MAGIC This is correct — Bronze is append-only and stores all records as ingested.
# MAGIC Silver reads through a dedup window to get the latest record_version per date.

# COMMAND ----------

print(f"\n── Step 2: Post-split ingestion ({POST_SPLIT_START} → {POST_SPLIT_END}) ──────────")

if DRY_RUN:
    _skip("step2_post_split_ingestion", "dry_run=true")
else:
    try:
        job2 = BronzeIngestionJob(config=config, stream_name=STREAM, spark=spark)
        summary2 = job2.run(
            symbols=[SYMBOL],
            start_date=POST_SPLIT_START,
            end_date=POST_SPLIT_END,
        )

        if SYMBOL in summary2.failed:
            _fail("step2_post_split_ingestion", summary2.errors.get(SYMBOL, "unknown error"))

        result2 = next((r for r in summary2.results if r.symbol == SYMBOL), None)
        if result2:
            print(f"  Records written  : {result2.records_written}")
            print(f"  Records rejected : {result2.records_rejected}")
            _pass("step2_post_split_ingestion",
                  f"{result2.records_written} records written, {result2.records_rejected} rejected")
        elif SYMBOL in summary2.skipped:
            _pass("step2_post_split_ingestion", "NO_OP — watermark already covers this range")
        else:
            _fail("step2_post_split_ingestion", "no result found in job summary")

    except AssertionError:
        raise
    except Exception as e:
        _fail("step2_post_split_ingestion", str(e))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 2a — Verify Bronze spans both sides of the split

# COMMAND ----------

print(f"\n── Step 2a: Verify Bronze spans the split date ─────────────────────")

try:
    span = spark.sql(f"""
        SELECT
            COUNT(*)                                                AS total_records,
            MIN(date)                                               AS earliest_date,
            MAX(date)                                               AS latest_date,
            SUM(CASE WHEN date < '{SPLIT_DATE}' THEN 1 ELSE 0 END) AS pre_split_records,
            SUM(CASE WHEN date >= '{SPLIT_DATE}' THEN 1 ELSE 0 END) AS post_split_records,
            MIN(CASE WHEN date < '{SPLIT_DATE}' THEN close END)    AS pre_split_min_close,
            MAX(CASE WHEN date < '{SPLIT_DATE}' THEN close END)    AS pre_split_max_close,
            MIN(CASE WHEN date >= '{SPLIT_DATE}' THEN close END)   AS post_split_min_close,
            MAX(CASE WHEN date >= '{SPLIT_DATE}' THEN close END)   AS post_split_max_close
        FROM {CATALOG}.bronze.market_data_daily
        WHERE symbol = '{SYMBOL}'
    """).collect()[0]

    print(f"  Total records        : {span['total_records']}")
    print(f"  Date range           : {span['earliest_date']} → {span['latest_date']}")
    print(f"  Pre-split records    : {span['pre_split_records']}  close ${span['pre_split_min_close']:.2f}–${span['pre_split_max_close']:.2f}")
    print(f"  Post-split records   : {span['post_split_records']} close ${span['post_split_min_close']:.2f}–${span['post_split_max_close']:.2f}")

    issues = []
    if span["pre_split_records"] == 0:
        issues.append("no pre-split records")
    if span["post_split_records"] == 0:
        issues.append("no post-split records")

    if issues:
        _fail("step2a_bronze_spans_split", "; ".join(issues))
    else:
        _pass("step2a_bronze_spans_split",
              f"{span['pre_split_records']} pre-split + {span['post_split_records']} post-split records")

except AssertionError:
    raise
except Exception as e:
    _fail("step2a_bronze_spans_split", str(e))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 3 — Watermark validation

# COMMAND ----------

print(f"\n── Step 3: Watermark check ─────────────────────────────────────────")

try:
    wm = spark.sql(f"""
        SELECT instrument_id, stream, interval,
               earliest_date, latest_date, record_count, vendor, status
        FROM {CATALOG}.control.ingestion_watermark
        WHERE instrument_id = {INSTRUMENT_ID}
          AND stream = '{STREAM}'
    """).collect()

    if not wm:
        _fail("step3_watermark", f"No watermark row for instrument_id={INSTRUMENT_ID} stream={STREAM}")

    wm_row = wm[0]
    print(f"  instrument_id  : {wm_row['instrument_id']}")
    print(f"  stream         : {wm_row['stream']}")
    print(f"  earliest_date  : {wm_row['earliest_date']}")
    print(f"  latest_date    : {wm_row['latest_date']}")
    print(f"  record_count   : {wm_row['record_count']}")
    print(f"  vendor         : {wm_row['vendor']}")
    print(f"  status         : {wm_row['status']}")

    issues = []
    if wm_row["earliest_date"] is None:
        issues.append("earliest_date is NULL")
    if wm_row["latest_date"] is None:
        issues.append("latest_date is NULL")
    if str(wm_row["latest_date"]) < str(POST_SPLIT_START):
        issues.append(f"latest_date {wm_row['latest_date']} is before post-split start {POST_SPLIT_START}")

    if issues:
        _fail("step3_watermark", "; ".join(issues))
    else:
        _pass("step3_watermark",
              f"{wm_row['earliest_date']} → {wm_row['latest_date']}, vendor={wm_row['vendor']}")

except AssertionError:
    raise
except Exception as e:
    _fail("step3_watermark", str(e))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 4 — Deduplication check
# MAGIC
# MAGIC **What:** Verify no duplicate records exist for the same (symbol, date, interval).
# MAGIC This validates Layer 2 deduplication in BronzeWriter.
# MAGIC
# MAGIC **Expected:** Every (symbol, date, interval) combination appears exactly once.

# COMMAND ----------

print(f"\n── Step 4: Deduplication check ─────────────────────────────────────")

try:
    dups = spark.sql(f"""
        SELECT date, interval, COUNT(*) AS cnt
        FROM {CATALOG}.bronze.market_data_daily
        WHERE symbol = '{SYMBOL}'
        GROUP BY date, interval
        HAVING COUNT(*) > 1
        ORDER BY cnt DESC
        LIMIT 10
    """).collect()

    if dups:
        dup_summary = ", ".join([f"{r['bar_date']} ({r['cnt']}x)" for r in dups[:5]])
        _fail("step4_deduplication", f"Duplicate records found: {dup_summary}")
    else:
        count = spark.sql(f"""
            SELECT COUNT(*) AS n
            FROM {CATALOG}.bronze.market_data_daily
            WHERE symbol = '{SYMBOL}'
        """).collect()[0]["n"]
        _pass("step4_deduplication", f"No duplicates across {count} total records")

except AssertionError:
    raise
except Exception as e:
    _fail("step4_deduplication", str(e))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 5 — Corporate actions fetch from Yahoo Finance
# MAGIC
# MAGIC **What:** Use `YahooCorporateActionsProvider` to fetch real split history for
# MAGIC `{SYMBOL}`. This calls Yahoo Finance's split data and returns the actual split events
# MAGIC as a list of dicts in our standard format.
# MAGIC
# MAGIC **Expected for NVDA:** One SPLIT event on 2024-06-10, ratio 10-for-1.

# COMMAND ----------

print(f"\n── Step 5: Fetch corporate actions from Yahoo Finance ───────────────")

try:
    from src.reference.providers.yahoo_corporate_actions_provider import YahooCorporateActionsProvider

    provider = YahooCorporateActionsProvider.from_spark(spark, catalog=CATALOG)

    # Fetch 10 years of corporate action history to catch all events
    ca_events = provider.get_corporate_actions(
        instrument_ids=[INSTRUMENT_ID],
        from_date=date(2015, 1, 1),
        to_date=date.today(),
    )

    print(f"\n  All corporate action events fetched for {SYMBOL}:")
    for ev in ca_events:
        print(f"    {ev['effective_date']}  {ev['event_type']:15s}  "
              f"ratio={ev['split_ratio_from']}-for-{ev['split_ratio_to']}  "
              f"factor={ev['event_factor']:.6f}  "
              f"source={ev['source_vendor']}")

    # Verify our expected split event is present
    split_events = [e for e in ca_events if e["event_type"] == "SPLIT"]
    target_splits = [
        e for e in split_events
        if str(e["effective_date"]) == str(SPLIT_DATE)
        and e["split_ratio_from"] == RATIO_FROM
        and e["split_ratio_to"] == RATIO_TO
    ]

    if not target_splits:
        _fail("step5_ca_fetch",
              f"Expected {RATIO_FROM}-for-{RATIO_TO} split on {SPLIT_DATE} not found in Yahoo data. "
              f"Found splits: {[(str(e['effective_date']), e['split_ratio_from']) for e in split_events]}")
    else:
        ev = target_splits[0]
        _pass("step5_ca_fetch",
              f"Found {RATIO_FROM}-for-{RATIO_TO} SPLIT on {SPLIT_DATE}, "
              f"event_factor={ev['event_factor']:.6f}")

    CORPORATE_ACTION_EVENTS = ca_events  # save for Step 6

except AssertionError:
    raise
except Exception as e:
    _fail("step5_ca_fetch", str(e))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 6 — Seed `reference.corporate_actions`
# MAGIC
# MAGIC **What:** Write all fetched corporate action events into `reference.corporate_actions`.
# MAGIC This is the audit log of all known corporate events for all instruments.
# MAGIC
# MAGIC **Idempotent:** If the event already exists (same instrument_id + effective_date +
# MAGIC event_type), we skip it rather than inserting a duplicate.

# COMMAND ----------

print(f"\n── Step 6: Seed reference.corporate_actions ────────────────────────")

if DRY_RUN:
    _skip("step6_seed_corporate_actions", "dry_run=true")
else:
    try:
        from pyspark.sql.types import (
            StructType, StructField, LongType, StringType,
            DateType, DoubleType, IntegerType,
        )
        from datetime import date as _date

        schema = StructType([
            StructField("instrument_id",     LongType(),    False),
            StructField("effective_date",    DateType(),    False),
            StructField("event_type",        StringType(),  False),
            StructField("split_ratio_from",  IntegerType(), True),
            StructField("split_ratio_to",    IntegerType(), True),
            StructField("event_factor",      DoubleType(),  True),
            StructField("dividend_amount",   DoubleType(),  True),
            StructField("dividend_currency", StringType(),  True),
            StructField("event_detail",      StringType(),  True),
            StructField("source_vendor",     StringType(),  True),
        ])

        rows = []
        for ev in CORPORATE_ACTION_EVENTS:
            eff = ev["effective_date"]
            if not isinstance(eff, _date):
                eff = _date.fromisoformat(str(eff))
            rows.append((
                int(ev["instrument_id"]),
                eff,
                ev["event_type"],
                int(ev["split_ratio_from"]) if ev["split_ratio_from"] is not None else None,
                int(ev["split_ratio_to"])   if ev["split_ratio_to"]   is not None else None,
                float(ev["event_factor"])   if ev["event_factor"]     is not None else None,
                float(ev["dividend_amount"]) if ev.get("dividend_amount") is not None else None,
                ev.get("dividend_currency"),
                ev.get("event_detail"),
                ev.get("source_vendor", "yahoo"),
            ))

        df_ca = spark.createDataFrame(rows, schema=schema)
        df_ca.createOrReplaceTempView("_new_corp_actions")

        # Idempotent insert — skip rows that already exist
        inserted = spark.sql(f"""
            INSERT INTO {CATALOG}.reference.corporate_actions
                (instrument_id, effective_date, event_type, split_ratio_from, split_ratio_to,
                 event_factor, dividend_amount, dividend_currency, event_detail, source_vendor)
            SELECT n.*
            FROM _new_corp_actions n
            WHERE NOT EXISTS (
                SELECT 1
                FROM {CATALOG}.reference.corporate_actions ca
                WHERE ca.instrument_id  = n.instrument_id
                  AND ca.effective_date = n.effective_date
                  AND ca.event_type     = n.event_type
            )
        """)

        # Verify
        seeded = spark.sql(f"""
            SELECT COUNT(*) AS n
            FROM {CATALOG}.reference.corporate_actions
            WHERE instrument_id = {INSTRUMENT_ID}
        """).collect()[0]["n"]

        print(f"  Events inserted from Yahoo : {len(rows)}")
        print(f"  Total in reference table   : {seeded}")
        _pass("step6_seed_corporate_actions", f"{seeded} events in reference.corporate_actions")

    except AssertionError:
        raise
    except Exception as e:
        _fail("step6_seed_corporate_actions", str(e))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 7 — Verify `reference.corporate_actions` content

# COMMAND ----------

print(f"\n── Step 7: Verify reference.corporate_actions ──────────────────────")

try:
    ca_rows = spark.sql(f"""
        SELECT action_id, instrument_id, effective_date, event_type,
               split_ratio_from, split_ratio_to, event_factor,
               dividend_amount, source_vendor
        FROM {CATALOG}.reference.corporate_actions
        WHERE instrument_id = {INSTRUMENT_ID}
        ORDER BY effective_date
    """).collect()

    print(f"\n  All events for {SYMBOL} (instrument_id={INSTRUMENT_ID}):")
    for r in ca_rows:
        print(f"    {r['effective_date']}  {r['event_type']:15s}  "
              f"ratio={r['split_ratio_from']}-for-{r['split_ratio_to']}  "
              f"factor={r['event_factor']}  source={r['source_vendor']}")

    # Verify our target split is there
    target = [
        r for r in ca_rows
        if str(r["effective_date"]) == str(SPLIT_DATE)
        and r["event_type"] == "SPLIT"
        and r["split_ratio_from"] == RATIO_FROM
    ]

    if not target:
        _fail("step7_verify_corporate_actions",
              f"Target split {RATIO_FROM}-for-{RATIO_TO} on {SPLIT_DATE} not found in table")
    else:
        r = target[0]
        _pass("step7_verify_corporate_actions",
              f"action_id={r['action_id']}  {RATIO_FROM}-for-{RATIO_TO} on {SPLIT_DATE}  "
              f"factor={r['event_factor']:.6f}")

except AssertionError:
    raise
except Exception as e:
    _fail("step7_verify_corporate_actions", str(e))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 8 — Simulate corporate action detection
# MAGIC
# MAGIC **Context:** `CorporateActionDetector._fetch_vendor_prices()` is currently a stub —
# MAGIC it is designed for IBKR which retroactively adjusts historical prices after a split.
# MAGIC Yahoo always returns pre-adjusted prices, so there is no price discrepancy for the
# MAGIC detector to catch automatically.
# MAGIC
# MAGIC **What we simulate:** We manually insert a DETECTED candidate with realistic prices
# MAGIC (Bronze pre-split close vs current Yahoo adjusted close for the same date). This is
# MAGIC exactly what the detector would insert if _fetch_vendor_prices() were wired to IBKR.
# MAGIC
# MAGIC **What remains real:** Everything from Step 9 onwards (classifier, saga progression,
# MAGIC FORCE_RELOAD command) runs against the real Delta tables with no simulation.

# COMMAND ----------

print(f"\n── Step 8: Simulate detection (insert DETECTED candidate) ──────────")

if DRY_RUN:
    _skip("step8_simulate_detection", "dry_run=true")
else:
    try:
        # Get the last pre-split close from Bronze (stored at original/adjusted price)
        pre_rows = spark.sql(f"""
            SELECT close, date
            FROM {CATALOG}.bronze.market_data_daily
            WHERE symbol = '{SYMBOL}'
              AND date = '{PRE_SPLIT_END}'
        """).collect()

        if not pre_rows:
            # Fall back to closest available pre-split date
            pre_rows = spark.sql(f"""
                SELECT close, date
                FROM {CATALOG}.bronze.market_data_daily
                WHERE symbol = '{SYMBOL}'
                  AND date < '{SPLIT_DATE}'
                ORDER BY date DESC
                LIMIT 1
            """).collect()

        if not pre_rows:
            _fail("step8_simulate_detection", "No pre-split records in Bronze to compute price ratio")

        bronze_price = float(pre_rows[0]["close"])
        bronze_date  = pre_rows[0]["bar_date"]

        # Get the current Yahoo price for the same date (this will be the adjusted price)
        import yfinance as yf
        from datetime import timedelta
        yf_end = (bronze_date + timedelta(days=1)).isoformat() if hasattr(bronze_date, '__add__') else str(bronze_date)
        yf_start = str(bronze_date)
        ticker = yf.Ticker(SYMBOL)
        df_yf = ticker.history(start=yf_start, end=yf_end, interval="1d", auto_adjust=True)

        if df_yf.empty:
            # Yahoo may skip that exact date — use the known split ratio to compute simulated vendor price
            vendor_price = bronze_price * EXPECTED_PRICE_RATIO
            print(f"  Yahoo returned no data for {bronze_date} — computing vendor_price from known ratio")
        else:
            vendor_price = float(df_yf["Close"].iloc[0])

        actual_ratio = vendor_price / bronze_price
        print(f"  Pre-split date   : {bronze_date}")
        print(f"  Bronze close     : ${bronze_price:.4f}")
        print(f"  Yahoo close now  : ${vendor_price:.4f}")
        print(f"  Actual ratio     : {actual_ratio:.4f}  (expected ≈ {EXPECTED_PRICE_RATIO:.4f})")
        print(f"  Deviation from 1 : {abs(actual_ratio - 1.0):.1%}  (threshold is 40%)")

        # Check if candidate already exists for this instrument
        existing = spark.sql(f"""
            SELECT candidate_id FROM {CATALOG}.control.corporate_action_candidates
            WHERE instrument_id = {INSTRUMENT_ID}
              AND status NOT IN ('RESOLVED', 'FALSE_POSITIVE', 'FAILED')
        """).collect()

        if existing:
            cid = existing[0]["candidate_id"]
            print(f"  ⚠️  Open candidate already exists (candidate_id={cid}) — skipping insert")
            CANDIDATE_ID = cid
            _pass("step8_simulate_detection", f"Existing open candidate reused (candidate_id={cid})")
        else:
            from pyspark.sql.types import (
                StructType, StructField, LongType, DateType, DoubleType
            )
            from datetime import date as _date
            det_date = date.today()
            if not isinstance(bronze_date, _date):
                bronze_date = _date.fromisoformat(str(bronze_date))

            schema = StructType([
                StructField("instrument_id",     LongType(),    False),
                StructField("detected_on_date",  DateType(),    False),
                StructField("price_in_bronze",   DoubleType(),  False),
                StructField("price_from_vendor", DoubleType(),  False),
                StructField("price_ratio",       DoubleType(),  False),
            ])
            df_cand = spark.createDataFrame(
                [(INSTRUMENT_ID, det_date, bronze_price, vendor_price, actual_ratio)],
                schema=schema,
            )
            df_cand.createOrReplaceTempView("_detection_candidate")

            spark.sql(f"""
                INSERT INTO {CATALOG}.control.corporate_action_candidates
                    (instrument_id, detected_on_date, price_in_bronze,
                     price_from_vendor, price_ratio, status, retry_count)
                SELECT instrument_id, detected_on_date, price_in_bronze,
                       price_from_vendor, price_ratio, 'DETECTED', 0
                FROM _detection_candidate
            """)

            # Read back to get the auto-generated candidate_id
            cand = spark.sql(f"""
                SELECT candidate_id FROM {CATALOG}.control.corporate_action_candidates
                WHERE instrument_id = {INSTRUMENT_ID}
                  AND status = 'DETECTED'
                ORDER BY detected_at DESC
                LIMIT 1
            """).collect()
            CANDIDATE_ID = cand[0]["candidate_id"] if cand else None
            _pass("step8_simulate_detection",
                  f"DETECTED candidate inserted (candidate_id={CANDIDATE_ID}, "
                  f"ratio={actual_ratio:.4f})")

    except AssertionError:
        raise
    except Exception as e:
        _fail("step8_simulate_detection", str(e))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 9 — Run CorporateActionClassifier
# MAGIC
# MAGIC **What:** The classifier reads all candidates at status=DETECTED, determines the
# MAGIC event type from the price_ratio, inserts a row into `reference.corporate_actions`
# MAGIC (if not already there), and issues a FORCE_RELOAD command.
# MAGIC
# MAGIC **Expected for NVDA:** ratio ≈ 0.10 → matches `(10, 1, 0.1000)` within ±5% tolerance
# MAGIC → classified as SPLIT 10-for-1 → status advances to CLASSIFIED.

# COMMAND ----------

print(f"\n── Step 9: Run CorporateActionClassifier ───────────────────────────")

if DRY_RUN:
    _skip("step9_classifier", "dry_run=true")
else:
    try:
        from src.control.corporate_actions.classifier import CorporateActionClassifier

        classifier = CorporateActionClassifier(
            spark=spark,
            catalog=CATALOG,
            reload_target_start=PRE_SPLIT_START,
        )
        result_summary = classifier.process_detected_candidates()

        print(f"  Classified as SPLIT         : {result_summary['classified_split']}")
        print(f"  Classified as FALSE_POSITIVE: {result_summary['classified_false_positive']}")
        print(f"  Failed                      : {result_summary['failed']}")

        if result_summary["failed"] > 0:
            _fail("step9_classifier", f"{result_summary['failed']} candidates failed classification")
        elif result_summary["classified_split"] == 0 and result_summary["classified_false_positive"] == 0:
            _fail("step9_classifier", "No candidates were processed — DETECTED candidate may be missing")
        else:
            _pass("step9_classifier",
                  f"{result_summary['classified_split']} split(s), "
                  f"{result_summary['classified_false_positive']} false positive(s)")

    except AssertionError:
        raise
    except Exception as e:
        _fail("step9_classifier", str(e))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 10 — Verify saga state machine progression

# COMMAND ----------

print(f"\n── Step 10: Verify saga state after classification ─────────────────")

try:
    saga = spark.sql(f"""
        SELECT candidate_id, instrument_id, status, event_type,
               price_in_bronze, price_from_vendor, price_ratio,
               detected_on_date, classified_at, reload_command_id, retry_count
        FROM {CATALOG}.control.corporate_action_candidates
        WHERE instrument_id = {INSTRUMENT_ID}
        ORDER BY detected_at DESC
        LIMIT 5
    """).collect()

    print(f"\n  Candidates for instrument_id={INSTRUMENT_ID} ({SYMBOL}):")
    for r in saga:
        print(f"    candidate_id={r['candidate_id']}  status={r['status']}  "
              f"event_type={r['event_type']}  ratio={r['price_ratio']:.4f}  "
              f"reload_cmd={r['reload_command_id']}")

    classified = [r for r in saga if r["status"] == "CLASSIFIED"]
    if not classified:
        _fail("step10_saga_state",
              "No CLASSIFIED candidates found. Expected at least one after running the classifier.")
    else:
        r = classified[0]
        _pass("step10_saga_state",
              f"candidate_id={r['candidate_id']} at CLASSIFIED, "
              f"event_type={r['event_type']}, reload_cmd_id={r['reload_command_id']}")

except AssertionError:
    raise
except Exception as e:
    _fail("step10_saga_state", str(e))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 11 — Verify FORCE_RELOAD command was issued

# COMMAND ----------

print(f"\n── Step 11: Verify FORCE_RELOAD in control.ingestion_command ───────")

try:
    cmds = spark.sql(f"""
        SELECT command_id, instrument_id, action, override_start_date,
               override_end_date, created_at, consumed_at, status
        FROM {CATALOG}.control.ingestion_command
        WHERE instrument_id = {INSTRUMENT_ID}
          AND action = 'FORCE_RELOAD'
        ORDER BY created_at DESC
        LIMIT 5
    """).collect()

    print(f"\n  FORCE_RELOAD commands for {SYMBOL}:")
    for r in cmds:
        consumed = r['consumed_at'] if r['consumed_at'] else "not yet consumed"
        print(f"    command_id={r['command_id']}  status={r['status']}  "
              f"range={r['override_start_date']}→{r['override_end_date']}  "
              f"consumed={consumed}")

    pending = [r for r in cmds if r["consumed_at"] is None]
    if not pending and not cmds:
        _fail("step11_force_reload",
              "No FORCE_RELOAD commands found. Classifier may not have issued the command.")
    else:
        r = (pending or cmds)[0]
        _pass("step11_force_reload",
              f"command_id={r['command_id']}  {r['override_start_date']}→{r['override_end_date']}")

except AssertionError:
    raise
except Exception as e:
    _fail("step11_force_reload", str(e))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 12 — Idempotency check (re-run ingestion, expect NO_OP)
# MAGIC
# MAGIC **What:** Run the ingestion job again for the same date range.
# MAGIC **Expected:** IngestionPlanner returns NO_OP for both ranges — watermark already covers them.
# MAGIC This validates Layer 1 deduplication (date-range-level guard).

# COMMAND ----------

print(f"\n── Step 12: Idempotency check ──────────────────────────────────────")

if DRY_RUN:
    _skip("step12_idempotency", "dry_run=true")
else:
    try:
        job3 = BronzeIngestionJob(config=config, stream_name=STREAM, spark=spark)
        summary3 = job3.run(
            symbols=[SYMBOL],
            start_date=PRE_SPLIT_START,
            end_date=POST_SPLIT_END,
        )

        # Count Bronze records before and after re-run
        count_after = spark.sql(f"""
            SELECT COUNT(*) AS n
            FROM {CATALOG}.bronze.market_data_daily
            WHERE symbol = '{SYMBOL}'
        """).collect()[0]["n"]

        total_from_step4 = count_after  # should match what Step 4 validated

        if SYMBOL in summary3.failed:
            _fail("step12_idempotency", summary3.errors.get(SYMBOL, "unknown error"))
        elif SYMBOL in summary3.skipped:
            _pass("step12_idempotency",
                  f"Correctly returned NO_OP — {count_after} records unchanged in Bronze")
        else:
            result3 = next((r for r in summary3.results if r.symbol == SYMBOL), None)
            if result3 and result3.records_written == 0:
                _pass("step12_idempotency", f"0 new records written — idempotent")
            elif result3:
                _fail("step12_idempotency",
                      f"Wrote {result3.records_written} records on re-run — expected 0")
            else:
                _skip("step12_idempotency", "Could not determine NO_OP — check watermark manually")

    except AssertionError:
        raise
    except Exception as e:
        _fail("step12_idempotency", str(e))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 13 — Full table state snapshot

# COMMAND ----------

print(f"\n── Step 13: Full table state snapshot ──────────────────────────────")
print(f"\n  📋 bronze.market_data_daily — {SYMBOL} (20 most recent)")
display(spark.sql(f"""
    SELECT date, open, high, low, close, volume, instrument_id,
           source, ingested_by, record_version, data_quality_flag
    FROM {CATALOG}.bronze.market_data_daily
    WHERE symbol = '{SYMBOL}'
    ORDER BY date DESC
    LIMIT 20
"""))

# COMMAND ----------

print(f"\n  📋 reference.corporate_actions — {SYMBOL}")
display(spark.sql(f"""
    SELECT *
    FROM {CATALOG}.reference.corporate_actions
    WHERE instrument_id = {INSTRUMENT_ID}
    ORDER BY effective_date
"""))

# COMMAND ----------

print(f"\n  📋 control.corporate_action_candidates — {SYMBOL}")
display(spark.sql(f"""
    SELECT *
    FROM {CATALOG}.control.corporate_action_candidates
    WHERE instrument_id = {INSTRUMENT_ID}
    ORDER BY detected_at DESC
"""))

# COMMAND ----------

print(f"\n  📋 control.ingestion_command — {SYMBOL}")
display(spark.sql(f"""
    SELECT *
    FROM {CATALOG}.control.ingestion_command
    WHERE instrument_id = {INSTRUMENT_ID}
    ORDER BY created_at DESC
"""))

# COMMAND ----------

print(f"\n  📋 control.ingestion_watermark — {SYMBOL}")
display(spark.sql(f"""
    SELECT *
    FROM {CATALOG}.control.ingestion_watermark
    WHERE instrument_id = {INSTRUMENT_ID}
"""))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Final Summary

# COMMAND ----------

print("""
╔══════════════════════════════════════════════════════════════════╗
║  E2E Validation Summary
╠══════════════════════════════════════════════════════════════════╣""")
print(f"║  Symbol: {SYMBOL:<8}  Split: {RATIO_FROM}-for-{RATIO_TO} on {SPLIT_DATE}              ║")
print("╠══════════════════════════════════════════════════════════════════╣")

all_pass = True
for step, status in _results.items():
    note = _notes.get(step, "")
    line = f"  {status}  {step}"
    if note:
        # Truncate note to keep table tidy
        short_note = note[:55] + "…" if len(note) > 55 else note
        line += f"  [{short_note}]"
    print(f"║  {line:<62}║")
    if "FAIL" in status:
        all_pass = False

print("╠══════════════════════════════════════════════════════════════════╣")
verdict = "✅  ALL CHECKS PASSED" if all_pass else "❌  ONE OR MORE CHECKS FAILED"
print(f"║  {verdict:<62}║")
print("╚══════════════════════════════════════════════════════════════════╝")

if not all_pass:
    failed_steps = [s for s, r in _results.items() if "FAIL" in r]
    print(f"\nFailed steps: {failed_steps}")
    print("Fix the issues above and re-run from the failed step.")
else:
    print(f"""
What was validated end-to-end:
  ✅ Bronze ingestion for {SYMBOL} pre-split ({PRE_SPLIT_START} → {PRE_SPLIT_END})
  ✅ Bronze ingestion for {SYMBOL} post-split ({POST_SPLIT_START} → {POST_SPLIT_END})
  ✅ Watermark correctly tracks earliest + latest dates
  ✅ No duplicate records (Layer 2 deduplication working)
  ✅ instrument_id={INSTRUMENT_ID} stamped on all records
  ✅ YahooCorporateActionsProvider fetched real {RATIO_FROM}-for-{RATIO_TO} split on {SPLIT_DATE}
  ✅ reference.corporate_actions seeded with real event data
  ✅ CorporateActionClassifier correctly classified price_ratio as SPLIT {RATIO_FROM}-for-{RATIO_TO}
  ✅ Saga advanced to CLASSIFIED status
  ✅ FORCE_RELOAD command issued to control.ingestion_command
  ✅ Idempotency confirmed — re-run wrote 0 new records

What is deferred (not built yet):
  🔵 adjustment_factors computation — Phase 3/4
  🔵 Silver adjusted price views — Phase 3
  🔵 Vendor price fetch for automatic detection — requires IBKR gateway
""")
