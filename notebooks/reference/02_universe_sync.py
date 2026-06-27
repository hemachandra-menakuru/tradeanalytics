# Databricks notebook source
# MAGIC %md
# MAGIC # Universe Sync
# MAGIC
# MAGIC **Schedule:** Weekly (Sunday 2am) via Databricks Workflows.
# MAGIC **What it does:**
# MAGIC 1. Downloads ALL US-listed securities from NASDAQ Trader FTP → saves raw files to S3
# MAGIC 2. Downloads SPY + QQQ ETF holdings → saves raw files to S3
# MAGIC 3. Enriches new instruments with FIGI + ISIN via OpenFIGI API
# MAGIC 4. Writes to reference.instrument, instrument_listing, universe_membership
# MAGIC 5. Seeds reference.ticker_feed_config for our active instruments
# MAGIC
# MAGIC **Widgets:**
# MAGIC - `dry_run`      — true = preview changes, no writes (default: true)
# MAGIC - `etf_only`     — true = skip NASDAQ FTP, only sync ETF universe
# MAGIC - `skip_ibkr`    — true = skip IBKR con_id enrichment (default: true — gateway not reachable from cloud)
# MAGIC - OpenFIGI API key is read automatically from Databricks Secrets (`tradeanalytics/openfigi-api-key`)

# COMMAND ----------

# MAGIC %pip install requests openpyxl boto3 --quiet

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md ## Cell 1 — Widgets

# COMMAND ----------

dbutils.widgets.dropdown("dry_run",    "true",  ["true", "false"], "Dry Run")
dbutils.widgets.dropdown("etf_only",  "false", ["true", "false"], "ETF Only (skip NASDAQ FTP)")
dbutils.widgets.dropdown("skip_ibkr", "true",  ["true", "false"], "Skip IBKR Enrichment")

DRY_RUN   = dbutils.widgets.get("dry_run")   == "true"
ETF_ONLY  = dbutils.widgets.get("etf_only")  == "true"
SKIP_IBKR = dbutils.widgets.get("skip_ibkr") == "true"

# Read OpenFIGI key from Databricks Secrets — never hardcode credentials
try:
    OPENFIGI_KEY = dbutils.secrets.get(scope="tradeanalytics", key="openfigi-api-key")
except Exception:
    OPENFIGI_KEY = None
    logger.warning("OpenFIGI API key not found in Databricks Secrets — using free tier (10 items/batch)")

print(f"dry_run={DRY_RUN}  etf_only={ETF_ONLY}  skip_ibkr={SKIP_IBKR}")
if DRY_RUN:
    print("DRY RUN — no writes will be performed")
if SKIP_IBKR:
    print("IBKR enrichment skipped — run locally via Databricks Connect with gateway active to populate ibkr_con_id")

# COMMAND ----------

# MAGIC %md ## Cell 2 — Setup

# COMMAND ----------

import sys
import logging
import boto3
from datetime import date, datetime, timezone
from typing import Dict, List

from pyspark.sql.types import (
    StructType, StructField,
    StringType, BooleanType, IntegerType, LongType, DoubleType, DateType, TimestampType
)

sys.path.insert(0, "/Workspace/Repos/hemachandra-menakuru/tradeanalytics")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("universe_sync")

CATALOG    = "tradeanalytics"
RAW_BUCKET = "handh-trade-raw-use1"
TODAY      = date.today()
NOW        = datetime.now(timezone.utc)
SYNC_BATCH = f"NASDAQ_FTP_{TODAY.strftime('%Y%m%d')}"   # used as source marker to recover instrument_ids after batch insert

ETF_ACTIVE_UNIVERSES = {"SP500", "NDX100"}   # IWM / RUSSELL2000 = is_active False by default

print(f"catalog={CATALOG}  today={TODAY}  sync_batch={SYNC_BATCH}")

# COMMAND ----------

# MAGIC %md ## Cell 3 — Load existing instruments from Delta
# MAGIC
# MAGIC We pre-load the current state of reference.instrument + instrument_listing into memory.
# MAGIC This tells us which symbols already exist so we can classify each incoming instrument as:
# MAGIC - NEW    → INSERT into instrument + instrument_listing
# MAGIC - EXISTS → skip instrument insert, check if listing needs updating

# COMMAND ----------

existing_df = spark.sql(f"""
    SELECT
        i.instrument_id,
        i.isin,
        i.figi,
        i.ibkr_con_id,
        i.asset_class,
        l.symbol,
        l.exchange_mic,
        l.is_current
    FROM {CATALOG}.reference.instrument i
    JOIN {CATALOG}.reference.instrument_listing l
      ON i.instrument_id = l.instrument_id
     AND l.is_current = true
""")

# symbol → {instrument_id, isin, figi, ...}
existing: Dict[str, dict] = {
    row["symbol"]: row.asDict()
    for row in existing_df.collect()
}

print(f"Existing instruments in Delta: {len(existing)}")

# COMMAND ----------

# MAGIC %md ## Cell 4 — Fetch from external sources + save raw files to S3
# MAGIC
# MAGIC **Why save to S3 first?**
# MAGIC Every source file is saved byte-for-byte to S3 before parsing.
# MAGIC If parsing has a bug, we replay from S3 without re-downloading.
# MAGIC Same medallion principle as OHLCV market data in the bronze layer.
# MAGIC
# MAGIC S3 path: `s3://handh-trade-raw-use1/reference/{source}/{YYYY-MM-DD}/`

# COMMAND ----------

from src.reference.sources.nasdaq_ftp_source import NasdaqFtpSource
from src.reference.sources.etf_holdings_source import EtfHoldingsSource

s3 = boto3.client("s3")

def save_to_s3(content: str, key: str) -> None:
    """Save raw text content to S3 for audit + replay."""
    if not DRY_RUN:
        s3.put_object(Bucket=RAW_BUCKET, Key=key, Body=content.encode("utf-8"))
        logger.info(f"Saved to s3://{RAW_BUCKET}/{key}")
    else:
        logger.info(f"DRY RUN — would save to s3://{RAW_BUCKET}/{key}")

# ── ETF Holdings ──────────────────────────────────────────────────────────────
import requests as _requests

etf_raw_texts = {}
for etf_symbol, url, fmt in [
    ("SPY", "https://www.ssga.com/us/en/institutional/etfs/library-content/products/fund-data/etfs/us/holdings-daily-us-en-spy.xlsx", "xlsx"),
    ("QQQ", "https://www.invesco.com/us/financial-products/etfs/holdings/main/holdings/0?audienceType=Investor&ticker=QQQ", "csv"),
]:
    try:
        resp = _requests.get(url, timeout=30, headers={"User-Agent": "TradeAnalytics/1.0"})
        resp.raise_for_status()
        if fmt == "xlsx":
            key = f"reference/etf_holdings/{TODAY.strftime('%Y-%m-%d')}/{etf_symbol.lower()}_holdings.xlsx"
            if not DRY_RUN:
                s3.put_object(Bucket=RAW_BUCKET, Key=key, Body=resp.content)
            etf_raw_texts[etf_symbol] = resp.content   # bytes for xlsx
        else:
            key = f"reference/etf_holdings/{TODAY.strftime('%Y-%m-%d')}/{etf_symbol.lower()}_holdings.csv"
            save_to_s3(resp.text, key)
            etf_raw_texts[etf_symbol] = resp.text
        logger.info(f"{etf_symbol} holdings downloaded ({len(resp.content):,} bytes)")
    except Exception as e:
        logger.error(f"Failed to download {etf_symbol} holdings: {e}")

# Parse ETF holdings via our existing source client
etf_source      = EtfHoldingsSource()
etf_instruments = etf_source.fetch()

# symbol → {universe_codes, index_weight, source_etf, is_active}
etf_lookup: Dict[str, dict] = {}
for inst in etf_instruments:
    if inst.symbol not in etf_lookup:
        etf_lookup[inst.symbol] = {
            "universe_codes": list(inst.universe_codes),
            "index_weight":   inst.index_weight,
            "source_etf":     inst.source_etf,
            "is_active":      any(u in ETF_ACTIVE_UNIVERSES for u in inst.universe_codes),
        }
    else:
        for code in inst.universe_codes:
            if code not in etf_lookup[inst.symbol]["universe_codes"]:
                etf_lookup[inst.symbol]["universe_codes"].append(code)
        if any(u in ETF_ACTIVE_UNIVERSES for u in inst.universe_codes):
            etf_lookup[inst.symbol]["is_active"] = True

print(f"ETF holdings: {len(etf_lookup)} unique symbols")
print(f"  Active (SPY/QQQ): {sum(1 for v in etf_lookup.values() if v['is_active'])}")
print(f"  Inactive (IWM):   {sum(1 for v in etf_lookup.values() if not v['is_active'])}")

# ── NASDAQ FTP ────────────────────────────────────────────────────────────────
if ETF_ONLY:
    nasdaq_instruments = []
    print("etf_only=true — skipping NASDAQ FTP fetch")
else:
    nasdaq_source = NasdaqFtpSource()
    # Download raw text files first, save to S3, then parse
    import requests as req
    for filename, url in [
        ("nasdaqlisted.txt",  "http://ftp.nasdaqtrader.com/dynamic/SymbolDirectory/nasdaqlisted.txt"),
        ("otherlisted.txt",   "http://ftp.nasdaqtrader.com/dynamic/SymbolDirectory/otherlisted.txt"),
    ]:
        try:
            r = req.get(url, timeout=30)
            r.raise_for_status()
            save_to_s3(r.text, f"reference/nasdaq_ftp/{TODAY.strftime('%Y-%m-%d')}/{filename}")
        except Exception as e:
            logger.error(f"Failed to save {filename} to S3: {e}")

    nasdaq_instruments = nasdaq_source.fetch()
    print(f"NASDAQ FTP: {len(nasdaq_instruments)} instruments")

# ── Merge all sources ─────────────────────────────────────────────────────────
all_map: Dict[str, object] = {}
for inst in nasdaq_instruments:
    all_map[inst.symbol] = inst
for inst in etf_instruments:
    if inst.symbol in all_map:
        # ETF data enriches the NASDAQ record with universe_codes
        all_map[inst.symbol].universe_codes = inst.universe_codes
        all_map[inst.symbol].index_weight   = inst.index_weight
        all_map[inst.symbol].source_etf     = inst.source_etf
    else:
        all_map[inst.symbol] = inst

all_instruments = list(all_map.values())
print(f"Total unique instruments to process: {len(all_instruments)}")

# COMMAND ----------

# MAGIC %md ## Cell 5 — OpenFIGI Enrichment
# MAGIC
# MAGIC Only enrich instruments that:
# MAGIC   1. Are not already in Delta (new instruments only)
# MAGIC   2. OR are in Delta but missing FIGI (enrichment gap)
# MAGIC
# MAGIC We only enrich ETF universe members (~600) — not all 10,000 NASDAQ instruments.
# MAGIC There is no value in getting FIGI for instruments we have no plans to ingest.

# COMMAND ----------

from src.reference.sources.openfigi_client import OpenFigiClient

# Only enrich ETF universe members not already enriched
to_enrich = [
    inst for inst in all_instruments
    if inst.symbol in etf_lookup                           # is a universe member
    and (
        inst.symbol not in existing                        # brand new
        or not existing[inst.symbol].get("figi")          # or missing FIGI in Delta
    )
]

print(f"Instruments needing OpenFIGI enrichment: {len(to_enrich)}")

if to_enrich:
    client   = OpenFigiClient(api_key=OPENFIGI_KEY)
    client.enrich(to_enrich)
    enriched = sum(1 for i in to_enrich if i.figi)
    print(f"OpenFIGI: enriched {enriched}/{len(to_enrich)} ({len(to_enrich)-enriched} not found)")

# COMMAND ----------

# MAGIC %md ## Cell 6 — IBKR con_id Enrichment (local Databricks Connect only)
# MAGIC
# MAGIC `ibkr_con_id` is required for every IBKR API call during ingestion.
# MAGIC The IBKR gateway runs on localhost:5055 on the local Mac — unreachable from Databricks cloud.
# MAGIC
# MAGIC Run this notebook locally via Databricks Connect with `skip_ibkr=false` and gateway active.
# MAGIC The IBKRProvider also maintains a file-persisted con_id cache at:
# MAGIC `~/.tradeanalytics/ibkr_conid_cache.json` — so this step only needs to run once per new instrument.

# COMMAND ----------

if not SKIP_IBKR:
    from src.reference.sources.ibkr_contract_client import IbkrContractClient
    ibkr_client = IbkrContractClient(base_url="https://localhost:5055/v1/api")
    if not ibkr_client.health_check():
        print("IBKR gateway not reachable — skipping con_id enrichment")
        print("Authenticate first: curl -sk -X POST https://localhost:5055/v1/api/iserver/reauthenticate")
    else:
        new_only = [i for i in all_instruments if i.symbol not in existing]
        ibkr_client.enrich(new_only)
        resolved = sum(1 for i in new_only if i.ibkr_con_id is not None)
        print(f"IBKR: resolved {resolved}/{len(new_only)} con_ids")
else:
    print("Skipped (skip_ibkr=true)")

# COMMAND ----------

# MAGIC %md ## Cell 7 — Classify instruments (new vs existing)

# COMMAND ----------

new_listings: List[dict] = []

for inst in all_instruments:
    if inst.symbol not in existing:
        new_listings.append({
            "symbol":        inst.symbol,
            "company_name":  inst.company_name,
            "asset_class":   inst.asset_class,
            "exchange":      inst.exchange,
            "exchange_mic":  inst.exchange_mic,
            "currency":      "USD",
            "isin":          getattr(inst, "isin",         None),
            "figi":          getattr(inst, "figi",         None),
            "ibkr_con_id":   getattr(inst, "ibkr_con_id",  None),
            "universe_codes":getattr(inst, "universe_codes", []),
            "index_weight":  getattr(inst, "index_weight",  None),
            "source_etf":    getattr(inst, "source_etf",    None),
        })

print(f"New instruments to insert: {len(new_listings)}")
print(f"Already in Delta:          {len(existing)}")

if new_listings:
    print(f"\nSample new ({min(5, len(new_listings))}):")
    for item in new_listings[:5]:
        print(f"  {item['symbol']:8} | {item['asset_class']:7} | figi={item['figi']} | {item['company_name'][:35]}")

# COMMAND ----------

# MAGIC %md ## Cell 8 — Insert new instruments into reference.instrument
# MAGIC
# MAGIC **Why we use a SYNC_BATCH source marker:**
# MAGIC `instrument_id` is GENERATED ALWAYS AS IDENTITY — Delta assigns it automatically.
# MAGIC After the batch insert, we need those IDs to write instrument_listing (which has instrument_id as FK).
# MAGIC We tag every row with source = SYNC_BATCH, then immediately read back filtered by that tag
# MAGIC to recover the instrument_id → (isin, figi) mapping, which we use to build the listing rows.

# COMMAND ----------

if not DRY_RUN and new_listings:
    instrument_schema = StructType([
        StructField("isin",         StringType(),    True),
        StructField("figi",         StringType(),    True),
        StructField("ibkr_con_id",  LongType(),      True),
        StructField("asset_class",  StringType(),    False),
        StructField("is_active",    BooleanType(),   False),
        StructField("source",       StringType(),    True),
        StructField("created_at",   TimestampType(), False),
        StructField("updated_at",   TimestampType(), False),
    ])

    instrument_rows = [
        (
            item["isin"],
            item["figi"],
            item["ibkr_con_id"],
            item["asset_class"],
            False,          # is_active=False — activation is controlled via ticker_feed_config
            SYNC_BATCH,     # source marker — used to recover instrument_ids below
            NOW,
            NOW,
        )
        for item in new_listings
    ]

    spark.createDataFrame(instrument_rows, schema=instrument_schema) \
         .write.mode("append") \
         .saveAsTable(f"{CATALOG}.reference.instrument")

    print(f"Inserted {len(new_listings)} new instruments into reference.instrument")

elif DRY_RUN:
    print(f"DRY RUN — would insert {len(new_listings)} instruments")

# COMMAND ----------

# MAGIC %md ## Cell 9 — Recover instrument_ids and insert instrument_listing
# MAGIC
# MAGIC After the batch insert above, we read back rows tagged with SYNC_BATCH to get the
# MAGIC instrument_ids Delta assigned. We join on (isin, figi) to map each symbol back to
# MAGIC its instrument_id, then write the instrument_listing rows.

# COMMAND ----------

if not DRY_RUN and new_listings:
    # Read back the rows we just inserted — keyed by SYNC_BATCH
    inserted_df = spark.sql(f"""
        SELECT instrument_id, isin, figi, asset_class
        FROM {CATALOG}.reference.instrument
        WHERE source = '{SYNC_BATCH}'
    """)
    inserted_rows = {
        (row["isin"], row["figi"]): row["instrument_id"]
        for row in inserted_df.collect()
    }

    # Build instrument_listing rows using the recovered instrument_ids
    listing_schema = StructType([
        StructField("instrument_id", LongType(),    False),
        StructField("symbol",        StringType(),  False),
        StructField("exchange",      StringType(),  True),
        StructField("exchange_mic",  StringType(),  True),
        StructField("currency",      StringType(),  False),
        StructField("company_name",  StringType(),  True),
        StructField("valid_from",    DateType(),    False),
        StructField("is_current",    BooleanType(), False),
        StructField("created_at",    TimestampType(), False),
    ])

    listing_rows = []
    skipped      = 0
    for item in new_listings:
        # Look up instrument_id by (isin, figi) — primary join keys
        instrument_id = inserted_rows.get((item["isin"], item["figi"]))

        # Fallback chain: two separate `if` checks (NOT elif) so each level
        # independently retries if the previous attempt returned None
        if instrument_id is None:
            instrument_id = inserted_rows.get((item["isin"], None))   # isin only — figi was None
        if instrument_id is None:
            instrument_id = inserted_rows.get((None, item["figi"]))   # figi only — isin was None

        if instrument_id is None:
            logger.warning(f"Could not recover instrument_id for {item['symbol']} (isin={item['isin']}, figi={item['figi']}) — skipping listing")
            skipped += 1
            continue

        listing_rows.append((
            instrument_id,
            item["symbol"],
            item["exchange"],
            item["exchange_mic"],
            item["currency"],
            item["company_name"],
            TODAY,       # valid_from = today (first observed date, not actual IPO date)
            True,        # is_current = True
            NOW,
        ))

    if listing_rows:
        spark.createDataFrame(listing_rows, schema=listing_schema) \
             .write.mode("append") \
             .saveAsTable(f"{CATALOG}.reference.instrument_listing")
        print(f"Inserted {len(listing_rows)} rows into reference.instrument_listing")

    if skipped:
        print(f"Warning: {skipped} instruments skipped (could not recover instrument_id — both isin and figi are NULL)")

elif DRY_RUN:
    print(f"DRY RUN — would insert {len(new_listings)} listing rows")

# COMMAND ----------

# MAGIC %md ## Cell 10 — Insert universe membership
# MAGIC
# MAGIC For each ETF universe member (SP500, NDX100), insert a row into universe_membership.
# MAGIC We only insert for instruments that are universe members — not for all 10,000 NASDAQ listings.
# MAGIC An instrument can appear in multiple universes (AAPL is in both SP500 and NDX100).

# COMMAND ----------

if not DRY_RUN:
    # Build full symbol → instrument_id map (existing + newly inserted)
    symbol_to_id: Dict[str, int] = {sym: row["instrument_id"] for sym, row in existing.items()}

    if new_listings:
        inserted_df = spark.sql(f"""
            SELECT i.instrument_id, l.symbol
            FROM {CATALOG}.reference.instrument i
            JOIN {CATALOG}.reference.instrument_listing l
              ON i.instrument_id = l.instrument_id AND l.is_current = true
            WHERE i.source = '{SYNC_BATCH}'
        """)
        for row in inserted_df.collect():
            symbol_to_id[row["symbol"]] = row["instrument_id"]

    # Load existing memberships so we don't duplicate
    existing_memberships = set()
    try:
        mb_df = spark.sql(f"""
            SELECT instrument_id, universe_code
            FROM {CATALOG}.reference.universe_membership
            WHERE is_active = true
        """)
        existing_memberships = {(r["instrument_id"], r["universe_code"]) for r in mb_df.collect()}
    except Exception:
        pass

    membership_schema = StructType([
        StructField("instrument_id",  LongType(),    False),
        StructField("universe_code",  StringType(),  False),
        StructField("index_weight",   DoubleType(),  True),
        StructField("source_etf",     StringType(),  True),
        StructField("is_active",      BooleanType(), False),
        StructField("added_date",     DateType(),    False),
        StructField("created_at",     TimestampType(), False),
        StructField("updated_at",     TimestampType(), False),
    ])

    membership_rows = []
    for symbol, meta in etf_lookup.items():
        instrument_id = symbol_to_id.get(symbol)
        if instrument_id is None:
            continue
        for universe_code in meta["universe_codes"]:
            if (instrument_id, universe_code) not in existing_memberships:
                membership_rows.append((
                    instrument_id,
                    universe_code,
                    meta["index_weight"],
                    meta["source_etf"],
                    meta["is_active"],
                    TODAY,
                    NOW,
                    NOW,
                ))

    if membership_rows:
        spark.createDataFrame(membership_rows, schema=membership_schema) \
             .write.mode("append") \
             .saveAsTable(f"{CATALOG}.reference.universe_membership")
        print(f"Inserted {len(membership_rows)} universe membership rows")
    else:
        print("No new universe memberships to insert")

elif DRY_RUN:
    etf_members = sum(1 for sym in etf_lookup if sym in {i.symbol for i in all_instruments})
    print(f"DRY RUN — would insert up to {etf_members * 2} membership rows (SP500 + NDX100)")

# COMMAND ----------

# MAGIC %md ## Cell 11 — Seed reference.ticker_feed_config
# MAGIC
# MAGIC This seeds the DESIRED STATE for our 8 active instruments.
# MAGIC These are the only instruments that will flow through the bronze ingestion pipeline.
# MAGIC All other instruments in reference.instrument have is_active=false and no feed config.
# MAGIC
# MAGIC The history_start dates come from tickers.csv (our original bootstrap config).
# MAGIC Once this table is seeded, tickers.csv is no longer the source of truth.

# COMMAND ----------

# Our 8 active instruments with their ingestion preferences
ACTIVE_INSTRUMENTS = [
    {"symbol": "AAPL",  "history_start": "2016-01-01", "batch_group": "A", "priority": 1},
    {"symbol": "MSFT",  "history_start": "2016-01-01", "batch_group": "A", "priority": 2},
    {"symbol": "NVDA",  "history_start": "2016-01-01", "batch_group": "A", "priority": 3},
    {"symbol": "GOOGL", "history_start": "2016-01-01", "batch_group": "A", "priority": 4},
    {"symbol": "AMZN",  "history_start": "2016-01-01", "batch_group": "A", "priority": 5},
    {"symbol": "JPM",   "history_start": "2016-01-01", "batch_group": "B", "priority": 1},
    {"symbol": "SPY",   "history_start": "2016-01-01", "batch_group": "B", "priority": 2},
    {"symbol": "QQQ",   "history_start": "2016-01-01", "batch_group": "B", "priority": 3},
]

# SPY and QQQ are ETFs — they hold other stocks so they never appear as constituents
# of their own holdings files. Seed them explicitly into reference.instrument as etf asset_class.
ETF_INSTRUMENTS = [
    {"symbol": "SPY", "company_name": "SPDR S&P 500 ETF Trust",      "asset_class": "etf", "exchange": "NYSE Arca", "exchange_mic": "ARCX"},
    {"symbol": "QQQ", "company_name": "Invesco QQQ Trust Series 1",  "asset_class": "etf", "exchange": "NASDAQ",   "exchange_mic": "XNAS"},
]

if not DRY_RUN:
    # Correct asset_class for SPY/QQQ if they were previously inserted as 'equity'
    for etf in ETF_INSTRUMENTS:
        if etf["symbol"] in existing and existing[etf["symbol"]].get("asset_class") != etf["asset_class"]:
            instrument_id = existing[etf["symbol"]]["instrument_id"]
            spark.sql(f"""
                UPDATE {CATALOG}.reference.instrument
                SET asset_class = '{etf["asset_class"]}', updated_at = current_timestamp()
                WHERE instrument_id = {instrument_id}
            """)
            logger.info(f"Corrected {etf['symbol']} asset_class to {etf['asset_class']}")

    # Ensure SPY/QQQ are in new_listings if not already in Delta
    for etf in ETF_INSTRUMENTS:
        if etf["symbol"] not in existing and not any(r.get("symbol") == etf["symbol"] for r in new_listings):
            new_listings.append({
                "symbol":       etf["symbol"],
                "company_name": etf["company_name"],
                "asset_class":  etf["asset_class"],
                "exchange":     etf["exchange"],
                "exchange_mic": etf["exchange_mic"],
                "currency":     "USD",
                "isin":         None,
                "figi":         None,
                "ibkr_con_id":  None,
            })
            logger.info(f"Added {etf['symbol']} as explicit ETF seed instrument")

    # Build symbol → instrument_id map (need it for FK)
    symbol_to_id: Dict[str, int] = {sym: row["instrument_id"] for sym, row in existing.items()}

    if new_listings:
        inserted_df = spark.sql(f"""
            SELECT i.instrument_id, l.symbol
            FROM {CATALOG}.reference.instrument i
            JOIN {CATALOG}.reference.instrument_listing l
              ON i.instrument_id = l.instrument_id AND l.is_current = true
            WHERE i.source = '{SYNC_BATCH}'
        """)
        for row in inserted_df.collect():
            symbol_to_id[row["symbol"]] = row["instrument_id"]

    # Existing ticker_feed_config entries (skip if already seeded)
    existing_configs = set()
    try:
        cfg_df = spark.sql(f"""
            SELECT instrument_id FROM {CATALOG}.reference.ticker_feed_config
            WHERE stream = 'daily'
        """)
        existing_configs = {r["instrument_id"] for r in cfg_df.collect()}
    except Exception:
        pass

    feed_schema = StructType([
        StructField("instrument_id",     LongType(),      False),
        StructField("stream",            StringType(),    False),
        StructField("target_start_date", DateType(),      False),
        StructField("run_frequency",     StringType(),    False),
        StructField("batch_group",       StringType(),    False),
        StructField("priority",          IntegerType(),   False),
        StructField("is_active",         BooleanType(),   False),
        StructField("max_lookback_days", IntegerType(),   False),
        StructField("created_at",        TimestampType(), False),
        StructField("updated_at",        TimestampType(), False),
        StructField("created_by",        StringType(),    True),
    ])

    feed_rows = []
    missing   = []
    for item in ACTIVE_INSTRUMENTS:
        instrument_id = symbol_to_id.get(item["symbol"])
        if instrument_id is None:
            missing.append(item["symbol"])
            continue
        if instrument_id in existing_configs:
            print(f"  Skipped {item['symbol']} — feed config already exists")
            continue

        from datetime import date as _date
        start_parts = item["history_start"].split("-")
        target_start = _date(int(start_parts[0]), int(start_parts[1]), int(start_parts[2]))

        feed_rows.append((
            instrument_id,
            "daily",
            target_start,
            "daily",
            item["batch_group"],
            item["priority"],
            True,           # is_active = True
            365,            # max_lookback_days
            NOW,
            NOW,
            "universe_sync_notebook",
        ))

    if feed_rows:
        spark.createDataFrame(feed_rows, schema=feed_schema) \
             .write.mode("append") \
             .saveAsTable(f"{CATALOG}.reference.ticker_feed_config")
        print(f"Seeded {len(feed_rows)} rows into reference.ticker_feed_config")

    if missing:
        print(f"Warning: could not find instrument_id for: {missing}")
        print("These symbols may not have been inserted yet — re-run after NASDAQ FTP + ETF sync completes")

elif DRY_RUN:
    print(f"DRY RUN — would seed {len(ACTIVE_INSTRUMENTS)} rows into reference.ticker_feed_config")
    for item in ACTIVE_INSTRUMENTS:
        print(f"  {item['symbol']:8} | batch_group={item['batch_group']} | priority={item['priority']} | history_start={item['history_start']}")

# COMMAND ----------

# MAGIC %md ## Cell 12 — Summary

# COMMAND ----------

if not DRY_RUN:
    print("=" * 60)
    print("  Universe Sync — Complete")
    print("=" * 60)

    spark.sql(f"""
        SELECT asset_class,
               COUNT(*)                                          AS total_instruments,
               SUM(CASE WHEN um.instrument_id IS NOT NULL THEN 1 ELSE 0 END) AS universe_members,
               SUM(CASE WHEN fc.instrument_id IS NOT NULL THEN 1 ELSE 0 END) AS active_feed_configs
        FROM {CATALOG}.reference.instrument i
        LEFT JOIN {CATALOG}.reference.universe_membership um
               ON i.instrument_id = um.instrument_id AND um.is_active = true
        LEFT JOIN {CATALOG}.reference.ticker_feed_config fc
               ON i.instrument_id = fc.instrument_id AND fc.is_active = true AND fc.stream = 'daily'
        GROUP BY asset_class
        ORDER BY asset_class
    """).show()

    spark.sql(f"""
        SELECT fc.batch_group, l.symbol, fc.target_start_date, fc.priority
        FROM {CATALOG}.reference.ticker_feed_config fc
        JOIN {CATALOG}.reference.instrument_listing l
          ON fc.instrument_id = l.instrument_id AND l.is_current = true
        WHERE fc.stream = 'daily' AND fc.is_active = true
        ORDER BY fc.batch_group, fc.priority
    """).show()
else:
    print("DRY RUN complete — set dry_run=false to execute")
