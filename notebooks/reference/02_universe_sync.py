# Databricks notebook source
# Universe Sync Job — runs monthly via Databricks Workflows.
#
# What this does:
#   1. Downloads ALL US-listed securities from NASDAQ Trader FTP
#      → Populates reference.instrument as comprehensive master registry
#   2. Downloads SPY + QQQ + IWM ETF holdings
#      → Sets is_active in reference.instrument_feed_config
#   3. Enriches with FIGI + ISIN via OpenFIGI API
#   4. Validates tradeability via IBKR contract search
#   5. Reconciles changes:
#      - New listing    → INSERT instrument + listing + feed_config (is_active=false)
#      - Symbol change  → SCD Type 2 on instrument_listing
#      - Delisted       → is_active=false in feed_config
#      - Index addition → set is_active per ETF default (SPY/QQQ=true, IWM=false)
#
# Widgets:
#   dry_run        — "true" to preview changes without writing (default: true)
#   etf_only       — "true" to skip NASDAQ FTP, only sync ETF membership
#   openfigi_key   — optional API key for higher rate limits

# COMMAND ----------

import sys
import os
import logging
from datetime import date, datetime, timezone
from typing import Dict, List, Optional, Set, Tuple

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("universe_sync")

# COMMAND ----------
# MAGIC %md ## Widgets

# COMMAND ----------

dbutils.widgets.dropdown("dry_run",    "true",  ["true", "false"])
dbutils.widgets.dropdown("etf_only",  "false", ["true", "false"])
dbutils.widgets.dropdown("skip_ibkr", "true",  ["true", "false"])
dbutils.widgets.text("openfigi_key", "")

DRY_RUN      = dbutils.widgets.get("dry_run")    == "true"
ETF_ONLY     = dbutils.widgets.get("etf_only")   == "true"
SKIP_IBKR    = dbutils.widgets.get("skip_ibkr")  == "true"   # True when running in Databricks cloud
OPENFIGI_KEY = dbutils.widgets.get("openfigi_key").strip() or None

# IBKR gateway is only reachable from local Mac (localhost:5055)
# skip_ibkr=true (default) when running as a scheduled Databricks job
# skip_ibkr=false only when running locally via Databricks Connect with gateway active
if SKIP_IBKR:
    print("⚠️  skip_ibkr=true — IBKR con_id enrichment will be skipped")
    print("   Run locally via Databricks Connect with gateway active to populate con_id")

CATALOG  = "tradeanalytics"
SCHEMA   = "reference"
TODAY    = date.today()
NOW      = datetime.now(timezone.utc)

print(f"dry_run={DRY_RUN}  etf_only={ETF_ONLY}  as_of={TODAY}")

# COMMAND ----------
# MAGIC %md ## Step 1 — Load existing instruments from Delta

# COMMAND ----------

# Load existing instruments keyed by symbol (current listings only)
existing_df = spark.sql(f"""
    SELECT
        i.instrument_id,
        i.isin,
        i.figi,
        i.ibkr_con_id,
        i.asset_class,
        i.is_active       AS instrument_is_active,
        l.listing_id,
        l.symbol,
        l.company_name,
        l.exchange,
        l.exchange_mic,
        l.is_current
    FROM {CATALOG}.{SCHEMA}.instrument i
    JOIN {CATALOG}.{SCHEMA}.instrument_listing l
      ON i.instrument_id = l.instrument_id
     AND l.is_current = true
""")

existing = {
    row["symbol"]: row.asDict()
    for row in existing_df.collect()
}

print(f"Existing instruments in Delta: {len(existing)}")

# Load existing feed configs
feed_df = spark.sql(f"""
    SELECT instrument_id, stream_name, is_active
    FROM {CATALOG}.{SCHEMA}.instrument_feed_config
    WHERE stream_name = 'daily'
""")
existing_feed = {
    row["instrument_id"]: row.asDict()
    for row in feed_df.collect()
}

print(f"Existing feed configs: {len(existing_feed)}")

# COMMAND ----------
# MAGIC %md ## Step 2 — Fetch from external sources

# COMMAND ----------

sys.path.insert(0, "/Workspace/Repos/hemachandra-menakuru/tradeanalytics")

from src.reference.sources.nasdaq_ftp_source import NasdaqFtpSource
from src.reference.sources.etf_holdings_source import EtfHoldingsSource
from src.reference.sources.openfigi_client import OpenFigiClient

# Fetch ETF holdings (SPY / QQQ / IWM)
etf_source   = EtfHoldingsSource()
etf_instruments = etf_source.fetch()

# Build ETF membership lookup: symbol → {universe_codes, index_weight, source_etf, is_active}
# SPY/QQQ = is_active True, IWM = is_active False
ETF_ACTIVE_UNIVERSES = {"SP500", "NDX100"}   # IWM / RUSSELL2000 is False by default

etf_lookup: Dict[str, dict] = {}
for inst in etf_instruments:
    if inst.symbol not in etf_lookup:
        etf_lookup[inst.symbol] = {
            "universe_codes": inst.universe_codes[:],
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

# Fetch NASDAQ FTP (all US listings) unless etf_only mode
if ETF_ONLY:
    nasdaq_instruments = []
    print("etf_only=true — skipping NASDAQ FTP fetch")
else:
    nasdaq_source      = NasdaqFtpSource()
    nasdaq_instruments = nasdaq_source.fetch()
    print(f"NASDAQ FTP: {len(nasdaq_instruments)} instruments")

# Merge: ETF instruments take precedence (have universe_codes)
all_instruments_map: Dict[str, "RawInstrument"] = {}
for inst in nasdaq_instruments:
    all_instruments_map[inst.symbol] = inst
for inst in etf_instruments:
    if inst.symbol in all_instruments_map:
        all_instruments_map[inst.symbol].universe_codes = inst.universe_codes
        all_instruments_map[inst.symbol].index_weight   = inst.index_weight
        all_instruments_map[inst.symbol].source_etf     = inst.source_etf
    else:
        all_instruments_map[inst.symbol] = inst

all_instruments = list(all_instruments_map.values())
print(f"Total unique instruments to process: {len(all_instruments)}")

# COMMAND ----------
# MAGIC %md ## Step 3 — OpenFIGI Enrichment (new instruments only)

# COMMAND ----------

# Only enrich instruments not already in Delta (missing FIGI)
new_symbols = [
    inst for inst in all_instruments
    if inst.symbol not in existing or not existing[inst.symbol].get("figi")
]
print(f"Instruments needing FIGI enrichment: {len(new_symbols)}")

if new_symbols:
    client = OpenFigiClient(api_key=OPENFIGI_KEY)
    client.enrich(new_symbols)
    enriched_count = sum(1 for i in new_symbols if i.figi)
    print(f"OpenFIGI: enriched {enriched_count}/{len(new_symbols)}")

# COMMAND ----------
# MAGIC %md ## Step 4 — IBKR Contract Enrichment (local Databricks Connect only)

# COMMAND ----------

# Populate ibkr_con_id for new instruments.
# Skipped when running as a scheduled Databricks cloud job (gateway unreachable).
# Run locally with skip_ibkr=false and IBKR gateway active on localhost:5055.

if SKIP_IBKR:
    print("Skipping IBKR enrichment (skip_ibkr=true)")
else:
    from src.reference.sources.ibkr_contract_client import IbkrContractClient

    ibkr_base_url = "https://localhost:5055/v1/api"
    ibkr_client   = IbkrContractClient(base_url=ibkr_base_url)

    # Health check before starting
    if not ibkr_client.health_check():
        print("⚠️  IBKR gateway not reachable or not authenticated — skipping con_id enrichment")
        print(f"   Start gateway at {ibkr_base_url} and authenticate before re-running")
    else:
        # Only enrich brand-new instruments — not the full list every month
        new_only = [
            inst for inst in all_instruments
            if inst.symbol not in existing      # not in Delta yet
        ]
        print(f"IBKR enrichment: resolving con_id for {len(new_only)} new instruments")
        ibkr_client.enrich(new_only)

        resolved = sum(1 for i in new_only if i.ibkr_con_id is not None)
        print(f"IBKR enrichment: resolved {resolved}/{len(new_only)} con_ids")

# COMMAND ----------
# MAGIC %md ## Step 5 — Reconcile and classify changes

# COMMAND ----------

new_listings:      List[dict] = []   # brand new instruments
symbol_changes:    List[dict] = []   # existing instrument, new symbol (SCD Type 2)
delistings:        List[str]  = []   # in Delta but not in source anymore
feed_activations:  List[dict] = []   # need is_active update in feed_config
feed_inserts:      List[dict] = []   # new feed_config rows needed

incoming_symbols: Set[str] = {i.symbol for i in all_instruments}

# Find delistings: in Delta but not in incoming source
for symbol, row in existing.items():
    if symbol not in incoming_symbols:
        delistings.append(symbol)

# Process each incoming instrument
for inst in all_instruments:
    etf_meta    = etf_lookup.get(inst.symbol, {})
    should_be_active = etf_meta.get("is_active", False)

    if inst.symbol not in existing:
        # Brand new instrument
        new_listings.append({
            "symbol":        inst.symbol,
            "company_name":  inst.company_name,
            "asset_class":   inst.asset_class,
            "exchange":      inst.exchange,
            "exchange_mic":  inst.exchange_mic,
            "isin":          inst.isin,
            "figi":          inst.figi,
            "universe_codes":inst.universe_codes,
            "index_weight":  inst.index_weight,
            "source_etf":    inst.source_etf,
            "is_active":     should_be_active,
        })
    else:
        ex = existing[inst.symbol]
        instrument_id = ex["instrument_id"]

        # Check if feed_config needs update
        feed = existing_feed.get(instrument_id)
        if feed is None:
            feed_inserts.append({
                "instrument_id": instrument_id,
                "is_active":     should_be_active,
            })
        elif feed["is_active"] != should_be_active and not feed["is_active"]:
            # Only auto-activate, never auto-deactivate (manual control)
            if should_be_active:
                feed_activations.append({
                    "instrument_id": instrument_id,
                    "is_active":     True,
                })

print(f"\n── Reconciliation Summary ──────────────────")
print(f"  New listings:     {len(new_listings)}")
print(f"  Delistings:       {len(delistings)}")
print(f"  Feed inserts:     {len(feed_inserts)}")
print(f"  Feed activations: {len(feed_activations)}")

# COMMAND ----------
# MAGIC %md ## Step 6 — Write changes (skipped if dry_run=true)

# COMMAND ----------

if DRY_RUN:
    print("\n⚠️  DRY RUN — no writes performed")
    print(f"\nSample new listings ({min(5, len(new_listings))}):")
    for item in new_listings[:5]:
        print(f"  {item['symbol']:8} | {item['asset_class']:7} | active={item['is_active']} | {item['company_name'][:40]}")
    print(f"\nSample delistings ({min(5, len(delistings))}):")
    for sym in delistings[:5]:
        print(f"  {sym}")
else:
    from pyspark.sql.types import (
        StructType, StructField, StringType, BooleanType,
        DoubleType, LongType, DateType, TimestampType
    )

    # ── Insert new instruments ─────────────────────────────────────────────
    if new_listings:
        rows = []
        for item in new_listings:
            rows.append((
                item["isin"], item["figi"], None,   # ibkr_con_id filled later
                item["asset_class"], False,          # instrument is_active=False initially
                NOW, NOW,
            ))

        schema = StructType([
            StructField("isin",         StringType(),  True),
            StructField("figi",         StringType(),  True),
            StructField("ibkr_con_id",  LongType(),    True),
            StructField("asset_class",  StringType(),  False),
            StructField("is_active",    BooleanType(), False),
            StructField("created_at",   TimestampType(),False),
            StructField("updated_at",   TimestampType(),False),
        ])
        df = spark.createDataFrame(rows, schema)
        df.write.mode("append").saveAsTable(
            f"{CATALOG}.{SCHEMA}.instrument"
        )
        print(f"✓ Inserted {len(new_listings)} new instruments")

    # ── Delistings: deactivate feed_config ────────────────────────────────
    if delistings:
        symbols_str = ", ".join(f"'{s}'" for s in delistings)
        spark.sql(f"""
            UPDATE {CATALOG}.{SCHEMA}.instrument_feed_config fc
            SET is_active = false, updated_at = current_timestamp()
            WHERE fc.instrument_id IN (
                SELECT i.instrument_id
                FROM {CATALOG}.{SCHEMA}.instrument i
                JOIN {CATALOG}.{SCHEMA}.instrument_listing l
                  ON i.instrument_id = l.instrument_id AND l.is_current = true
                WHERE l.symbol IN ({symbols_str})
            )
        """)
        print(f"✓ Deactivated {len(delistings)} delisted instruments")

    # ── Feed config activations ────────────────────────────────────────────
    if feed_activations:
        for item in feed_activations:
            spark.sql(f"""
                UPDATE {CATALOG}.{SCHEMA}.instrument_feed_config
                SET is_active = true, updated_at = current_timestamp()
                WHERE instrument_id = {item['instrument_id']}
                  AND stream_name = 'daily'
            """)
        print(f"✓ Activated {len(feed_activations)} feed configs")

    print("\n✓ Universe sync complete")

# COMMAND ----------
# MAGIC %md ## Summary

# COMMAND ----------

summary = spark.sql(f"""
    SELECT
        asset_class,
        COUNT(*) AS total_instruments,
        SUM(CASE WHEN fc.is_active THEN 1 ELSE 0 END) AS active_feed
    FROM {CATALOG}.{SCHEMA}.instrument i
    LEFT JOIN {CATALOG}.{SCHEMA}.instrument_feed_config fc
           ON i.instrument_id = fc.instrument_id AND fc.stream_name = 'daily'
    GROUP BY asset_class
    ORDER BY asset_class
""")
summary.show()
