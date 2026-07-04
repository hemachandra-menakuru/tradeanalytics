# Databricks notebook source
# MAGIC %md
# MAGIC # 06 — Load staged vendor-ID mappings into reference.instrument_vendor_id
# MAGIC
# MAGIC **Step 2 of 2** of the repeatable vendor-ID seeding flow (two-plane split):
# MAGIC - Step 1: `scripts/seed_vendor_ids.py` (Mac — gateway reachable) qualifies
# MAGIC   unmapped listings and stages JSON to
# MAGIC   `s3://handh-trade-raw-use1/reference/vendor_id_seed/<vendor>/<timestamp>.json`
# MAGIC - Step 2 (this notebook): read the staged file, validate, INSERT new
# MAGIC   mappings. Databricks is the only Delta writer.
# MAGIC
# MAGIC **Safety:**
# MAGIC - Never overwrites existing current mappings — conflicts (same instrument,
# MAGIC   different conId) are REPORTED for human review, not auto-resolved
# MAGIC   (that's SCD-2 territory — corporate-action review)
# MAGIC - Re-runnable: loading the same staged file twice inserts nothing new
# MAGIC
# MAGIC **Widgets:** `vendor` (default ibkr), `staged_file` (blank = latest staged file)

# COMMAND ----------
import json

dbutils.widgets.text("vendor",      "ibkr", "Vendor")
dbutils.widgets.text("staged_file", "",     "Staged file path (blank = latest)")

VENDOR      = dbutils.widgets.get("vendor").strip() or "ibkr"
CATALOG     = "tradeanalytics"
RAW_BUCKET  = "handh-trade-raw-use1"
STAGE_DIR   = f"s3://{RAW_BUCKET}/reference/vendor_id_seed/{VENDOR}/"

staged_file = dbutils.widgets.get("staged_file").strip()
if not staged_file:
    files = sorted(dbutils.fs.ls(STAGE_DIR), key=lambda f: f.name)
    if not files:
        raise ValueError(f"No staged files under {STAGE_DIR} — run scripts/seed_vendor_ids.py first")
    staged_file = files[-1].path  # timestamps in names → last = latest

print(f"Loading staged file: {staged_file}")
doc = json.loads(dbutils.fs.head(staged_file, 50_000_000))
print(f"Staged at {doc['staged_at']} via {doc.get('gateway')} — "
      f"{doc['mapping_count']} mapping(s), {len(doc.get('failures', []))} failure(s)")

# COMMAND ----------
# MAGIC %md ## Review — what the discovery run found

# COMMAND ----------
import pandas as pd

mappings = doc["mappings"]
display(spark.createDataFrame(pd.DataFrame(mappings)))

if doc.get("failures"):
    print("⚠ Discovery failures (NOT loaded — investigate separately):")
    for f in doc["failures"]:
        print(f"   {f['symbol']}: {f['error']}")

# COMMAND ----------
# MAGIC %md ## Validate against existing mappings

# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, LongType

schema = StructType([
    StructField("instrument_id",        LongType(),   False),
    StructField("symbol",               StringType(), True),
    StructField("vendor",               StringType(), False),
    StructField("vendor_instrument_id", StringType(), False),
    StructField("vendor_exchange",      StringType(), True),
])
staged_df = spark.createDataFrame(
    [(m["instrument_id"], m["symbol"], m["vendor"],
      m["vendor_instrument_id"], m.get("vendor_exchange")) for m in mappings],
    schema=schema,
)
staged_df.createOrReplaceTempView("_staged_mappings")

# Conflicts: instrument already has a CURRENT mapping with a DIFFERENT id
conflicts = spark.sql(f"""
    SELECT s.symbol, s.instrument_id,
           s.vendor_instrument_id AS staged_id,
           v.vendor_instrument_id AS existing_id
    FROM _staged_mappings s
    JOIN {CATALOG}.reference.instrument_vendor_id v
      ON v.instrument_id = s.instrument_id
     AND v.vendor = s.vendor AND v.is_current = true
    WHERE v.vendor_instrument_id <> s.vendor_instrument_id
""").collect()

if conflicts:
    print("❌ CONFLICTS — existing current mapping differs from staged. NOT loading these;")
    print("   resolve via SCD-2 review (possible corporate action / relisting):")
    for c in conflicts:
        print(f"   {c.symbol}: existing={c.existing_id} staged={c.staged_id}")
else:
    print("✅ No conflicts with existing mappings")

# COMMAND ----------
# MAGIC %md ## Load — insert NEW mappings only (idempotent)

# COMMAND ----------
result = spark.sql(f"""
    INSERT INTO {CATALOG}.reference.instrument_vendor_id
        (instrument_id, vendor, vendor_instrument_id, vendor_exchange,
         valid_from, is_current, notes, created_at)
    SELECT s.instrument_id, s.vendor, s.vendor_instrument_id, s.vendor_exchange,
           current_date(), true,
           'seeded via vendor_id_seed flow — staged file: {staged_file.split("/")[-1]}',
           current_timestamp()
    FROM _staged_mappings s
    LEFT JOIN {CATALOG}.reference.instrument_vendor_id v
           ON v.instrument_id = s.instrument_id
          AND v.vendor = s.vendor AND v.is_current = true
    WHERE v.vendor_instrument_id IS NULL
""")

# COMMAND ----------
# MAGIC %md ## Verify

# COMMAND ----------
summary = spark.sql(f"""
    SELECT vendor, is_current, COUNT(*) AS mappings
    FROM {CATALOG}.reference.instrument_vendor_id
    GROUP BY vendor, is_current
""")
display(summary)

unmapped = spark.sql(f"""
    SELECT COUNT(*) AS still_unmapped
    FROM {CATALOG}.reference.instrument_listing l
    LEFT JOIN {CATALOG}.reference.instrument_vendor_id v
           ON v.instrument_id = l.instrument_id
          AND v.vendor = '{VENDOR}' AND v.is_current = true
    WHERE l.is_current = true AND v.vendor_instrument_id IS NULL
""").collect()[0].still_unmapped
print(f"Current listings still unmapped for {VENDOR}: {unmapped}")
print("(any remainder should be dead/placeholder tickers recorded as discovery failures)")
