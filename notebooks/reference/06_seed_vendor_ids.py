# Databricks notebook source
# MAGIC %md
# MAGIC # 06 — Seed vendor IDs (fully Databricks-run, repeatable)
# MAGIC
# MAGIC Discovers IBKR conIds for all **unmapped current listings** and loads them
# MAGIC into `reference.instrument_vendor_id`. Run this any time — after adding new
# MAGIC instruments, after universe expansion — it is differential and idempotent.
# MAGIC
# MAGIC **How it works (Two-Plane pattern, CLAUDE.md §14):** Databricks cannot reach
# MAGIC the IB Gateway (serverless has no egress), so this notebook submits a
# MAGIC `QUALIFY_INSTRUMENTS` manifest to the agent queue; the EC2 fetch agent
# MAGIC (which sits next to the gateway) qualifies the batch and writes the results
# MAGIC manifest back; this notebook polls for it, validates, and loads.
# MAGIC
# MAGIC **Prerequisites:** the fetch agent must be RUNNING on the EC2 box.
# MAGIC
# MAGIC **Safety:** never overwrites existing current mappings — conflicts are
# MAGIC reported for SCD-2 review; failures (dead tickers) reported, not loaded.

# COMMAND ----------
import json, time, uuid
from datetime import datetime, timezone

dbutils.widgets.text("vendor",          "ibkr", "Vendor")
dbutils.widgets.text("timeout_minutes", "45",   "Max wait for agent (minutes)")

VENDOR     = dbutils.widgets.get("vendor").strip() or "ibkr"
TIMEOUT_S  = int(dbutils.widgets.get("timeout_minutes")) * 60
CATALOG    = "tradeanalytics"
RAW_BUCKET = "handh-trade-raw-use1"
PENDING    = f"s3://{RAW_BUCKET}/control/fetch/{VENDOR}/pending"
DONE       = f"s3://{RAW_BUCKET}/control/fetch/{VENDOR}/done"
FAILED     = f"s3://{RAW_BUCKET}/control/fetch/{VENDOR}/failed"

# COMMAND ----------
# MAGIC %md ## Step 1 — Find unmapped current listings (differential)

# COMMAND ----------
unmapped = spark.sql(f"""
    SELECT l.instrument_id, l.symbol, l.currency
    FROM {CATALOG}.reference.instrument_listing l
    LEFT JOIN {CATALOG}.reference.instrument_vendor_id v
           ON v.instrument_id = l.instrument_id
          AND v.vendor = '{VENDOR}' AND v.is_current = true
    WHERE l.is_current = true AND v.vendor_instrument_id IS NULL
    ORDER BY l.symbol
""").collect()

if not unmapped:
    print("✅ Nothing to do — every current listing already has a mapping.")
    dbutils.notebook.exit("no_unmapped_listings")

print(f"{len(unmapped)} unmapped listing(s) → submitting qualification request to agent")
est_min = round(len(unmapped) * 1.2 / 60, 1)
print(f"Estimated agent processing time: ~{est_min} min")

# COMMAND ----------
# MAGIC %md ## Step 2 — Submit QUALIFY_INSTRUMENTS manifest to the agent queue

# COMMAND ----------
request_key = f"qualify_{VENDOR}_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}_{uuid.uuid4().hex[:6]}"
manifest = {
    "contract_version": "2",
    "task_type":        "QUALIFY_INSTRUMENTS",
    "request_key":      request_key,
    "batch_id":         request_key,
    "stream":           "reference",
    "load_type":        "SEED",
    "instruments": [
        {"instrument_id": r.instrument_id, "symbol": r.symbol, "currency": r.currency}
        for r in unmapped
    ],
    "lineage": {
        "requested_at": datetime.now(timezone.utc).isoformat(),
        "requested_by": "notebooks/reference/06_seed_vendor_ids",
    },
}
dbutils.fs.put(f"{PENDING}/{request_key}.json", json.dumps(manifest, indent=2), True)
print(f"Submitted: {PENDING}/{request_key}.json")

# COMMAND ----------
# MAGIC %md ## Step 3 — Wait for the agent's results

# COMMAND ----------
def _try_read(path):
    try:
        return json.loads(dbutils.fs.head(path, 50_000_000))
    except Exception:
        return None

result, waited = None, 0
poll = 15
while waited < TIMEOUT_S:
    result = _try_read(f"{DONE}/{request_key}.json")
    if result:
        break
    failed_doc = _try_read(f"{FAILED}/{request_key}.json")
    if failed_doc:
        raise RuntimeError(f"Agent FAILED the request: {failed_doc.get('error_message')}")
    time.sleep(poll)
    waited += poll
    if waited % 120 == 0:
        print(f"  … waiting ({waited//60} min)")

if not result:
    raise TimeoutError(
        f"No result after {TIMEOUT_S//60} min. Is the fetch agent running on EC2? "
        f"Check: journalctl -u fetch-agent-{VENDOR} | tail. The request remains "
        f"queued and will be processed when the agent returns; re-run this "
        f"notebook with widget staged to load, or simply re-run later."
    )

mappings  = result.get("mappings", [])
failures  = result.get("failures", [])
print(f"✅ Agent returned: {len(mappings)} mapped, {len(failures)} failed")

# COMMAND ----------
# MAGIC %md ## Step 4 — Review

# COMMAND ----------
import pandas as pd
if mappings:
    display(spark.createDataFrame(pd.DataFrame(mappings)))
if failures:
    print("⚠ Qualification failures (NOT loaded — typically dead/placeholder tickers):")
    for f in failures:
        print(f"   {f['symbol']}: {f['error']}")

# COMMAND ----------
# MAGIC %md ## Step 5 — Validate (conflicts) and load (new-only, idempotent)

# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, LongType

if mappings:
    schema = StructType([
        StructField("instrument_id",        LongType(),   False),
        StructField("symbol",               StringType(), True),
        StructField("vendor",               StringType(), False),
        StructField("vendor_instrument_id", StringType(), False),
        StructField("vendor_exchange",      StringType(), True),
    ])
    spark.createDataFrame(
        [(m["instrument_id"], m["symbol"], m["vendor"],
          m["vendor_instrument_id"], m.get("vendor_exchange")) for m in mappings],
        schema=schema,
    ).createOrReplaceTempView("_staged_mappings")

    conflicts = spark.sql(f"""
        SELECT s.symbol, s.vendor_instrument_id AS staged_id,
               v.vendor_instrument_id AS existing_id
        FROM _staged_mappings s
        JOIN {CATALOG}.reference.instrument_vendor_id v
          ON v.instrument_id = s.instrument_id
         AND v.vendor = s.vendor AND v.is_current = true
        WHERE v.vendor_instrument_id <> s.vendor_instrument_id
    """).collect()
    if conflicts:
        print("❌ CONFLICTS — existing mapping differs; NOT loading these (SCD-2 review):")
        for c in conflicts:
            print(f"   {c.symbol}: existing={c.existing_id} staged={c.staged_id}")

    spark.sql(f"""
        INSERT INTO {CATALOG}.reference.instrument_vendor_id
            (instrument_id, vendor, vendor_instrument_id, vendor_exchange,
             valid_from, is_current, notes, created_at)
        SELECT s.instrument_id, s.vendor, s.vendor_instrument_id, s.vendor_exchange,
               current_date(), true,
               'seeded via 06_seed_vendor_ids — request {request_key}',
               current_timestamp()
        FROM _staged_mappings s
        LEFT JOIN {CATALOG}.reference.instrument_vendor_id v
               ON v.instrument_id = s.instrument_id
              AND v.vendor = s.vendor AND v.is_current = true
        WHERE v.vendor_instrument_id IS NULL
    """)

# COMMAND ----------
# MAGIC %md ## Step 6 — Verify

# COMMAND ----------
display(spark.sql(f"""
    SELECT vendor, is_current, COUNT(*) AS mappings
    FROM {CATALOG}.reference.instrument_vendor_id GROUP BY vendor, is_current
"""))
still = spark.sql(f"""
    SELECT COUNT(*) AS n FROM {CATALOG}.reference.instrument_listing l
    LEFT JOIN {CATALOG}.reference.instrument_vendor_id v
           ON v.instrument_id = l.instrument_id
          AND v.vendor = '{VENDOR}' AND v.is_current = true
    WHERE l.is_current = true AND v.vendor_instrument_id IS NULL
""").collect()[0].n
print(f"Still unmapped: {still} (should equal the failure count — dead tickers)")
