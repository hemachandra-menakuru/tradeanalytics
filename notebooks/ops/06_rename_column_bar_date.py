# Databricks notebook source
# MAGIC %md
# MAGIC # Ops: Rename `date` → `bar_date` in Bronze Delta table
# MAGIC
# MAGIC **When to run:** Once, against the live `tradeanalytics` catalog in Databricks.
# MAGIC Run on a regular cluster (not serverless) with Unity Catalog access.
# MAGIC
# MAGIC **What this does:**
# MAGIC - Renames the `date` column to `bar_date` in `tradeanalytics.bronze.market_data_daily`
# MAGIC - Verifies the rename succeeded and all existing data is intact
# MAGIC - Safe to re-run — the column rename is skipped if `bar_date` already exists
# MAGIC
# MAGIC **Why `bar_date`:**
# MAGIC - `date` is a SQL reserved word — causes ambiguity in every query
# MAGIC - `bar_date` is explicit and pairs with the existing `bar_time_utc` column
# MAGIC - Consistent naming: `bar_date + bar_time_utc` = complete bar identifier for daily and intraday
# MAGIC - In Phase 5 we'll have a `gold.trades` table with its own `trade_date` (fill date);
# MAGIC   `bar_date` prevents confusion between an OHLCV bar date and a trade execution date
# MAGIC
# MAGIC **Safety:** Delta RENAME COLUMN is a metadata-only operation (no data rewrite).
# MAGIC It is instant and reversible with another RENAME COLUMN.

# COMMAND ----------

dbutils.widgets.text("catalog", "tradeanalytics", "Unity Catalog name")
dbutils.widgets.dropdown("dry_run", "true", ["true", "false"], "Dry run (no changes)")

CATALOG  = dbutils.widgets.get("catalog").strip()
DRY_RUN  = dbutils.widgets.get("dry_run").lower() == "true"
TABLE    = f"{CATALOG}.bronze.market_data_daily"

print(f"Catalog : {CATALOG}")
print(f"Table   : {TABLE}")
print(f"Dry run : {DRY_RUN}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 1 — Check current schema

# COMMAND ----------

print(f"\n── Current schema of {TABLE} ──")
schema_df = spark.sql(f"DESCRIBE TABLE {TABLE}")
schema_rows = schema_df.collect()

col_names = [r["col_name"] for r in schema_rows if r["col_name"] and not r["col_name"].startswith("#")]
print(f"Columns: {col_names}")

has_date     = "date"     in col_names
has_bar_date = "bar_date" in col_names

print(f"\n  'date'     column exists : {has_date}")
print(f"  'bar_date' column exists : {has_bar_date}")

if has_bar_date and not has_date:
    print("\n  ✅ bar_date already exists and date is gone — rename was already applied. Nothing to do.")
    dbutils.notebook.exit("ALREADY_DONE")

if not has_date and not has_bar_date:
    raise ValueError(f"Neither 'date' nor 'bar_date' found in {TABLE}. Check the table name.")

if has_date and has_bar_date:
    raise ValueError(
        f"Both 'date' AND 'bar_date' exist in {TABLE}. "
        f"Investigate before proceeding — this is an unexpected state."
    )

print(f"\n  → 'date' column found. Renaming to 'bar_date'.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 2 — Pre-rename record count

# COMMAND ----------

pre_count = spark.sql(f"SELECT COUNT(*) AS n FROM {TABLE}").collect()[0]["n"]
print(f"Record count BEFORE rename: {pre_count:,}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 3 — Rename column

# COMMAND ----------

if DRY_RUN:
    print("  ⏭ DRY RUN — skipping ALTER TABLE. Set dry_run=false to execute.")
else:
    print(f"  Executing: ALTER TABLE {TABLE} RENAME COLUMN date TO bar_date")
    spark.sql(f"ALTER TABLE {TABLE} RENAME COLUMN date TO bar_date")
    print("  ✅ RENAME COLUMN complete.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 4 — Verify rename and record count

# COMMAND ----------

if DRY_RUN:
    print("  ⏭ DRY RUN — skipping verification.")
else:
    # Verify schema
    new_schema_rows = spark.sql(f"DESCRIBE TABLE {TABLE}").collect()
    new_col_names = [r["col_name"] for r in new_schema_rows if r["col_name"] and not r["col_name"].startswith("#")]

    print(f"\n  Columns after rename:")
    for c in new_col_names:
        marker = "  ← renamed" if c == "bar_date" else ""
        print(f"    {c}{marker}")

    assert "bar_date" in new_col_names, "FAIL: bar_date not found after rename"
    assert "date"     not in new_col_names, "FAIL: old 'date' column still present"

    # Verify record count unchanged
    post_count = spark.sql(f"SELECT COUNT(*) AS n FROM {TABLE}").collect()[0]["n"]
    print(f"\n  Record count AFTER rename : {post_count:,}")
    assert post_count == pre_count, f"FAIL: record count changed ({pre_count} → {post_count})"

    # Spot-check data
    sample = spark.sql(f"""
        SELECT symbol, bar_date, interval, open, close, source, instrument_id
        FROM {TABLE}
        ORDER BY bar_date DESC
        LIMIT 5
    """)
    print(f"\n  Sample rows (most recent 5):")
    display(sample)

    print(f"\n  ✅ Rename verified. {post_count:,} records intact. bar_date column operational.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 5 — Enable Delta column mapping (required for RENAME COLUMN)
# MAGIC
# MAGIC **Note:** Delta RENAME COLUMN requires `delta.columnMapping.mode = 'name'`.
# MAGIC If the ALTER TABLE above failed with a column mapping error, run this cell first,
# MAGIC then re-run Step 3.

# COMMAND ----------

# Uncomment and run ONLY if Step 3 failed with:
# "DELTA_UNSUPPORTED_RENAME_COLUMN: Column rename is only supported for Delta tables with column mapping enabled"

# spark.sql(f"""
#     ALTER TABLE {TABLE}
#     SET TBLPROPERTIES (
#         'delta.minReaderVersion' = '2',
#         'delta.minWriterVersion' = '5',
#         'delta.columnMapping.mode' = 'name'
#     )
# """)
# print("Column mapping enabled — now re-run Step 3.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 6 — Verify ingestion watermark (not affected by rename)

# COMMAND ----------

wm = spark.sql(f"""
    SELECT instrument_id, stream, interval, earliest_date, latest_date,
           record_count, vendor, status
    FROM {CATALOG}.control.ingestion_watermark
    ORDER BY instrument_id
""")
print("Watermark table (not affected by rename — shown for completeness):")
display(wm)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

if DRY_RUN:
    print("""
DRY RUN COMPLETE — no changes made.

To apply the rename for real:
  1. Set dry_run = false in the widget
  2. Re-run all cells

The rename is:
  ALTER TABLE tradeanalytics.bronze.market_data_daily RENAME COLUMN date TO bar_date

This is a metadata-only operation (instant, no data rewrite).
It is reversible: RENAME COLUMN bar_date TO date if needed.
""")
else:
    print(f"""
✅ RENAME COMPLETE

  Table   : {TABLE}
  Before  : ... date  DATE ...
  After   : ... bar_date DATE ...
  Records : {pre_count:,} (unchanged)

Next steps:
  - All Python code (providers, writer, validator, job) already updated
  - All 402 unit tests passing
  - Re-run Bronze ingestion to confirm end-to-end pipeline works with bar_date
  - No Silver or Gold tables exist yet — no downstream schema changes needed
""")
