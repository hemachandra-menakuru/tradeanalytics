# Databricks notebook source
# MAGIC %md
# MAGIC # Ops: Rename `interval` → `bar_interval`
# MAGIC
# MAGIC **Purpose:** Rename the `interval` column to `bar_interval` on all affected Delta tables.
# MAGIC `INTERVAL` is a Spark SQL data type keyword — using it as a column name creates ambiguity.
# MAGIC
# MAGIC **Affected tables:**
# MAGIC - `tradeanalytics.bronze.market_data_daily`
# MAGIC - `tradeanalytics.control.ingestion_watermark`
# MAGIC
# MAGIC **Safety:** Metadata-only operation. No data is rewritten. Instant and reversible.
# MAGIC Safe to re-run — skips tables where `bar_interval` already exists.

# COMMAND ----------

dbutils.widgets.text("catalog",  "tradeanalytics", "Catalog")
dbutils.widgets.text("dry_run",  "true",            "Dry run (true/false)")

catalog  = dbutils.widgets.get("catalog")
dry_run  = dbutils.widgets.get("dry_run").lower() == "true"

print(f"catalog  = {catalog}")
print(f"dry_run  = {dry_run}")

# COMMAND ----------

# Step 1 — Enable column mapping on both tables (required for RENAME COLUMN)
# Delta columnMapping must be set before RENAME COLUMN can be issued.
# Safe to run even if already enabled.

tables_to_rename = [
    (f"{catalog}.bronze.market_data_daily",    "interval", "bar_interval"),
    (f"{catalog}.control.ingestion_watermark", "interval", "bar_interval"),
]

for full_table, old_col, new_col in tables_to_rename:
    cols = [r["col_name"] for r in spark.sql(f"DESCRIBE TABLE {full_table}").collect()]

    if new_col in cols:
        print(f"[SKIP]  {full_table}.{old_col} → already renamed to {new_col}")
        continue

    if old_col not in cols:
        print(f"[SKIP]  {full_table}: neither '{old_col}' nor '{new_col}' found — check table")
        continue

    print(f"[{'DRY RUN' if dry_run else 'EXEC'}] {full_table}: RENAME COLUMN {old_col} → {new_col}")

    if not dry_run:
        # Enable column mapping (required once per table; harmless if already set)
        spark.sql(f"""
            ALTER TABLE {full_table}
            SET TBLPROPERTIES (
                'delta.minReaderVersion' = '2',
                'delta.minWriterVersion' = '5',
                'delta.columnMapping.mode' = 'name'
            )
        """)
        spark.sql(f"ALTER TABLE {full_table} RENAME COLUMN {old_col} TO {new_col}")
        print(f"[DONE]  {full_table}.{old_col} → {new_col}")

# COMMAND ----------

# Step 2 — Verify post-rename record counts match
print("\\n=== Post-rename verification ===")

for full_table, _, new_col in tables_to_rename:
    count = spark.sql(f"SELECT COUNT(*) AS n FROM {full_table}").collect()[0]["n"]
    print(f"  {full_table}: {count:,} rows  (bar_interval column present: {new_col})")
    sample = spark.sql(f"SELECT DISTINCT {new_col} FROM {full_table} LIMIT 5").collect()
    print(f"  distinct bar_interval values: {[r[new_col] for r in sample]}")
