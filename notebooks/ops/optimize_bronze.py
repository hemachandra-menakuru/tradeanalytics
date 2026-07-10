# Databricks notebook source
# MAGIC %md
# MAGIC # optimize_bronze — small-file compaction + auto-compaction going forward
# MAGIC
# MAGIC **Why:** raw_to_bronze appends one small file per instrument per run, so
# MAGIC `bronze.market_data_daily` accreted ~1,286 files → a trivial `GROUP BY` spawned
# MAGIC 1,286 tasks and took ~12 min. This compacts them and turns on auto-compaction
# MAGIC so it does not recur.
# MAGIC
# MAGIC **Safe on append-only:** `OPTIMIZE` is bin-packing (compaction) — it rewrites
# MAGIC files but changes NO data, so it does not violate `delta.appendOnly=true`.
# MAGIC Idempotent / re-runnable.
# MAGIC
# MAGIC **Cost:** run as a JOB, or DETACH the serverless session when done — don't leave
# MAGIC it warm (a compaction of a large table is a real Spark job).

# COMMAND ----------
# MAGIC %sql
# MAGIC -- 1. Auto-compaction going forward: writes coalesce into fewer/larger files,
# MAGIC --    and a compaction runs after commits — stops the small-file buildup at source.
# MAGIC ALTER TABLE tradeanalytics.bronze.market_data_daily SET TBLPROPERTIES (
# MAGIC   'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC   'delta.autoOptimize.autoCompact'   = 'true'
# MAGIC );

# COMMAND ----------
# MAGIC %sql
# MAGIC -- 2. One-time compaction of the files already there.
# MAGIC OPTIMIZE tradeanalytics.bronze.market_data_daily;

# COMMAND ----------
# MAGIC %sql
# MAGIC -- 3. Verify: numFiles should drop sharply (hundreds → a handful).
# MAGIC DESCRIBE DETAIL tradeanalytics.bronze.market_data_daily;

# COMMAND ----------
# MAGIC %md
# MAGIC Optional periodic maintenance (autoCompact usually makes this unnecessary):
# MAGIC re-run cell 2 monthly, or schedule this notebook. `VACUUM` (default 7-day
# MAGIC retention) can reclaim the pre-compaction files later — but VACUUM is a delete
# MAGIC of tombstoned files, so run it deliberately and never with a 0-hour retention.
