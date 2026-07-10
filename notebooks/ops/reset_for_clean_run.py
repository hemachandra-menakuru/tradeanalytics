# Databricks notebook source
# MAGIC %md
# MAGIC # Reset All Tables for a Clean Ingestion Run
# MAGIC
# MAGIC **When to run:** Before a full re-ingestion from scratch across all instruments.
# MAGIC
# MAGIC **What this does:**
# MAGIC - Clears all Bronze price data (market_data_daily, market_data_rejected)
# MAGIC - Clears all ingestion state (watermarks, job log, commands, candidates)
# MAGIC - Does NOT touch reference tables (instrument, listing, feed_config, calendar)
# MAGIC
# MAGIC **What is preserved:**
# MAGIC - `reference.instrument` — permanent instrument master
# MAGIC - `reference.instrument_listing` — symbols and exchanges
# MAGIC - `reference.instrument_vendor_id` — vendor IDs
# MAGIC - `reference.universe_membership` — universe config
# MAGIC - `reference.ticker_feed_config` — desired state (what to fetch)
# MAGIC - `reference.market_calendar` — exchange calendar
# MAGIC - `reference.corporate_actions` — corporate action audit log
# MAGIC
# MAGIC ⚠️ This is a destructive operation. Run only in dev — never in production.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 — Confirm before proceeding

# COMMAND ----------

print("=" * 60)
print("  RESET FOR CLEAN RUN")
print("=" * 60)
print()
print("Tables that will be CLEARED:")
print("  bronze.market_data_daily")
print("  bronze.market_data_rejected")
print("  control.ingestion_watermark")
print("  control.job_run_log")
print("  control.ingestion_command")
print("  control.corporate_action_candidates")
print()
print("Tables that will be PRESERVED:")
print("  reference.instrument")
print("  reference.instrument_listing")
print("  reference.instrument_vendor_id")
print("  reference.universe_membership")
print("  reference.ticker_feed_config")
print("  reference.market_calendar")
print("  reference.corporate_actions")
print()
print("Run the cells below to proceed.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 — Clear Bronze tables
# MAGIC
# MAGIC Both Bronze tables have `delta.appendOnly = true`.
# MAGIC We temporarily disable it, delete, then re-enable.

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE tradeanalytics.bronze.market_data_daily
# MAGIC SET TBLPROPERTIES ('delta.appendOnly' = 'false');

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM tradeanalytics.bronze.market_data_daily;

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE tradeanalytics.bronze.market_data_daily
# MAGIC SET TBLPROPERTIES ('delta.appendOnly' = 'true');

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE tradeanalytics.bronze.market_data_rejected
# MAGIC SET TBLPROPERTIES ('delta.appendOnly' = 'false');

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM tradeanalytics.bronze.market_data_rejected;

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE tradeanalytics.bronze.market_data_rejected
# MAGIC SET TBLPROPERTIES ('delta.appendOnly' = 'true');

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 — Clear control tables

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM tradeanalytics.control.ingestion_watermark;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- job_run_log is appendOnly — disable, delete, re-enable
# MAGIC ALTER TABLE tradeanalytics.control.job_run_log
# MAGIC SET TBLPROPERTIES ('delta.appendOnly' = 'false');

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM tradeanalytics.control.job_run_log;

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE tradeanalytics.control.job_run_log
# MAGIC SET TBLPROPERTIES ('delta.appendOnly' = 'true');

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM tradeanalytics.control.ingestion_command;

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM tradeanalytics.control.corporate_action_candidates;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 — Verify all tables are empty

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'bronze.market_data_daily'            AS table_name, COUNT(*) AS rows FROM tradeanalytics.bronze.market_data_daily
# MAGIC UNION ALL
# MAGIC SELECT 'bronze.market_data_rejected',                        COUNT(*) FROM tradeanalytics.bronze.market_data_rejected
# MAGIC UNION ALL
# MAGIC SELECT 'control.ingestion_watermark',                        COUNT(*) FROM tradeanalytics.control.ingestion_watermark
# MAGIC UNION ALL
# MAGIC SELECT 'control.job_run_log',                                COUNT(*) FROM tradeanalytics.control.job_run_log
# MAGIC UNION ALL
# MAGIC SELECT 'control.ingestion_command',                          COUNT(*) FROM tradeanalytics.control.ingestion_command
# MAGIC UNION ALL
# MAGIC SELECT 'control.corporate_action_candidates',                COUNT(*) FROM tradeanalytics.control.corporate_action_candidates
# MAGIC ORDER BY table_name;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 — Confirm reference tables are intact

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'reference.instrument'           AS table_name, COUNT(*) AS rows FROM tradeanalytics.reference.instrument
# MAGIC UNION ALL
# MAGIC SELECT 'reference.instrument_listing',                  COUNT(*) FROM tradeanalytics.reference.instrument_listing
# MAGIC UNION ALL
# MAGIC SELECT 'reference.ticker_feed_config',                  COUNT(*) FROM tradeanalytics.reference.ticker_feed_config
# MAGIC UNION ALL
# MAGIC SELECT 'reference.universe_membership',                 COUNT(*) FROM tradeanalytics.reference.universe_membership
# MAGIC ORDER BY table_name;

# COMMAND ----------

print("✅ Reset complete. All Bronze and control tables cleared.")
print("   Reference tables intact. Ready for clean ingestion run.")
